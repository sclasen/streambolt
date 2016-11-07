package streambolt

import (
	"errors"
	"fmt"
	"io"
	"log"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/boltdb/bolt"
)

const BootstrapSequence = "00000000000000000000"

type SnapshotGenerator interface {
	Bootstrap(*bolt.Tx) (initialKinesisSeq string, err error)
	OnStart(*bolt.Tx) error
	OnRecords(*bolt.Tx, *kinesis.GetRecordsOutput) error
}

type ShardSnapshotFinder struct {
	S3Client       s3iface.S3API
	SnapshotBucket string
	SnapshotPath   string
	LocalPath      string
	Stream         string
	ShardId        string
}

type ShardSnapshotter struct {
	S3Client       s3iface.S3API
	KinesisClient  kinesisiface.KinesisAPI
	Generator      SnapshotGenerator
	SnapshotBucket string
	SnapshotPath   string
	LocalPath      string
	Stream         string
	ShardId        string
	DoneLag        int64
	CompactDB      bool
}

//TODO ListSnapshots and DeleteShapshot for GCing. or maybe just GCBefore(time.Time)

func (s *ShardSnapshotter) Finder() *ShardSnapshotFinder {
	return &ShardSnapshotFinder{
		S3Client:       s.S3Client,
		SnapshotBucket: s.SnapshotBucket,
		SnapshotPath:   s.SnapshotPath,
		LocalPath:      s.LocalPath,
		Stream:         s.Stream,
		ShardId:        s.ShardId,
	}
}

func (s *ShardSnapshotter) SnapshotShard() (*Snapshot, error) {
	finder := s.Finder()
	latest, err := finder.FindLatestSnapshot()
	if err != nil {
		return nil, err
	}

	if latest != nil {
		err = finder.DownloadSnapshot(*latest)
		if err != nil {
			return nil, err
		}
	} else {
		l, err := s.BootstrapSnapshot()
		if err != nil {
			return nil, err
		}
		latest = l
	}

	if latest != nil {
		working, err := s.ToWorkingCopy(*latest)
		if err != nil {
			return nil, err
		}
		updatedSeq, err := s.UpdateWorkingCopy(working, latest.KinesisSeq)
		if err != nil {
			log.Printf("component=shard-snapshotter shard-id=%s fn=snapshot-shard at=error-updating-working-copy removing=%s", s.ShardId, working)
			re := os.Remove(working)
			if re != nil {
				log.Printf("component=shard-snapshotter shard-id=%s fn=snapshot-shard at=error-removing-working-copy copy=%s", s.ShardId, working)
			}
			return nil, err
		}
		updatedSnapshot := finder.SnapshotFromKinesisSeq(updatedSeq)
		err = s.FromWorkingCopy(working, updatedSnapshot)
		if err != nil {
			if err != nil {
				log.Printf("component=shard-snapshotter shard-id=%s fn=snapshot-shard at=error-moving-working-copy removing=%s", s.ShardId, working)
				re := os.Remove(working)
				if re != nil {
					log.Printf("component=shard-snapshotter shard-id=%s fn=snapshot-shard at=error-removing-working-copy copy=%s", s.ShardId, working)
				}
				return nil, err
			}
			return nil, err
		}
		err = s.UploadSnapshot(updatedSnapshot)
		if err != nil {
			return nil, err
		}
		return &updatedSnapshot, nil
	}

	return nil, errors.New("no snapshot generated")
}

func (s *ShardSnapshotFinder) FindSnapshots() ([]Snapshot, error) {
	snapshots := []Snapshot{}
	eachPage := func(o *s3.ListObjectsOutput, _ bool) bool {
		log.Printf("component=shard-snapshotter shard-id=%s fn=snapshot-shard at=list-objects-page", s.ShardId)
		for _, obj := range o.Contents {
			if ss := s.SnapshotFromS3Key(*obj.Key); ss != nil {
				snapshots = append(snapshots, *ss)
				log.Printf("component=shard-snapshotter shard-id=%s fn=snapshot-shard at=snapshot-key key=%s", s.ShardId, *obj.Key)
			} else {
				log.Printf("component=shard-snapshotter shard-id=%s fn=snapshot-shard at=non-snapshot-key key=%s", s.ShardId, *obj.Key)
			}
		}
		return false
	}
	err := s.S3Client.ListObjectsPages(&s3.ListObjectsInput{
		Bucket: aws.String(s.SnapshotBucket),
		Prefix: aws.String(s.S3Prefix()),
	}, eachPage)

	if err != nil {
		log.Printf("component=shard-snapshotter shard-id=%s fn=snapshot-shard at=list-objects-error error=%s", s.ShardId, err)
		return nil, err
	}
	return snapshots, nil
}

func (s *ShardSnapshotFinder) FindLatestSnapshot() (*Snapshot, error) {
	snapshots, err := s.FindSnapshots()
	if err != nil {
		return nil, err
	}
	latest := big.NewInt(-1)
	var latestSnap *Snapshot

	for _, ss := range snapshots {
		seq, ok := big.NewInt(0).SetString(ss.KinesisSeq, 10)
		if ok && latest.Cmp(seq) < 0 {
			latest = seq
			temp := ss
			latestSnap = &temp
		}
	}

	if latest.Cmp(big.NewInt(-1)) == 0 {
		log.Printf("component=shard-snapshotter shard-id=%s fn=find-latest-snapshot at=no-snapshots", s.ShardId)
		return nil, nil
	}
	log.Printf("component=shard-snapshotter shard-id=%s fn=find-latest-snapshot at=snapshot snapshot=%s", s.ShardId, latestSnap.S3Key)
	return latestSnap, nil
}

func (s *ShardSnapshotter) BootstrapSnapshot() (*Snapshot, error) {
	init := s.Finder().SnapshotFromKinesisSeq(BootstrapSequence)
	db, err := bolt.Open(init.LocalFile, 0600, nil)
	if err != nil {
		log.Printf("component=shard-snapshotter shard-id=%s fn=bootstrap-snapshot at=bolt-open-error error=%s", s.ShardId, err)
		return nil, err
	}
	defer func() {
		_ = db.Close()
	}()

	updatedSeq := ""

	err = db.Update(func(tx *bolt.Tx) error {
		seq, err := s.Generator.Bootstrap(tx)
		if err != nil {
			log.Printf("component=shard-snapshotter shard-id=%s fn=bootstrap-snapshot at=error error=%s", s.ShardId, err)
			return err
		}
		updatedSeq = seq
		return nil
	})

	if err != nil {
		return nil, err
	}

	snapshot := s.Finder().SnapshotFromKinesisSeq(updatedSeq)

	//Need to close db before compacting it.
	err = db.Close()

	if err != nil {
		return nil, err
	}

	return &snapshot, nil
}

func (s *ShardSnapshotFinder) DownloadSnapshot(snapshot Snapshot) error {
	out, err := s.S3Client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(s.SnapshotBucket),
		Key:    aws.String(snapshot.S3Key),
	})
	if err != nil {
		log.Printf("component=shard-snapshotter shard-id=%s fn=download-snapshot at=get-obj-error error=%s", s.ShardId, err)
		return err
	}
	f, err := os.Create(snapshot.LocalFile)
	defer f.Close()
	defer out.Body.Close()

	if err != nil {
		return err
	}

	_, err = io.Copy(f, out.Body)

	if err != nil {
		log.Printf("component=shard-snapshotter shard-id=%s fn=download-snapshot at=copy-error error=%s", s.ShardId, err)
	}

	return err
}

func (s *ShardSnapshotter) ToWorkingCopy(snapshot Snapshot) (string, error) {
	copy := (fmt.Sprintf("%s/working-%s-%s-%d", s.LocalPath, s.Stream, s.ShardId, time.Now().UnixNano()))
	return copy, exec.Command("mv", snapshot.LocalFile, copy).Run()
}

func (s *ShardSnapshotter) UpdateWorkingCopy(workingCopyFilename string, lastSequence string) (string, error) {
	db, err := bolt.Open(workingCopyFilename, 0600, nil)
	if err != nil {
		log.Printf("component=shard-snapshotter shard-id=%s fn=update-working-copy at=bolt-open-error error=%s", s.ShardId, err)
		return "", err
	}
	defer db.Close()

	updatedSeq := lastSequence

	err = db.Update(func(tx *bolt.Tx) error {
		err = s.Generator.OnStart(tx)
		if err != nil {
			log.Printf("component=shard-snapshotter shard-id=%s fn=update-working-copy at=on-start-error error=%s", s.ShardId, err)
			return err
		}
		updatedSeq, err = s.UpdateSnapshot(tx, lastSequence)
		return err
	})

	return updatedSeq, err
}

func (s *ShardSnapshotter) UpdateSnapshot(tx *bolt.Tx, startingAfter string) (string, error) {

	latest := startingAfter
	for {

		log.Printf("component=shard-snapshotter shard-id=%s fn=update-snapshot at=get-iterator after=%s", s.ShardId, latest)
		gsi := &kinesis.GetShardIteratorInput{
			StreamName:             aws.String(s.Stream),
			ShardId:                aws.String(s.ShardId),
			ShardIteratorType:      aws.String(kinesis.ShardIteratorTypeAfterSequenceNumber),
			StartingSequenceNumber: aws.String(latest),
		}

		if latest == BootstrapSequence {
			gsi.ShardIteratorType = aws.String(kinesis.ShardIteratorTypeTrimHorizon)
			gsi.StartingSequenceNumber = nil
		}

		it, err := s.KinesisClient.GetShardIterator(gsi)

		if err != nil {
			log.Printf("component=shard-snapshotter shard-id=%s fn=update-snapshot at=get-iterator-error error=%s", s.ShardId, err)
			return "", err
		}

		iterator := it.ShardIterator
		if iterator == nil {
			log.Printf("component=shard-snapshotter shard-id=%s fn=update-snapshot stream=%s shard=%s at=shard-closed", s.ShardId, s.Stream, s.ShardId)
			return "", nil
		}

		for {
			gr, err := s.KinesisClient.GetRecords(&kinesis.GetRecordsInput{
				ShardIterator: iterator,
			})
			if err != nil {
				if aerr, ok := err.(awserr.Error); ok {
					if aerr.Code() == "ExpiredIteratorException" {
						break
					}
				}
				log.Printf("component=shard-snapshotter shard-id=%s fn=update-snapshot at=get-records-error error=%s", s.ShardId, err)
				return "", err
			}

			log.Printf("component=shard-snapshotter shard-id=%s fn=update-snapshot at=get-records records=%d behind=%d", s.ShardId, len(gr.Records), *gr.MillisBehindLatest)

			iterator = gr.NextShardIterator
			if iterator == nil {
				log.Printf("component=shard-snapshotter shard-id=%s fn=update-snapshot stream=%s shard=%s at=shard-closed", s.ShardId, s.Stream, s.ShardId)
				return "", nil
			}

			err = s.Generator.OnRecords(tx, gr)
			if err != nil {
				log.Printf("component=shard-snapshotter shard-id=%s fn=update-snapshot at=on-records-error error=%s", s.ShardId, err)
				return "", err
			}

			if r := len(gr.Records); r > 0 {
				latest = *gr.Records[r-1].SequenceNumber
			}

			if *gr.MillisBehindLatest < s.DoneLag {
				log.Printf("component=shard-snapshotter shard-id=%s fn=update-snapshot at=done behind=%d done-lag=%d", s.ShardId, *gr.MillisBehindLatest, s.DoneLag)
				return latest, nil
			}
		}
	}
}

func (s *ShardSnapshotter) FromWorkingCopy(file string, snapshot Snapshot) error {
	log.Printf("component=shard-shapshotter shard-id=%s fn=from-working-copy at=start", s.ShardId)
	if s.CompactDB {
		log.Printf("component=shard-shapshotter shard-id=%s fn=from-working-copy at=compact", s.ShardId)
		compactErr := exec.Command("bolt", "compact", "-o", snapshot.LocalFile, file).Run()
		if compactErr == nil {
			ss, serr := os.Stat(file)
			ds, derr := os.Stat(snapshot.LocalFile)
			before := int64(-1)
			after := int64(-1)
			if serr == nil {
				before = ss.Size()
			}
			if derr == nil {
				after = ds.Size()
			}

			log.Printf("component=shard-shapshotter shard-id=%s fn=from-working-copy at=compacted-working-copy size-before=%d size-after=%d", s.ShardId, before, after)

			return nil
		}
		log.Printf("component=shard-shapshotter shard-id=%s fn=from-working-copy at=compaction-error status=fallback-to-simple-mv error=%q", s.ShardId, compactErr)
	}
	log.Printf("component=shard-shapshotter shard-id=%s fn=from-working-copy at=simple-mv", s.ShardId)
	return exec.Command("mv", file, snapshot.LocalFile).Run()
}

func (s *ShardSnapshotter) UploadSnapshot(snapshot Snapshot) error {
	f, err := os.OpenFile(snapshot.LocalFile, syscall.O_RDONLY, 0666)
	defer f.Close()
	if err != nil {
		return err
	}

	_, err = s.S3Client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(s.SnapshotBucket),
		Key:    aws.String(snapshot.S3Key),
		Body:   f,
	})

	return err

}

func (s *ShardSnapshotter) DeleteSnapshotsInS3OlderThan(age time.Duration) (*s3.DeleteObjectsOutput, error) {
	deleteBefore := time.Now().Add(-age)
	toDelete := []*s3.ObjectIdentifier{}
	eachPage := func(o *s3.ListObjectsOutput, _ bool) bool {
		for _, c := range o.Contents {
			if c.LastModified.Before(deleteBefore) {
				oi := &s3.ObjectIdentifier{
					Key: c.Key,
				}
				log.Printf("component=shard-shapshotter shard-id=%s fn=delete-snapshots at=found-deleteable modified=%s key=%s", s.ShardId, *c.LastModified, *c.Key)
				toDelete = append(toDelete, oi)
			}
		}
		return true
	}

	err := s.S3Client.ListObjectsPages(&s3.ListObjectsInput{
		Prefix: aws.String(s.Finder().S3Prefix()),
		Bucket: aws.String(s.SnapshotBucket),
	}, eachPage)

	if err != nil {
		log.Printf("component=shard-shapshotter shard-id=%s fn=delete-snapshots at=list-error error=%s", s.ShardId, err)
		return nil, err
	}

	if len(toDelete) == 0 {
		log.Printf("component=shard-shapshotter shard-id=%s fn=delete-snapshots at=no-eligible-snapshots", s.ShardId)
		return &s3.DeleteObjectsOutput{}, nil
	}

	out, err := s.S3Client.DeleteObjects(&s3.DeleteObjectsInput{
		Bucket: aws.String(s.SnapshotBucket),
		Delete: &s3.Delete{
			Objects: toDelete,
		},
	})

	if err != nil {
		log.Printf("component=shard-shapshotter shard-id=%s fn=delete-snapshots at=delete-objects-error error=%s", s.ShardId, err)
	} else {
		log.Printf("component=shard-shapshotter shard-id=%s fn=delete-snapshots at=sent-delete-objects num-errors=%d", s.ShardId, len(out.Errors))
	}

	return out, err

}

func (s *ShardSnapshotFinder) S3Prefix() string {
	return fmt.Sprintf("%s/%s-%s-", s.SnapshotPath, s.Stream, s.ShardId)
}

type Snapshot struct {
	SnapshotFilename string
	LocalFile        string
	S3Key            string
	KinesisSeq       string
}

func (s *ShardSnapshotFinder) SnapshotFromS3Key(s3key string) *Snapshot {
	prefixes := []string{s.SnapshotPath, "/", s.Stream, "-", s.ShardId, "-"}
	suffix := ".boltdb"

	trimmed := s3key
	for _, p := range prefixes {
		if ok, t := trimIfPrefixed(trimmed, p); !ok {
			return nil
		} else {
			trimmed = t
		}
	}

	if ok, t := trimIfSuffixed(trimmed, suffix); !ok {
		return nil
	} else {
		trimmed = t
	}

	kinesisSeq := trimmed

	ss := s.SnapshotFromKinesisSeq(kinesisSeq)
	return &ss
}

func (s *ShardSnapshotFinder) SnapshotFromKinesisSeq(kinesisSeq string) Snapshot {
	snapshotFilename := fmt.Sprintf("%s-%s-%s.boltdb", s.Stream, s.ShardId, kinesisSeq)
	s3Key := fmt.Sprintf("%s/%s", s.SnapshotPath, snapshotFilename)
	local := fmt.Sprintf("%s/%s", s.LocalPath, snapshotFilename)
	local, err := filepath.Abs(local)
	if err != nil {
		panic(err)
	}
	return Snapshot{
		S3Key:            s3Key,
		SnapshotFilename: snapshotFilename,
		KinesisSeq:       kinesisSeq,
		LocalFile:        local,
	}
}

func trimIfPrefixed(from, prefix string) (bool, string) {
	if strings.HasPrefix(from, prefix) {
		return true, strings.TrimPrefix(from, prefix)
	} else {
		return false, from
	}
}

func trimIfSuffixed(from, suffix string) (bool, string) {
	if strings.HasSuffix(from, suffix) {
		return true, strings.TrimSuffix(from, suffix)
	} else {
		return false, from
	}
}
