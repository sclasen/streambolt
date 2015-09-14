package streambolt

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/boltdb/bolt"
	"io"
	"log"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

const BootstrapSequence = "00000000000000000000"

type SnapshotGenerator interface {
	Bootstrap(*bolt.Tx) (initialKinesisSeq string, err error)
	OnStart(*bolt.Tx) error
	OnRecords(*bolt.Tx, *kinesis.GetRecordsOutput) error
}

type ShardSnapshotFinder struct {
	s3api          s3iface.S3API
	kapi           kinesisiface.KinesisAPI
	snapshotBucket string
	snapshotPath   string
	localPath      string
	stream         string
	shard          string
}

type ShardSnapshotter struct {
	s3api          s3iface.S3API
	kapi           kinesisiface.KinesisAPI
	generator      SnapshotGenerator
	snapshotBucket string
	snapshotPath   string
	localPath      string
	stream         string
	shard          string
	doneLag        int64
}

func (s *ShardSnapshotter) Finder() *ShardSnapshotFinder {
	return &ShardSnapshotFinder{
		s3api:          s.s3api,
		kapi:           s.kapi,
		snapshotBucket: s.snapshotBucket,
		snapshotPath:   s.snapshotPath,
		localPath:      s.localPath,
		stream:         s.stream,
		shard:          s.shard,
	}
}

func (s *ShardSnapshotter) SnapshotShard() error {
	finder := s.Finder()
	latest, err := finder.FindLatestSnapshot()
	if err != nil {
		return err
	}

	if latest != nil {
		err = finder.DownloadSnapshot(*latest)
		if err != nil {
			return err
		}
	} else {
		l, err := s.BootstrapSnapshot()
		if err != nil {
			return err
		}
		latest = l
	}

	if latest != nil {
		working, err := s.ToWorkingCopy(*latest)
		if err != nil {
			return err
		}
		updatedSeq, err := s.UpdateWorkingCopy(working, latest.KinesisSeq)
		if err != nil {
			return err
		}
		updatedSnapshot := finder.SnapshotFromKinesisSeq(updatedSeq)
		err = s.FromWorkingCopy(working, updatedSnapshot)
		if err != nil {
			return err
		}
		err = s.UploadSnapshot(updatedSnapshot)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *ShardSnapshotFinder) FindLatestSnapshot() (*Snapshot, error) {
	snapshots := []Snapshot{}
	eachPage := func(o *s3.ListObjectsOutput, _ bool) bool {
		log.Printf("component=shard-snapshotter fn=snapshot-shard at=list-objects-page")
		for _, obj := range o.Contents {
			if ss := s.SnapshotFromS3Key(*obj.Key); ss != nil {
				snapshots = append(snapshots, *ss)
				log.Printf("component=shard-snapshotter fn=snapshot-shard at=snapshot-key key=%s", *obj.Key)
			} else {
				log.Printf("component=shard-snapshotter fn=snapshot-shard at=non-snapshot-key key=%s", *obj.Key)
			}
		}
		return false
	}
	err := s.s3api.ListObjectsPages(&s3.ListObjectsInput{
		Bucket: aws.String(s.snapshotBucket),
		Prefix: aws.String(s.S3Prefix()),
	}, eachPage)

	if err != nil {
		log.Printf("component=shard-snapshotter fn=snapshot-shard at=list-objects-error error=%s", err)
		return nil, err
	}
	latest := big.NewInt(0)
	var latestSnap *Snapshot

	for _, ss := range snapshots {
		seq, ok := big.NewInt(0).SetString(ss.KinesisSeq, 10)
		if ok && latest.Cmp(seq) < 0 {
			latest = seq
			temp := ss
			latestSnap = &temp
		}
	}

	if latest.Cmp(big.NewInt(0)) == 0 {
		log.Printf("component=shard-snapshotter fn=find-latest-snapshot at=no-snapshots")
		return nil, nil
	}
	log.Printf("component=shard-snapshotter fn=find-latest-snapshot at=snapshot snapshot=%s", latestSnap.S3Key)
	return latestSnap, nil
}

func (s *ShardSnapshotter) BootstrapSnapshot() (*Snapshot, error) {
	init := s.Finder().SnapshotFromKinesisSeq(BootstrapSequence)
	db, err := bolt.Open(init.LocalFile, 0600, nil)
	if err != nil {
		log.Printf("component=shard-snapshotter fn=bootstrap-snapshot at=bolt-open-error error=%s", err)
		return nil, err
	}
	defer db.Close()

	updatedSeq := ""

	err = db.Update(func(tx *bolt.Tx) error {
		seq, err := s.generator.Bootstrap(tx)
		if err != nil {
			log.Printf("component=shard-snapshotter fn=bootstrap-snapshot at=error error=%s", err)
			return err
		}
		updatedSeq = seq
		return nil
	})

	if err != nil {
		return nil, err
	}

	snapshot := s.Finder().SnapshotFromKinesisSeq(updatedSeq)
	s.FromWorkingCopy(init.LocalFile, snapshot)
	return &snapshot, nil
}

func (s *ShardSnapshotFinder) DownloadSnapshot(snapshot Snapshot) error {
	out, err := s.s3api.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(s.snapshotBucket),
		Key:    aws.String(snapshot.S3Key),
	})
	if err != nil {
		log.Printf("component=shard-snapshotter fn=download-snapshot at=get-obj-error error=%s", err)
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
		log.Printf("component=shard-snapshotter fn=download-snapshot at=copoy-error error=%s", err)
	}

	return err
}

func (s *ShardSnapshotter) ToWorkingCopy(snapshot Snapshot) (string, error) {
	copy := (fmt.Sprintf("%s/working-%d", s.localPath, time.Now().UnixNano()))
	return copy, exec.Command("mv", snapshot.LocalFile, copy).Run()
}

func (s *ShardSnapshotter) UpdateWorkingCopy(workingCopyFilename string, lastSequence string) (string, error) {
	db, err := bolt.Open(workingCopyFilename, 0600, nil)
	if err != nil {
		log.Printf("component=shard-snapshotter fn=update-working-copy at=bolt-open-error error=%s", err)
		return "", err
	}
	defer db.Close()

	updatedSeq := lastSequence

	err = db.Update(func(tx *bolt.Tx) error {
		err = s.generator.OnStart(tx)
		if err != nil {
			log.Printf("component=shard-snapshotter fn=update-working-copy at=on-start-error error=%s", err)
			return err
		}
		updatedSeq, err = s.UpdateSnapshot(tx, lastSequence)
		return err
	})

	return updatedSeq, err
}

func (s *ShardSnapshotter) UpdateSnapshot(tx *bolt.Tx, startingAfter string) (string, error) {

	latest := startingAfter
	log.Printf("component=shard-snapshotter fn=update-snapshot at=get-iterator after=%s", startingAfter)
	gsi := &kinesis.GetShardIteratorInput{
		StreamName:             aws.String(s.stream),
		ShardID:                aws.String(s.shard),
		ShardIteratorType:      aws.String(kinesis.ShardIteratorTypeAfterSequenceNumber),
		StartingSequenceNumber: aws.String(startingAfter),
	}

	if startingAfter == BootstrapSequence {
		gsi.ShardIteratorType = aws.String(kinesis.ShardIteratorTypeTrimHorizon)
		gsi.StartingSequenceNumber = nil
	}

	it, err := s.kapi.GetShardIterator(gsi)

	if err != nil {
		log.Printf("component=shard-snapshotter fn=update-snapshot at=get-iterator-error error=%s", err)
		return "", err
	}

	iterator := it.ShardIterator

	for {
		gr, err := s.kapi.GetRecords(&kinesis.GetRecordsInput{
			ShardIterator: iterator,
		})
		if err != nil {
			log.Printf("component=shard-snapshotter fn=update-snapshot at=get-records-error error=%s", err)
			return "", err
		}

		log.Printf("component=shard-snapshotter fn=update-snapshot at=get-records records=%d behind=%d", len(gr.Records), *gr.MillisBehindLatest)

		iterator = gr.NextShardIterator

		err = s.generator.OnRecords(tx, gr)
		if err != nil {
			log.Printf("component=shard-snapshotter fn=update-snapshot at=on-records-error error=%s", err)
			return "", err
		}

		if r := len(gr.Records); r > 0 {
			latest = *gr.Records[r-1].SequenceNumber
		}

		if *gr.MillisBehindLatest < s.doneLag {
			log.Printf("component=shard-snapshotter fn=update-snapshot at=done behind=%d done-lag=%d", *gr.MillisBehindLatest, s.doneLag)
			return latest, nil
		}
	}

}

func (s *ShardSnapshotter) FromWorkingCopy(file string, snapshot Snapshot) error {
	return exec.Command("mv", file, snapshot.LocalFile).Run()
}

func (s *ShardSnapshotter) UploadSnapshot(snapshot Snapshot) error {
	f, err := os.OpenFile(snapshot.LocalFile, syscall.O_RDONLY, 0666)
	defer f.Close()
	if err != nil {
		return err
	}

	_, err = s.s3api.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(s.snapshotBucket),
		Key:    aws.String(snapshot.S3Key),
		Body:   f,
	})

	return err

}

func (s *ShardSnapshotFinder) S3Prefix() string {
	return fmt.Sprintf("%s/%s-%s-", s.snapshotPath, s.stream, s.shard)
}

type Snapshot struct {
	SnapshotFilename string
	LocalFile        string
	S3Key            string
	KinesisSeq       string
}

func (s *ShardSnapshotFinder) SnapshotFromS3Key(s3key string) *Snapshot {
	prefixes := []string{s.snapshotPath, "/", s.stream, "-", s.shard, "-"}
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
	snapshotFilename := fmt.Sprintf("%s-%s-%s.boltdb", s.stream, s.shard, kinesisSeq)
	s3Key := fmt.Sprintf("%s/%s", s.snapshotPath, snapshotFilename)
	local := fmt.Sprintf("%s/%s", s.localPath, snapshotFilename)
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
