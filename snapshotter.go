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
	"strings"
	"syscall"
	"time"
)

const BootstrapSequence = "00000000000000000000"

type ShardSnapshotter struct {
	s3api          s3iface.S3API
	kapi           kinesisiface.KinesisAPI
	bootstrap      func(*bolt.Tx) (initialKinesisSeq string, err error)
	onStart        func(*bolt.Tx) error
	onRecords      func(*bolt.Tx, *kinesis.GetRecordsOutput) error
	snapshotBucket string
	snapshotPath   string
	localPath      string
	stream         string
	shard          string
	doneLag        int64
}

func (s *ShardSnapshotter) SnapshotShard() error {
	snapshots := []Snapshot{}

	eachPage := func(o *s3.ListObjectsOutput, _ bool) bool {
		log.Printf("component=shard-snapshotter fn=snapshot-shard at=list-objects-page")
		for _, obj := range o.Contents {
			snapshots = append(snapshots, s.SnapshotFromS3Key(*obj.Key))
		}
		return false
	}
	err := s.s3api.ListObjectsPages(&s3.ListObjectsInput{
		Bucket: aws.String(s.snapshotBucket),
		Prefix: aws.String(s.S3Prefix()),
	}, eachPage)

	if err != nil {
		log.Printf("component=shard-snapshotter fn=snapshot-shard at=list-objects-error error=%s", err)
		return err
	}

	var latest *Snapshot
	if len(snapshots) > 0 {
		latest = s.FindLatestSnapshot(snapshots)
		err = s.DownloadSnapshot(*latest)
		if err != nil {
			return err
		}
	} else {
		latest, err = s.BootstrapSnapshot()
		if err != nil {
			return err
		}
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
		updatedSnapshot := s.SnapshotFromKinesisSeq(updatedSeq)
		err = s.FromWorkingCopy(working, updatedSnapshot)
		if err != nil {
			return err
		}
		err = s.UploadSnapshot(updatedSnapshot)
		if err != nil {
			return err
		}
	}
	return err
}

func (s *ShardSnapshotter) FindLatestSnapshot(snapshots []Snapshot) *Snapshot {
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
		return nil
	}
	log.Printf("component=shard-snapshotter fn=find-latest-snapshot at=snapshot snapshot=%s", latestSnap.S3Key)
	return latestSnap
}

func (s *ShardSnapshotter) BootstrapSnapshot() (*Snapshot, error) {
	init := s.SnapshotFromKinesisSeq(BootstrapSequence)
	db, err := bolt.Open(init.LocalFile, 0600, nil)
	if err != nil {
		log.Printf("component=shard-snapshotter fn=bootstrap-snapshot at=bolt-open-error error=%s", err)
		return nil, err
	}
	defer db.Close()

	updatedSeq := ""

	err = db.Update(func(tx *bolt.Tx) error {
		seq, err := s.bootstrap(tx)
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

	snapshot := s.SnapshotFromKinesisSeq(updatedSeq)
	s.FromWorkingCopy(init.LocalFile, snapshot)
	return &snapshot, nil
}

func (s *ShardSnapshotter) DownloadSnapshot(snapshot Snapshot) error {
	out, err := s.s3api.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(s.snapshotBucket),
		Key:    aws.String(snapshot.S3Key),
	})
	if err != nil {
		log.Printf("component=shard-snapshotter fn=download-snapshot at=get-obj-error error=%s", err)
		return err
	}
	f, err := os.OpenFile(snapshot.LocalFile, syscall.O_WRONLY, 777)
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
		err = s.onStart(tx)
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

	it, err := s.kapi.GetShardIterator(&kinesis.GetShardIteratorInput{
		StreamName:             aws.String(s.stream),
		ShardID:                aws.String(s.shard),
		ShardIteratorType:      aws.String(kinesis.ShardIteratorTypeAfterSequenceNumber),
		StartingSequenceNumber: aws.String(startingAfter),
	})

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

		log.Printf("component=shard-snapshotter fn=update-snapshot at=get-records records=%s behind=%d", len(gr.Records), gr.MillisBehindLatest)

		iterator = gr.NextShardIterator

		err = s.onRecords(tx, gr)
		if err != nil {
			log.Printf("component=shard-snapshotter fn=update-snapshot at=on-records-error error=%s", err)
			return "", err
		}

		if r := len(gr.Records); r > 0 {
			latest = *gr.Records[r-1].SequenceNumber
		}

		if *gr.MillisBehindLatest < s.doneLag {
			return latest, nil
		}
	}

}

func (s *ShardSnapshotter) FromWorkingCopy(file string, snapshot Snapshot) error {
	return exec.Command("mv", file, snapshot.LocalFile).Run()
}

func (s *ShardSnapshotter) UploadSnapshot(snapshot Snapshot) error {
	f, err := os.OpenFile(snapshot.LocalFile, syscall.O_RDONLY, 777)
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

func (s *ShardSnapshotter) S3Prefix() string {
	return fmt.Sprintf("%s/%s-%s-", s.snapshotPath, s.stream, s.shard)
}

type Snapshot struct {
	SnapshotFilename string
	LocalFile        string
	S3Key            string
	KinesisSeq       string
}

func (s *ShardSnapshotter) SnapshotFromS3Key(s3key string) Snapshot {
	snapshotFilename := strings.Replace(s3key, s.snapshotPath+"/", "", 1)
	last := strings.Replace(snapshotFilename, fmt.Sprintf("%s-%s-", s.stream, s.shard), "", 1)
	kinesisSeq := strings.Replace(last, ".boltdb", "", 1)
	return Snapshot{
		S3Key:            s3key,
		SnapshotFilename: snapshotFilename,
		KinesisSeq:       kinesisSeq,
		LocalFile:        fmt.Sprintf("%s/%s", s.localPath, snapshotFilename),
	}
}

func (s *ShardSnapshotter) SnapshotFromKinesisSeq(kinesisSeq string) Snapshot {
	snapshotFilename := fmt.Sprintf("%s-%s-%s.boltdb", s.stream, s.shard, kinesisSeq)
	s3Key := fmt.Sprintf("%s/%s", s.snapshotPath, snapshotFilename)
	local := fmt.Sprintf("%s/%s", s.localPath, snapshotFilename)
	return Snapshot{
		S3Key:            s3Key,
		SnapshotFilename: snapshotFilename,
		KinesisSeq:       kinesisSeq,
		LocalFile:        local,
	}
}
