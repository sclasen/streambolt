package streambolt

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/boltdb/bolt"
	"github.com/docker/docker/pkg/random"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestSnapshots(t *testing.T) {
	s := &ShardSnapshotter{
		Stream:       "my-stream-name",
		ShardID:      "shardId-000000000000",
		SnapshotPath: "snapshots",
		LocalPath:    "local",
	}
	last := "49553601453818580880174946772985676824316826975981273090"

	key := s.Finder().SnapshotFromS3Key(fmt.Sprintf("%s/%s-%s-%s.boltdb", s.SnapshotPath, s.Stream, s.ShardID, last))
	if key == nil {
		t.Fatal("NULL KEY")
	}
	seq := s.Finder().SnapshotFromKinesisSeq(last)

	for _, snapshot := range []Snapshot{*key, seq} {
		t.Log(snapshot)
		if snapshot.KinesisSeq != last {
			t.Fatal(snapshot.KinesisSeq, "NOT", last)
		}

		if local, err := filepath.Abs(fmt.Sprintf("%s/%s-%s-%s.boltdb", s.LocalPath, s.Stream, s.ShardID, last)); err != nil || local != snapshot.LocalFile {
			t.Fatal("LOCAL NOT", snapshot.LocalFile)
		}

		if snapshot.SnapshotFilename != fmt.Sprintf("%s-%s-%s.boltdb", s.Stream, s.ShardID, last) {
			t.Fatal("SNAP NOT", snapshot.SnapshotFilename)
		}

		if snapshot.S3Key != fmt.Sprintf("%s/%s-%s-%s.boltdb", s.SnapshotPath, s.Stream, s.ShardID, last) {
			t.Fatal("S3  NOT", snapshot.S3Key)
		}

		if !strings.HasPrefix(snapshot.S3Key, s.Finder().S3Prefix()) {
			t.Fatal("PREFIX NOT", snapshot.S3Key)
		}
	}
}

func TestIntegration(t *testing.T) {
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" || os.Getenv("AWS_ACCESS_KEY_ID") == "" {
		t.Log("No AWS creds, skipping integration test")
		return
	}
	c := aws.NewConfig().WithRegion("us-east-1")
	s3c := s3.New(c)
	kc := kinesis.New(c)

	stream := fmt.Sprintf("snapshot-%d", time.Now().Unix())
	bucket := os.Getenv("SNAPSHOT_BUCKET")
	snapshotPath := "snapshots"
	localPath := "./local"

	_, err := kc.CreateStream(&kinesis.CreateStreamInput{
		ShardCount: aws.Int64(1),
		StreamName: aws.String(stream),
	})

	if err != nil {
		t.Fatal(err)
	}

	_, err = s3c.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(os.Getenv("SNAPSHOT_BUCKET")),
	})

	if err != nil {
		t.Fatal(err)
	}

	defer kc.DeleteStream(&kinesis.DeleteStreamInput{
		StreamName: aws.String(stream),
	})

	var snapshotter *ShardSnapshotter

	for {
		o, err := kc.DescribeStream(&kinesis.DescribeStreamInput{
			StreamName: aws.String(stream),
		})
		if err != nil {
			t.Fatal(err)
		}

		log.Printf("stream-status=%s", *o.StreamDescription.StreamStatus)

		if *o.StreamDescription.StreamStatus == kinesis.StreamStatusActive {
			snapshotter = &ShardSnapshotter{
				KinesisClient:  kc,
				S3Client:       s3c,
				SnapshotBucket: bucket,
				SnapshotPath:   snapshotPath,
				LocalPath:      localPath,
				Stream:         stream,
				ShardID:        *o.StreamDescription.Shards[0].ShardID,
				DoneLag:        10,
				Generator:      &TestSnapshotGen{},
			}
			break
		}
		time.Sleep(1 * time.Second)
	}

	if snapshotter == nil {
		t.Fatal("NIL SNAPSHOTTER")
	}

	total, err := PutData(snapshotter.KinesisClient, snapshotter.Stream)
	if err != nil {
		t.Fatal(err)
	}

	ss, err := snapshotter.SnapshotShard()
	if err != nil {
		t.Fatal(err)
	}

	finder := snapshotter.Finder()
	ss, err = finder.FindLatestSnapshot()
	if err != nil || ss == nil {
		t.Fatal(err, ss)
	}

	err = os.Remove(ss.LocalFile)
	if err != nil {
		t.Fatal(err)
	}

	err = finder.DownloadSnapshot(*ss)
	if err != nil {
		t.Fatal(err)
	}

	verifyTotal, err := TotalSnapshot(*ss)
	if err != nil {
		t.Fatal(err)
	}

	if total != verifyTotal {
		t.Fatal(total, verifyTotal)
	}

	secondTotal, err := PutData(snapshotter.KinesisClient, snapshotter.Stream)
	if err != nil {
		t.Fatal(err)
	}

	ss2, err := snapshotter.SnapshotShard()
	if err != nil {
		t.Fatal(err)
	}

	ss2, err = finder.FindLatestSnapshot()
	if err != nil || ss2 == nil {
		t.Fatal(err, ss2)
	}

	err = os.Remove(ss2.LocalFile)
	if err != nil {
		t.Fatal(err)
	}

	err = finder.DownloadSnapshot(*ss2)
	if err != nil {
		t.Fatal(err)
	}

	secondVerifyTotal, err := TotalSnapshot(*ss2)
	if err != nil {
		t.Fatal(err)
	}

	if total+secondTotal != secondVerifyTotal {
		t.Fatal(total, secondTotal, secondVerifyTotal)
	}

	err = os.Remove(ss2.LocalFile)
	if err != nil {
		t.Fatal(err)
	}

}

func PutData(kc kinesisiface.KinesisAPI, stream string) (int, error) {
	r := random.Rand
	total := 0
	records := []*kinesis.PutRecordsRequestEntry{}
	for i := 1; i <= 100; i++ {
		k := strconv.Itoa(i)
		v := r.Int()
		total += v
		r := &kinesis.PutRecordsRequestEntry{
			PartitionKey: aws.String(k),
			Data:         []byte(strconv.Itoa(v)),
		}
		records = append(records, r)
	}

	_, err := kc.PutRecords(&kinesis.PutRecordsInput{
		StreamName: aws.String(stream),
		Records:    records,
	})

	if err != nil {
		return 0, err
	}

	return total, nil
}

func TotalSnapshot(ss Snapshot) (int, error) {
	db, err := bolt.Open(ss.LocalFile, 0666, nil)
	if err != nil {
		return 0, err
	}
	defer db.Close()

	verifyTotal := 0

	db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(keycounts).Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			//log.Printf("key=%s value=%s\n", k, v)
			num, err := strconv.Atoi(string(v))
			if err != nil {
				return err
			}
			verifyTotal += num
		}
		return nil
	})

	return verifyTotal, nil
}

var keycounts = []byte("keycounts")

type TestSnapshotGen struct{}

func (s *TestSnapshotGen) Bootstrap(tx *bolt.Tx) (initialKinesisSeq string, err error) {
	return BootstrapSequence, nil
}

func (s *TestSnapshotGen) OnStart(tx *bolt.Tx) error {
	_, err := tx.CreateBucketIfNotExists(keycounts)
	return err
}

func (s *TestSnapshotGen) OnRecords(tx *bolt.Tx, gro *kinesis.GetRecordsOutput) error {
	bucket := tx.Bucket(keycounts)

	for _, r := range gro.Records {
		key := []byte(*r.PartitionKey)
		value := string(r.Data)
		parsedValue, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		old := bucket.Get(key)
		if old != nil {
			parsedOld, err := strconv.Atoi(string(old))
			if err != nil {
				return err
			}
			parsedValue += parsedOld
		}
		//log.Printf("onRecord=%s value=%d", *r.PartitionKey, parsedValue)
		bucket.Put(key, []byte(strconv.Itoa(parsedValue)))

	}
	return nil
}
