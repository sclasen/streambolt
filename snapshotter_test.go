package streambolt

import (
	"fmt"
	"testing"
	"strings"
)

func TestSnapshots(t *testing.T) {
	s := &ShardSnapshotter{
		stream:       "my-stream-name",
		shard:        "shardId-000000000000",
		snapshotPath: "snapshots",
		localPath:    "local",
	}
	last := "49553601453818580880174946772985676824316826975981273090"

	key := s.SnapshotFromS3Key(fmt.Sprintf("%s/%s-%s-%s.boltdb", s.snapshotPath, s.stream, s.shard, last))
	seq := s.SnapshotFromKinesisSeq(last)

	for _, snapshot := range []Snapshot{key, seq} {
		t.Log(snapshot)
		if snapshot.KinesisSeq != last {
			t.Fatal(snapshot.KinesisSeq, "NOT", last)
		}

		if snapshot.LocalFile != fmt.Sprintf("%s/%s-%s-%s.boltdb", s.localPath, s.stream, s.shard, last) {
			t.Fatal("LOCAL NOT", snapshot.LocalFile)
		}

		if snapshot.SnapshotFilename != fmt.Sprintf("%s-%s-%s.boltdb", s.stream, s.shard, last) {
			t.Fatal("SNAP NOT", snapshot.SnapshotFilename)
		}

		if snapshot.S3Key != fmt.Sprintf("%s/%s-%s-%s.boltdb", s.snapshotPath, s.stream, s.shard, last) {
			t.Fatal("S3  NOT", snapshot.S3Key)
		}

		if ! strings.HasPrefix(snapshot.S3Key, s.S3Prefix()) {
			t.Fatal("PREFIX NOT", snapshot.S3Key)
		}
	}
}


