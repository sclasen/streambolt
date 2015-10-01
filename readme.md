streambolt
==========

snapshot kinesis stream shards into boltdb files that are stored in s3.

bootstrap your kinesis consumer from those boltdb files stored in s3.

built with aws-sdk-go.


usage
-----

Snapshot Producer

```go

type MySnapshotGen struct{}

func (s *MySnapshotGen) Bootstrap(tx *bolt.Tx) (initialKinesisSeq string, err error) {
    //update the bolt tx, and return the latest sequence number considered to be in the snapshot.
    //return BootstrapSequence to start from the beginning of the stream.
	return BootstrapSequence, nil
}

func (s *MySnapshotGen) OnStart(tx *bolt.Tx) error {
    //update the bolt tx, one time setup before it recieves records.
	_, err := tx.CreateBucketIfNotExists([]byte("a-bucket"))
	return err
}

func (s *MySnapshotGen) OnRecords(tx *bolt.Tx, gro *kinesis.GetRecordsOutput) error {
    //update the bolt tx with records from kinesis
	bucket := tx.Bucket("a-bucket")
	for _, r := range gro.Records {
	    //update the snapshot
	}
	return nil
}

snapshotter := &ShardSnapshotter{
				KinesisClient:  kinesis.New(...),
				S3Client:       s3.New(...),
				SnapshotBucket: bucket,
				SnapshotPath:   snapshotPath,
				LocalPath:      localPath,
				Stream:         stream,
				ShardId:        shardId,
				DoneLag:        10,
				Generator:      &MySnapshotGen{},
			}

//finds/downloads latest snapshot, or bootstraps it.
//updates the snapshot until the kinesis consumer is caught up.
//uploads the new latest snapshot.
snapshotter.SnapshotShard()

```


Snapshot Consumer



```go
finder := &ShardSnapshotFinder{
				S3Client:       s3c,
				SnapshotBucket: bucket,
				SnapshotPath:   snapshotPath,
				LocalPath:      localPath,
				Stream:         stream,
				Shard:          shardId,
			}

ss, err := finder.FindLatestSnapshot()

err = finder.DownloadSnapshot(*ss)

db, err := bolt.Open(ss.LocalFile, 0666, nil)

//do things with the db.
```