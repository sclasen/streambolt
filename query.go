package streambolt

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/boltdb/bolt"
	"log"
	"time"
)

type QueryDBUpdater interface {
	OnStart(*bolt.Tx) error
	OnRecords(*bolt.Tx, *kinesis.GetRecordsOutput) error
}

type ShardQueryDB struct {
	Finder          *ShardSnapshotFinder
	Updater         QueryDBUpdater
	KinesisClient   kinesisiface.KinesisAPI
	UpdateInterval  time.Duration
	stopUpdating    chan struct{}
	stoppedUpdating chan struct{}
	db              *bolt.DB
}

func (d *ShardQueryDB) Query(queryFn func(*bolt.Tx) error) error {
	if d.db == nil {
		return fmt.Errorf("database is nil")
	}

	return d.db.View(queryFn)
}

func (d *ShardQueryDB) Start() error {
	d.stoppedUpdating = make(chan struct{}, 1)
	d.stopUpdating = make(chan struct{}, 1)
	ss, err := d.Finder.FindLatestSnapshot()
	if err != nil {
		log.Printf("component=shard-query fn=start at=find-snapshot-error error=%s", err)
		return err
	}

	if ss != nil {
		err = d.Finder.DownloadSnapshot(*ss)
		if err != nil {
			log.Printf("component=shard-query fn=start at=download-snapshot-error error=%s", err)
			return err
		}
		log.Printf("component=shard-query fn=start at=downloaded-snapshot snapshot=%s", ss.LocalFile)

	} else {
		log.Printf("component=shard-query fn=start at=no-shapshots msg=%q", "using kinesis only")
		bs := d.Finder.SnapshotFromKinesisSeq(BootstrapSequence)
		ss = &bs
	}

	d.db, err = bolt.Open(ss.LocalFile, 0600, nil)

	if err != nil {
		log.Printf("component=shard-query fn=start at=open-db-error error=%s", err)
		return err
	}

	d.db.Update(d.Updater.OnStart)

	go d.applyUpdates(ss.KinesisSeq)

	return nil
}

func (d *ShardQueryDB) Stop() {
	log.Printf("component=shard-query fn=stop at=stopping")
	close(d.stopUpdating)
	<-d.stoppedUpdating
	d.db.Close()
	d.db = nil
	log.Printf("component=shard-query fn=stop at=stopped")
}

func (d *ShardQueryDB) applyUpdates(startingAfter string) {

	latest := startingAfter
	trigger := make(chan struct{}, 1)
	fire := struct{}{}
	trigger <- fire
	for {
		select {
		case <-d.stopUpdating:
			close(d.stoppedUpdating)
			return
		case <-trigger:
			log.Printf("component=shard-query fn=update-snapshot at=get-iterator after=%s", latest)
			gsi := &kinesis.GetShardIteratorInput{
				StreamName:             aws.String(d.Finder.Stream),
				ShardId:                aws.String(d.Finder.ShardId),
				ShardIteratorType:      aws.String(kinesis.ShardIteratorTypeAfterSequenceNumber),
				StartingSequenceNumber: aws.String(latest),
			}

			if latest == BootstrapSequence {
				gsi.ShardIteratorType = aws.String(kinesis.ShardIteratorTypeTrimHorizon)
				gsi.StartingSequenceNumber = nil
			}

			it, err := d.KinesisClient.GetShardIterator(gsi)

			if err != nil {
				log.Printf("component=shard-query fn=update-snapshot at=get-iterator-error error=%s", err)
				time.AfterFunc(d.UpdateInterval, func() {
					trigger <- fire
				})
				continue
			}

			iterator := it.ShardIterator
			trigger <- fire
			for {
				select {
				case <-d.stopUpdating:
					close(d.stoppedUpdating)
					return
				case <-trigger:
					gr, err := d.KinesisClient.GetRecords(&kinesis.GetRecordsInput{
						ShardIterator: iterator,
					})
					if err != nil {
						if aerr, ok := err.(awserr.Error); ok {
							if aerr.Code() == "ExpiredIteratorException" {
								log.Printf("component=shard-query fn=update-snapshot at=expired-iterator")
								trigger <- fire
								break
							}
						} else {
							log.Printf("component=shard-query fn=update-snapshot at=get-records-error error=%s", err)
							time.AfterFunc(d.UpdateInterval, func() {
								trigger <- fire
							})
							break
						}
					}

					log.Printf("component=shard-query fn=update-snapshot at=get-records records=%d behind=%d", len(gr.Records), *gr.MillisBehindLatest)

					iterator = gr.NextShardIterator

					if r := len(gr.Records); r > 0 {
						err = d.db.Update(func(tx *bolt.Tx) error {
							return d.Updater.OnRecords(tx, gr)
						})
					}

					if err != nil {
						log.Printf("component=shard-query fn=update-snapshot at=on-records-error error=%s", err)
					}

					if r := len(gr.Records); r > 0 {
						latest = *gr.Records[r-1].SequenceNumber
					}

					if len(gr.Records) < 10000 {
						time.AfterFunc(d.UpdateInterval, func() {
							trigger <- fire
						})
					} else {
						//catching up
						trigger <- fire
					}
				}
			}
		}
	}
}
