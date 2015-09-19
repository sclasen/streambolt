package main

import (
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/codegangsta/cli"
	"os"
)

/*
streambolt count bucket file
streambolt list  bucket startInclusivePrefix endExclusivePrefix file
streambolt get   bucket key file
streambolt all key bucket bucket file
*/

var bucket = cli.StringFlag{
	Name:  "bucket, b",
	Usage: "bucket to inspect",
}

var key = cli.StringFlag{
	Name:  "key, k",
	Usage: "key to inspect",
}

var db = cli.StringFlag{
	Name:  "db, d",
	Usage: ".boltdb file to inspect",
}

var start = cli.StringFlag{
	Name:  "start, s",
	Usage: "inclusive start key range",
}

var end = cli.StringFlag{
	Name:  "end, e",
	Usage: "exclusive end key range",
}

func main() {
	app := cli.NewApp()
	app.Name = "streambolt"
	app.Usage = "inspect streambolt generated snapshots"
	app.Commands = []cli.Command{
		{
			Name:    "count",
			Aliases: []string{"c"},
			Usage:   "count the keys in a bucket",
			Action:  Count,
			Flags:   []cli.Flag{bucket, db},
		},
		{
			Name:    "list",
			Aliases: []string{"l"},
			Usage:   "show the values for a key range in a bucket",
			Action:  Count,
			Flags:   []cli.Flag{bucket, db, start, end},
		},
		{
			Name:    "get",
			Aliases: []string{"g"},
			Usage:   "get the value for a key in a bucket",
			Action:  Count,
			Flags:   []cli.Flag{bucket, db, key},
		},
		{
			Name:    "all",
			Aliases: []string{"a"},
			Usage:   "get the values for a key in many buckets",
			Action:  Count,
			Flags:   []cli.Flag{bucket, db, key},
		},
	}
	app.Run(os.Args)
}

func withBolt(c *cli.Context, view func(*bolt.Tx) error) {
	db := c.String("db")
	if db == "" {
		fmt.Println("No --db")
	} else {
		db, err := bolt.Open(db, 0600, nil)
		if err != nil {
			fmt.Println(err)
		} else {
			defer db.Close()
			err := db.View(view)
			if err != nil {
				fmt.Println(err)
			}
		}
	}
}

func Count(c *cli.Context) {
	b := c.String("bucket")
	if b == "" {
		println("No --bucket")
	} else {
		withBolt(c, func(tx *bolt.Tx) error {
			count := 0
			bb := tx.Bucket([]byte(b))
			bb.ForEach(func(k, v []byte) error {
				count++
				return nil
			})
			fmt.Printf("bucket: %s key-count: %d\n", b, count)
			return nil
		})
	}
}
