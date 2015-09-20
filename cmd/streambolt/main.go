package main

import (
	"bytes"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/codegangsta/cli"
	"os"
	"strings"
)

/*
streambolt count bucket file
streambolt keys  bucket prefix file
streambolt list  bucket prefix file
streambolt get   bucket key file
streambolt all key bucket bucket file
*/

var bucket = cli.StringFlag{
	Name:  "bucket, b",
	Usage: "bucket to inspect",
}

var buckets = cli.StringFlag{
	Name:  "buckets, bs",
	Usage: "comma seperated buckets to inspect",
}

var key = cli.StringFlag{
	Name:  "key, k",
	Usage: "key to inspect",
}

var db = cli.StringFlag{
	Name:   "db, d",
	Usage:  ".boltdb file to inspect",
	EnvVar: "BOLTDB",
}

var start = cli.StringFlag{
	Name:  "start, s",
	Usage: "inclusive start key range",
}

var end = cli.StringFlag{
	Name:  "end, e",
	Usage: "exclusive end key range",
}

var prefix = cli.StringFlag{
	Name:  "prefix, p",
	Usage: "key prefix",
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
			Name:    "keys",
			Aliases: []string{"k"},
			Usage:   "show the keys (for a key prefix) in a bucket",
			Action:  Keys,
			Flags:   []cli.Flag{bucket, db, prefix},
		},
		{
			Name:    "list",
			Aliases: []string{"l"},
			Usage:   "show the values (for a key prefix) in a bucket",
			Action:  List,
			Flags:   []cli.Flag{bucket, db, prefix},
		},
		{
			Name:    "get",
			Aliases: []string{"g"},
			Usage:   "get the value for a key in a bucket",
			Action:  Get,
			Flags:   []cli.Flag{bucket, db, key},
		},
		{
			Name:    "all",
			Aliases: []string{"a"},
			Usage:   "get the values for a key in many buckets, first bucket is the keylist",
			Action:  All,
			Flags:   []cli.Flag{buckets, db, key},
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

func withBucket(c *cli.Context, view func(string, *bolt.Tx) error) {
	b := c.String("bucket")
	if b == "" {
		println("No --bucket")
	} else {
		withBolt(c, func(tx *bolt.Tx) error {
			return view(b, tx)
		})
	}
}

func withBuckets(c *cli.Context, view func([]string, *bolt.Tx) error) {
	b := c.String("buckets")
	if b == "" {
		println("No --buckets")
	} else {
		withBolt(c, func(tx *bolt.Tx) error {
			return view(strings.Split(b, ","), tx)
		})
	}
}

func Count(c *cli.Context) {
	withBucket(c, func(bucket string, tx *bolt.Tx) error {
		count := 0
		bb := tx.Bucket([]byte(bucket))
		bb.ForEach(func(k, v []byte) error {
			count++
			return nil
		})
		fmt.Printf("bucket=%s key-count=%d\n", bucket, count)
		return nil
	})
}

func Keys(c *cli.Context) {
	withBucket(c, func(bucket string, tx *bolt.Tx) error {
		bb := tx.Bucket([]byte(bucket))
		cu := bb.Cursor()
		if p := c.String("prefix"); p != "" {
			for k, _ := cu.Seek(b(p)); bytes.HasPrefix(k, b(p)); k, _ = cu.Next() {
				fmt.Println(s(k))
			}
		} else {
			for k, _ := cu.First(); k != nil; k, _ = cu.Next() {
				fmt.Println(s(k))
			}
		}
		return nil
	})
}

func Get(c *cli.Context) {
	withBucket(c, func(bucket string, tx *bolt.Tx) error {
		bb := tx.Bucket([]byte(bucket))
		if key := c.String("key"); key != "" {
			fmt.Println(s(bb.Get(b(key))))
		}
		return nil
	})
}

func List(c *cli.Context) {
	withBucket(c, func(bucket string, tx *bolt.Tx) error {
		bb := tx.Bucket([]byte(bucket))
		cu := bb.Cursor()
		if p := c.String("prefix"); p != "" {
			for k, v := cu.Seek(b(p)); bytes.HasPrefix(k, b(p)); k, v = cu.Next() {
				fmt.Printf("key=%s value=%s\n", s(k), s(v))
			}
		} else {
			for k, v := cu.First(); k != nil; k, v = cu.Next() {
				fmt.Printf("key=%s value=%s\n", s(k), s(v))
			}
		}
		return nil
	})
}

func All(c *cli.Context) {
	withBuckets(c, func(buckets []string, tx *bolt.Tx) error {
		if k := c.String("key"); k != "" {
			bucketMap := map[string]*bolt.Bucket{}
			for _, ab := range buckets {
				bucketMap[ab] = tx.Bucket(b(ab))
			}
			fmt.Println(others(bucketMap, b(k)))
		}
		return nil
	})
}

func others(o map[string]*bolt.Bucket, key []byte) string {
	ret := ""
	for k, b := range o {
		v := b.Get(key)
		ret = ret + fmt.Sprintf("%s=%s", k, s(v))
	}
	return ret
}

func b(s string) []byte {
	return []byte(s)
}

func s(b []byte) string {
	return string(b)
}
