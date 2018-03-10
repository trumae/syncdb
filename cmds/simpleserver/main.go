package main

import (
	"flag"
	"log"
	"time"

	"github.com/trumae/syncdb"
)

func main() {
	filedb := ""
	flag.StringVar(&filedb, "filedb", "file.db", "database file")

	company := ""
	flag.StringVar(&company, "company", "company1", "company id")

	node := ""
	flag.StringVar(&node, "node", "id1", "node id")

	flag.Parse()

	db1, err := syncdb.New(filedb)
	if err != nil {
		log.Fatal(err)
	}

	db1.Begin()
	db1.Set("company", company)
	db1.Set("id", node)
	db1.Commit()

	for {
		err = db1.Sync()
		if err != nil {
			log.Fatal(err)
		}

		time.Sleep(300 * time.Second)
	}
}
