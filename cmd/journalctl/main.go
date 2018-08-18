package main

import (
	"bytes"
	"io"
	"log"
	"os"
	"time"

	"github.com/smarterclayton/journalread"
)

func main() {
	log.SetFlags(0)
	log.SetOutput(os.Stdout)
	if len(os.Args) != 2 {
		log.Fatalf("You must specify the path to a journal file as the first argument")
	}
	f, err := os.Open(os.Args[1])
	if err != nil {
		log.Fatalf("Unable to open journal file: %v", err)
	}
	r, err := journalread.NewEntryReader(f)
	if err != nil {
		log.Fatalf("Unable to open journal file: %v", err)
	}
	for {
		entry, err := r.NextEntry()
		if err == io.EOF {
			return
		}
		if err != nil {
			log.Fatalf("Failure to read entry in journal: %v", err)
		}
		data, err := r.FirstData("MESSAGE=", entry.Items...)
		if err != nil {
			log.Fatalf("warning: Failure to read data for journal entry: %v", err)
			continue
		}
		if data != nil {
			if i := bytes.IndexByte(data, 0x00); i != -1 {
				data = data[:i]
			}
			log.Printf("%-30s %s", time.Unix(int64(entry.Realtime/1000000), int64(entry.Realtime%1000000)).UTC().Format(time.RFC3339Nano), string(data))
		}
	}
}
