package main

import (
	"flag"
	"log"

	"github.com/ksegun/rotatesnapshot"
)

var (
	gcpProject string
	filter     string
)

func init() {
	flag.StringVar(&gcpProject, "project", "", "The Google Cloud Platform project name to use for this invocation.")
	flag.StringVar(&filter, "filter", "", "Option filter string for the snapshot listing.")
}

func main() {
	flag.Parse()
	client, err := rotatesnapshot.NewGCPProvider(gcpProject, filter)
	if err != nil {
		log.Fatalf("%+v", err)
	}
	if err = rotatesnapshot.Rotate(client); err != nil {
		log.Fatalf("%+v", err)
	}
}
