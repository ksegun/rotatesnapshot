package rotatesnapshot

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"golang.org/x/oauth2/google"
	"google.golang.org/api/compute/v1"
)

// GCP ...
type GCP struct {
	ctx     context.Context
	client  *http.Client
	project string
	filter  string
}

// NewGCPProvider ...
func NewGCPProvider(p, f string) *GCP {
	ctx := context.Background()

	c, err := google.DefaultClient(ctx, compute.CloudPlatformScope)
	if err != nil {
		log.Fatal(err)
	}

	return &GCP{ctx: ctx, client: c, project: p, filter: f}
}

// ListSnapshots ...
func (g *GCP) ListSnapshots() ([]Snapshot, error) {
	computeService, err := compute.New(g.client)
	if err != nil {
		log.Fatal(err)
	}

	// Project ID for this request.
	project := g.project
	filter := g.filter

	var snapshots []Snapshot

	req := computeService.Snapshots.List(project)
	if filter != "" {
		req.Filter(filter)
	}
	if err := req.Pages(g.ctx, func(page *compute.SnapshotList) error {
		for _, snapshot := range page.Items {

			t, _ := time.Parse(time.RFC3339, snapshot.CreationTimestamp)
			snapshots = append(snapshots, Snapshot{Name: snapshot.Name, CreateTime: t})
		}
		return nil
	}); err != nil {
		return nil, err
	}

	fmt.Printf("Total Snapshots: %v\n", len(snapshots))

	return snapshots, err
}

// DeleteSnapshots Delete a list of snapshots
func (g *GCP) DeleteSnapshots(s []string) error {
	computeService, err := compute.New(g.client)
	if err != nil {
		log.Fatal(err)
	}

	for _, v := range s {
		resp, err := computeService.Snapshots.Delete(g.project, v).Context(g.ctx).Do()
		if err != nil {
			return err
		}
		fmt.Printf("%#v\n", resp.Status)
	}

	return nil
}
