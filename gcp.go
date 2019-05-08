package rotatesnapshot

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/compute/v1"
)

// GCP Google Cloud Provider
type GCP struct {
	Ctx     context.Context
	Client  *http.Client
	Project string
	Filter  string
}

// NewGCPProvider Create a Google Cloud Provider
func NewGCPProvider(p, f string) (*GCP, error) {
	ctx := context.Background()

	c, err := google.DefaultClient(ctx, compute.CloudPlatformScope)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create default client")
	}

	return &GCP{Ctx: ctx, Client: c, Project: p, Filter: f}, nil
}

// ListSnapshots List Google Cloud Provider snapshots
func (g *GCP) ListSnapshots() ([]Snapshot, error) {
	computeService, err := compute.New(g.Client)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create compute")
	}

	// Project ID for this request.
	project := g.Project
	filter := g.Filter

	var snapshots []Snapshot

	req := computeService.Snapshots.List(project)
	if filter != "" {
		req.Filter(filter)
	}
	if err := req.Pages(g.Ctx, func(page *compute.SnapshotList) error {
		for _, snapshot := range page.Items {

			t, _ := time.Parse(time.RFC3339, snapshot.CreationTimestamp)
			snapshots = append(snapshots, Snapshot{Name: snapshot.Name, CreateTime: t})
		}
		return nil
	}); err != nil {
		return nil, errors.Wrap(err, "failed to list snapshots")
	}

	return snapshots, nil
}

// DeleteSnapshots Delete Google Cloud Provider snapshots
func (g *GCP) DeleteSnapshots(s []string) error {
	computeService, err := compute.New(g.Client)
	if err != nil {
		return errors.Wrap(err, "failed to create compute")
	}

	for _, v := range s {
		resp, err := computeService.Snapshots.Delete(g.Project, v).Context(g.Ctx).Do()
		if err != nil {
			return errors.Wrapf(err, "failed to delete snapshot %s", v)
		}
		fmt.Printf("%#v\n", resp.Status)
	}

	return nil
}
