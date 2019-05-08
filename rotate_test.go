package rotatesnapshot

import (
	"fmt"
	"testing"
	"time"
)

const (
	HourlyTest  = "hourly"
	DailyTest   = "daily"
	WeeklyTest  = "weekly"
	MonthlyTest = "monthly"
)

type FakeProvider struct {
	duration string
}

func (f *FakeProvider) ListSnapshots() ([]Snapshot, error) {
	var s []Snapshot
	var count int

	n := "snapshot"
	now := time.Now()

	switch f.duration {
	case HourlyTest:
		count = 24
	case DailyTest:
		count = 24 * 7
	case WeeklyTest:
		count = 24 * 7 * 4
	case MonthlyTest:
		count = 24 * 7 * 4 * 3
	}

	var t time.Time
	for count > 0 {
		t = now.Add(time.Duration(-count) * time.Hour)
		s = append(s, Snapshot{Name: fmt.Sprintf("%s-%v", n, t.Format("20060102150405")), CreateTime: t})
		count--
	}

	return s, nil
}

func (f *FakeProvider) DeleteSnapshots(s []string) error {
	return nil
}

func TestRotate(t *testing.T) {
	type args struct {
		p Provider
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "test hourly rotation",
			args: args{p: &FakeProvider{duration: HourlyTest}},
		},
		{
			name: "test daily rotation",
			args: args{p: &FakeProvider{duration: DailyTest}},
		},
		{
			name: "test weekly rotation",
			args: args{p: &FakeProvider{duration: WeeklyTest}},
		},
		{
			name: "test montly rotation",
			args: args{p: &FakeProvider{duration: MonthlyTest}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Rotate(tt.args.p)
		})
	}
}
