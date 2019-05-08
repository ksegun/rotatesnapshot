package rotatesnapshot

import (
	"flag"
	"fmt"
	"sort"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

type retention struct {
	Hourly  int `yaml:"retention.hourly,omitempty"`
	Daily   int `yaml:"retention.daily,omitempty"`
	Weekly  int `yaml:"retention.weekly,omitempty"`
	Monthly int `yaml:"retention.monthly,omitempty"`
	Minimum int `yaml:"retention.minimum,omitempty"`
}

type rotation struct {
	Daily  int    `yaml:"rotation.daily,omitempty"`
	Weekly string `yaml:"rotation.weekly,omitempty"`
}

// Config configuration settings
type Config struct {
	Policy   retention `mapstructure:"retention,omitempty"`
	Rotation rotation  `mapstructure:"rotation,omitempty"`
}

// Provider interface
type Provider interface {
	ListSnapshots() ([]Snapshot, error)
	DeleteSnapshots(s []string) error
}

// Snapshot struct
type Snapshot struct {
	Name       string
	CreateTime time.Time
}

const (
	periodHourly  = "hourly"
	periodDaily   = "daily"
	periodWeekly  = "weekly"
	periodMonthly = "monthly"
)

var (
	config = Config{
		Policy:   retention{Hourly: 12, Daily: 7, Weekly: 4, Monthly: 3, Minimum: 10},
		Rotation: rotation{Daily: 23, Weekly: "Sunday"}}

	durationHourly  time.Duration
	durationDaily   time.Duration
	durationWeekly  time.Duration
	durationMonthly time.Duration

	periods        = []string{periodHourly, periodDaily, periodWeekly, periodMonthly}
	periodDuration map[string]time.Duration

	snapshotsToDelete = make(map[string]struct{})

	rotateHourly = make(map[string]struct{})
	rotateDailyy = make(map[string]struct{})
	rotateWeekly = make(map[string]struct{})

	dryRun  bool
	verbose bool
)

func init() {
	flag.BoolVar(&dryRun, "dry-run", false, "If true, only print the snapshots to delete, without deleting them.")
	flag.BoolVar(&verbose, "verbose", false, "If true, log detailed output.")
	initConfig()
}

func dump(a []string) {
	for _, i := range a {
		fmt.Println(i)
	}
}

func dedup(s []string) {
	sort.Strings(s)
	j := 0
	for i := 1; i < len(s); i++ {
		if s[j] == s[i] {
			continue
		}
		j++
		s[j] = s[i]
	}
	s = s[:j+1]
}

func difference(a, b []string) []string {
	mb := map[string]bool{}
	for _, x := range b {
		mb[x] = true
	}
	var ab []string
	for _, x := range a {
		if _, ok := mb[x]; !ok {
			ab = append(ab, x)
		}
	}
	return ab
}

func index(vs []string, t string) int {
	for i, v := range vs {
		if v == t {
			return i
		}
	}
	return -1
}

func include(vs []string, t string) bool {
	return index(vs, t) >= 0
}

func startOfMonth(date time.Time) time.Time {
	return date.AddDate(0, 0, -date.Day()+1)
}

func endOfMonth(date time.Time) time.Time {
	return date.AddDate(0, 1, -date.Day())
}

func firstOfMonth(t time.Time, w string) bool {
	s := startOfMonth(t)
	for i := 0; i < 7; i++ {
		if s.Weekday().String() == w && s.Day() == t.Day() {
			return true
		}
		s = s.AddDate(0, 0, 1)
	}
	return false
}

func isRotationTime(t, now time.Time, p, n string) bool {
	if include(periods, p) {
		switch p {
		case periodHourly:
			if t.Hour() == config.Rotation.Daily {
				rotateHourly[n] = struct{}{}
				return true
			}
		case periodDaily:
			if t.Weekday().String() == config.Rotation.Weekly {
				rotateDailyy[n] = struct{}{}
				return true
			}
		case periodWeekly:
			if firstOfMonth(t, config.Rotation.Weekly) {
				rotateWeekly[n] = struct{}{}
				return true
			}
		default:
			return false
		}
	}
	return false
}

func rotateSnapshots(items []Snapshot, period, nextPeriod string, maxAge time.Duration) {
	now := time.Now()
	e := now.Add(-maxAge)

	var b time.Time
	for _, item := range items {
		b = item.CreateTime
		if b.Before(e) {
			if !(nextPeriod != "" && isRotationTime(b, now, period, item.Name)) {
				snapshotsToDelete[item.Name] = struct{}{}
			}
		}
	}
}

// Rotate rotate snapshots based on condifure retention policy
func Rotate(p Provider) error {
	s, err := p.ListSnapshots()
	if err != nil {
		return err
	}

	var snapshots []string
	for _, v := range s {
		snapshots = append(snapshots, v.Name)
	}

	rotateSnapshots(s, periodHourly, periodDaily, durationHourly)
	rotateSnapshots(s, periodDaily, periodWeekly, durationDaily)
	rotateSnapshots(s, periodWeekly, periodMonthly, durationWeekly)
	rotateSnapshots(s, periodMonthly, "", durationMonthly)

	if verbose {
		fmt.Printf("Settings: %+v\n", config)

		fmt.Printf("Rotate Hourly: %v\n", len(rotateHourly))
		fmt.Printf("Rotate Daily: %v\n", len(rotateDailyy))
		fmt.Printf("Rotate Weekly: %v\n", len(rotateWeekly))
	}

	ol := len(snapshots)
	dl := len(snapshotsToDelete)

	// if deleting will drop us below the minimum number of backups skip
	if dl > 0 && (ol-dl) >= config.Policy.Minimum {
		keys := make([]string, 0, len(snapshotsToDelete))
		for k := range snapshotsToDelete {
			keys = append(keys, k)
		}
		dedup(keys)
		sort.Strings(keys)

		snapshotsRetained := difference(snapshots, keys)

		fmt.Println("")
		fmt.Printf("          Snapshots: %v\n", len(snapshots))
		fmt.Printf("  Snapshots to keep: %v\n", len(snapshotsRetained))
		fmt.Printf("Snapshots to delete: %v\n", len(keys))

		if verbose {
			fmt.Println("")
			fmt.Println("Snapshots To Keep")
			fmt.Println("=================")
			dump(snapshotsRetained)

			fmt.Println("")
			fmt.Println("Snapshots To Delete")
			fmt.Println("===================")
			dump(keys)
		}

		if !dryRun {
			err = p.DeleteSnapshots(keys)
		}
	}

	return err
}

func initConfig() {
	viper.SetConfigType("yaml")
	viper.SetConfigName("config")
	viper.AddConfigPath(".")
	viper.AddConfigPath("..")
	viper.SetEnvPrefix("rsnap")

	if err := viper.ReadInConfig(); err != nil {
		fmt.Printf("%+v\n", errors.WithStack(err))
	}

	if err := viper.Unmarshal(&config); err != nil {
		fmt.Printf("%+v\n", errors.WithStack(err))
	}

	durationHourly = time.Duration(config.Policy.Hourly) * time.Hour
	durationDaily = time.Duration(config.Policy.Daily) * time.Hour * 24
	durationWeekly = time.Duration(config.Policy.Weekly) * time.Hour * 24 * 7
	durationMonthly = time.Duration(config.Policy.Monthly) * time.Hour * 24 * 7 * 4

	periodDuration = map[string]time.Duration{
		periodHourly:  durationHourly,
		periodDaily:   durationDaily,
		periodWeekly:  durationWeekly,
		periodMonthly: durationMonthly,
	}
}
