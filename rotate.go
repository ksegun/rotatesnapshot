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

// Compare a with b and extract the list of items of a not in b
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

// Find the index of string t in list
func index(vs []string, t string) int {
	for i, v := range vs {
		if v == t {
			return i
		}
	}
	return -1
}

// Return true if list contains string t
func include(vs []string, t string) bool {
	return index(vs, t) >= 0
}

// Find the start of the month for a given date
func startOfMonth(date time.Time) time.Time {
	return date.AddDate(0, 0, -date.Day()+1)
}

// Find the end of the month for a given date
func endOfMonth(date time.Time) time.Time {
	return date.AddDate(0, 1, -date.Day())
}

// Return true if the date is the first on the month for the given weekday
// e.g Fist Sunday of the month ....
func firstWeekdayOfMonth(t time.Time, w string) bool {
	s := startOfMonth(t)
	for i := 0; i < 7; i++ {
		if s.Weekday().String() == w && s.Day() == t.Day() {
			return true
		}
		s = s.AddDate(0, 0, 1)
	}
	return false
}

// Return true if the snaopshot should be rotated
func isRotationTime(t, now time.Time, p, n string) bool {
	if include(periods, p) {
		switch p {
		case periodHourly:
			if t.Hour() == config.Rotation.Daily {
				return true
			}
		case periodDaily:
			if t.Weekday().String() == config.Rotation.Weekly {
				return true
			}
		case periodWeekly:
			if firstWeekdayOfMonth(t, config.Rotation.Weekly) {

				return true
			}
		default:
			return false
		}
	}
	return false
}

// Rotate all snapshots for the period
func rotateSnapshots(items []Snapshot, deletes map[string]struct{}, period, nextPeriod string, maxAge time.Duration) {
	now := time.Now()
	e := now.Add(-maxAge)

	var b time.Time
	for _, item := range items {
		b = item.CreateTime
		if b.Before(e) {
			if !(nextPeriod != "" && isRotationTime(b, now, period, item.Name)) {
				deletes[item.Name] = struct{}{}
			}
		}
	}
}

// SetVerbose set the verbose log falg
func SetVerbose(v bool) {
	verbose = v
}

// Rotate rotate snapshots based on condifured retention policy
func Rotate(p Provider) error {
	s, err := p.ListSnapshots()
	if err != nil {
		return err
	}

	var snapshots []string
	for _, v := range s {
		snapshots = append(snapshots, v.Name)
	}

	snapshotsToDelete := make(map[string]struct{})

	rotateSnapshots(s, snapshotsToDelete, periodHourly, periodDaily, durationHourly)
	rotateSnapshots(s, snapshotsToDelete, periodDaily, periodWeekly, durationDaily)
	rotateSnapshots(s, snapshotsToDelete, periodWeekly, periodMonthly, durationWeekly)
	rotateSnapshots(s, snapshotsToDelete, periodMonthly, "", durationMonthly)

	fmt.Printf("          Snapshots: %v\n", len(snapshots))
	fmt.Printf("Snapshots to delete: %v\n", len(snapshotsToDelete))

	dlen := len(snapshotsToDelete)
	if dlen > 0 {
		deletes := make([]string, 0, dlen)
		for k := range snapshotsToDelete {
			deletes = append(deletes, k)
		}
		sort.Strings(deletes)

		snapshotsRetained := difference(snapshots, deletes)
		rlen := len(snapshotsRetained)

		fmt.Printf("  Snapshots to keep: %v\n", rlen)

		if verbose {
			fmt.Println("")
			fmt.Println("Snapshots To Keep")
			fmt.Println("=================")
			dump(snapshotsRetained)

			fmt.Println("")
			fmt.Println("Snapshots To Delete")
			fmt.Println("===================")
			dump(deletes)
		}

		// if deleting will drop us below the minimum number of backups skip
		if rlen > config.Policy.Minimum {
			if !dryRun {
				err = p.DeleteSnapshots(deletes)
			}
		} else {
			fmt.Printf("Remaining snapshot count %d is below configured minimum %d, skipping delete.\n", rlen, config.Policy.Minimum)
		}
	}

	return err
}

func initConfig() {
	viper.SetConfigType("yaml")
	viper.SetConfigName("config")
	viper.AddConfigPath(".")
	viper.AddConfigPath("..")
	viper.SetEnvPrefix("snap")

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
