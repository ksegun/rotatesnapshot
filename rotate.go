package rotatesnapshot

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/spf13/viper"
)

// Retention The rotation policy configuration
type Retention struct {
	Hourly  int `yaml:"retention.hourly,omitempty"`
	Daily   int `yaml:"retention.daily,omitempty"`
	Weekly  int `yaml:"retention.weekly,omitempty"`
	Monthly int `yaml:"retention.monthly,omitempty"`
	Minimum int `yaml:"retention.minimum,omitempty"`
}

// Configuration configuration settings
type Config struct {
	Policy Retention `mapstructure:"retention,omitempty"`
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

// Constants ...
const (
	PeriodHourly  = "hourly"
	PeriodDaily   = "daily"
	PeriodWeekly  = "weekly"
	PeriodMonthly = "monthly"

	RotationDaily  = 23
	RotationWeekly = time.Sunday
)

// Variables ...
var (
	config = Config{Policy: Retention{Hourly: 12, Daily: 7, Weekly: 4, Monthly: 3}}

	DurationHourly  time.Duration
	DurationDaily   time.Duration
	DurationWeekly  time.Duration
	DurationMonthly time.Duration

	Periods        = []string{PeriodHourly, PeriodDaily, PeriodWeekly, PeriodMonthly}
	PeriodDuration map[string]time.Duration

	SnapshotsToDelete []string

	DryRun bool
)

func init() {
	flag.BoolVar(&DryRun, "dry-run", false, "If true, only print the snapshots to delete, without deleting them.")
	initConfig()
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

func endOfMonth(date time.Time) time.Time {
	return date.AddDate(0, 1, -date.Day())
}

func isRotationTime(t, now time.Time, p string) bool {
	//fmt.Println("Period: ", p)
	if include(Periods, p) {
		//e := t.Add(-PeriodDuration[p])
		switch p {
		case PeriodHourly:
			//fmt.Println("Hour: ", t.Hour())
			if t.Hour() == RotationDaily {
				return true
			}
		case PeriodDaily:
			if t.Weekday() == RotationWeekly && t.Hour() == RotationDaily {
				return true
			}
		case PeriodWeekly:
			e := endOfMonth(now)
			if (t.Day() == e.Day()) && t.Hour() == RotationDaily {
				return true
			}
		}
	}
	return false
}

func rotate(item Snapshot, period, nextPeriod string, maxAge time.Duration) {
	//fmt.Println("=======================================================================================")
	//fmt.Printf("       Name: %s\n", item.Name)
	//fmt.Printf("     Period: %s\n", period)
	//fmt.Printf("Next Period: %s\n", nextPeriod)
	//fmt.Printf("  Timestamp: %v\n", item.CreationTimestamp)
	//fmt.Printf("    Max Age: %v\n", maxAge)
	//fmt.Println("=======================================================================================")

	now := time.Now()

	b := item.CreateTime
	e := now.Add(-maxAge)

	//fmt.Printf("Earliest Time: %v\n", e)
	//fmt.Printf("  Backup Time: %v\n", b)

	if b.Before(e) {
		//fmt.Printf("Backup %v is too old\n", item.Name)
		if nextPeriod != "" && isRotationTime(b, now, period) {
			fmt.Println("Rotating Snapshot...", item.Name)
			//Backups[nextPeriod] = append(Backups[nextPeriod], item.Name)
		} else {
			// remove snapshot
			fmt.Println("Removing Snapshot...", item.Name)
			if !DryRun {
				SnapshotsToDelete = append(SnapshotsToDelete, item.Name)
			}
		}
		return
	}

}

// Rotate ...
func Rotate(p Provider) {
	var snapshots []Snapshot

	snapshots, err := p.ListSnapshots()
	if err != nil {
		log.Println(err)
		return
	}

	for _, snapshot := range snapshots {
		rotate(snapshot, PeriodHourly, PeriodDaily, DurationHourly)
		rotate(snapshot, PeriodDaily, PeriodWeekly, DurationDaily)
		rotate(snapshot, PeriodWeekly, PeriodMonthly, DurationWeekly)
		rotate(snapshot, PeriodMonthly, "", DurationMonthly)
	}

	ol := len(snapshots)
	dl := len(SnapshotsToDelete)

	// if deleting will drop us below the minimum number of backups skip
	if dl > 0 && (ol-dl) >= config.Policy.Minimum {
		fmt.Printf("Snapshots to delete: %v\n", len(SnapshotsToDelete))
		p.DeleteSnapshots(SnapshotsToDelete)
	}

}

func initConfig() {
	viper.SetConfigType("yaml")
	viper.SetConfigName("config")
	viper.AddConfigPath(".")
	viper.AddConfigPath("..")
	viper.SetEnvPrefix("rsnap")
	if err := viper.ReadInConfig(); err != nil {
		log.Printf("Error: %v\n", err)
	}
	if err := viper.Unmarshal(&config); err != nil {
		log.Printf("Error: %v\n", err)
	}
	log.Printf("Settings: %v\n", config)

	DurationHourly = time.Duration(config.Policy.Hourly) * time.Hour
	DurationDaily = time.Duration(config.Policy.Daily) * time.Hour * 24
	DurationWeekly = time.Duration(config.Policy.Weekly) * time.Hour * 24 * 7
	DurationMonthly = time.Duration(config.Policy.Monthly) * time.Hour * 24 * 7 * 4

	PeriodDuration = map[string]time.Duration{
		PeriodHourly:  DurationHourly,
		PeriodDaily:   DurationDaily,
		PeriodWeekly:  DurationWeekly,
		PeriodMonthly: DurationMonthly,
	}
}
