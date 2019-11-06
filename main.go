package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/mistifyio/go-zfs"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"
)

func main() {
	app := cli.NewApp()
	app.Name = "zfsback"
	app.Version = "1"
	app.Usage = "ZFS's has your back"
	app.Flags = []cli.Flag{
		cli.BoolFlag{
			Name:  "debug",
			Usage: "enable debug output in the logs",
		},
	}
	app.Commands = []cli.Command{
		snapshotCommand,
		purgeCommand,
	}
	app.Before = func(clix *cli.Context) error {
		if clix.GlobalBool("debug") {
			logrus.SetLevel(logrus.DebugLevel)
		}
		return nil
	}
	if err := app.Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

const (
	Day          = 24 * time.Hour
	Week         = 7 * Day
	TypeSnapshot = "snapshot"
)

var purgeCommand = cli.Command{
	Name:  "purge",
	Usage: "purge old snapshots",
	Flags: []cli.Flag{
		cli.DurationFlag{
			Name:  "older-than,o",
			Usage: "purge snapshots older than",
			Value: 2 * Week,
		},
	},
	Action: func(clix *cli.Context) error {
		mark := time.Now().Add(-clix.Duration("older-than"))
		data, err := zfs.GetDataset("tank")
		if err != nil {
			return err
		}
		sets, err := data.Children(0)
		if err != nil {
			return err
		}
		for _, d := range sets {
			if d.Type != TypeSnapshot {
				continue
			}
			parts := strings.SplitN(d.Name, "@", 2)
			created, err := time.Parse(time.RFC3339, parts[1])
			if err != nil {
				// skip parse errors as that means we don't manage this snapshot
				continue
			}
			if created.Before(mark) {
				// purge snapshot
				logrus.WithField("name", parts[0]).Debug("destroy snapshot")
				if err := d.Destroy(zfs.DestroyDefault); err != nil {
					return err
				}
			}
		}
		return nil
	},
}

var snapshotCommand = cli.Command{
	Name:  "snapshot",
	Usage: "snapshot",
	Action: func(clix *cli.Context) error {
		now := time.Now()
		names := clix.Args()
		for _, name := range names {
			set, err := zfs.GetDataset(name)
			if err != nil {
				return err
			}
			snapshot, err := set.Snapshot(now.Format(time.RFC3339), false)
			if err != nil {
				return err
			}
			fmt.Printf("snapshot %s\n", snapshot.Name)
		}
		return nil
	},
}
