package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/mistifyio/go-zfs"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"
)

func main() {
	app := cli.NewApp()
	app.Name = "flux"
	app.Version = "1"
	app.Usage = "going back in time with zfs"
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
			created, _, err := getCreatedTime(d)
			if err != nil {
				continue
			}
			if created.Before(mark) {
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
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:  "send,s",
			Usage: "send to an ssh target",
		},
		cli.StringFlag{
			Name:  "dest,d",
			Usage: "destination",
		},
		cli.UintFlag{
			Name:  "uid",
			Usage: "ssh user",
		},
		cli.UintFlag{
			Name:  "gid",
			Usage: "ssh group",
		},
	},
	Action: func(clix *cli.Context) error {
		var (
			now    = time.Now()
			names  = clix.Args()
			target = clix.String("send")
			dest   = clix.String("dest")
		)
		for _, name := range names {
			set, err := zfs.GetDataset(name)
			if err != nil {
				return err
			}
			snapshots, err := getSnapshots(set)
			if err != nil {
				return err
			}
			prev := snapshots[len(snapshots)-1]

			snapshot, err := set.Snapshot(now.Format(time.RFC3339), false)
			if err != nil {
				return err
			}

			if target != "" {
				if dest == "" {
					return errors.New("no dest specified")
				}
				if err := send(target, dest, uint32(clix.Uint("uid")), uint32(clix.Uint("gid")), snapshot, prev); err != nil {
					return err
				}
			}
		}
		return nil
	},
}

func send(target, dest string, uid, gid uint32, set *zfs.Dataset, prev *ExtDataset) error {
	ssh := sshSend(target, dest)
	ssh.SysProcAttr = &syscall.SysProcAttr{
		Credential: &syscall.Credential{
			Uid: uid,
			Gid: gid,
		},
	}
	in, err := ssh.StdinPipe()
	if err != nil {
		return err
	}
	defer in.Close()

	ssh.Stderr = os.Stderr
	ssh.Stdout = os.Stdout
	if err := ssh.Start(); err != nil {
		return err
	}
	if err := set.IncrementalSend(prev.Dataset, in); err != nil {
		return err
	}
	return ssh.Wait()
}

func sshSend(target, dest string) *exec.Cmd {
	return exec.Command("ssh", target, "zfs", "recv", dest)
}

type ExtDataset struct {
	*zfs.Dataset
	BaseName string
	Created  time.Time
}

func getSnapshots(set *zfs.Dataset) ([]*ExtDataset, error) {
	sets, err := set.Children(0)
	if err != nil {
		return nil, err
	}
	var out []*ExtDataset
	for _, s := range sets {
		if s.Type == TypeSnapshot {
			created, name, err := getCreatedTime(s)
			if err != nil {
				continue
			}
			out = append(out, &ExtDataset{
				Dataset:  s,
				BaseName: name,
				Created:  created,
			})
		}
	}
	sort.Sort(byCreated(out))
	return out, nil
}

var errNoTime = errors.New("no time specified")

func getCreatedTime(set *zfs.Dataset) (time.Time, string, error) {
	parts := strings.SplitN(set.Name, "@", 2)
	created, err := time.Parse(time.RFC3339, parts[1])
	if err != nil {
		return time.Time{}, "", errNoTime
	}
	return created, parts[0], nil
}

type byCreated []*ExtDataset

func (s byCreated) Len() int {
	return len(s)
}

func (s byCreated) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s byCreated) Less(i, j int) bool {
	return s[i].Created.Before(s[j].Created)
}
