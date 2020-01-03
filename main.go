package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strconv"
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
		cli.BoolFlag{
			Name:  "dry",
			Usage: "display don't delete",
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
			created, err := getCreationTime(d)
			if err != nil {
				logrus.WithError(err).Error("get creation time")
				continue
			}
			if created.Before(mark) {
				logrus.Debugf("destory %s", d.Name)
				if !clix.Bool("dry") {
					if err := d.Destroy(zfs.DestroyDefault); err != nil {
						logrus.WithError(err).Error("unable destroy")
					}
				}
			}
		}
		return nil
	},
}

const creationProp = "creation"

func getCreationTime(d *zfs.Dataset) (time.Time, error) {
	p, err := d.GetProperty(creationProp)
	if err != nil {
		return time.Time{}, err
	}
	v, err := strconv.Atoi(p)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(int64(v), 0), nil
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
		cli.BoolFlag{
			Name:  "init",
			Usage: "send the inital snapshot",
		},
	},
	Action: func(clix *cli.Context) error {
		var (
			now    = time.Now()
			names  = clix.Args()
			target = clix.String("send")
			dest   = clix.String("dest")
			initS  = clix.Bool("init")
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
			if initS {
				prev = nil
			}

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
	if prev == nil {
		if err := set.SendSnapshot(in); err != nil {
			return err
		}
		return ssh.Wait()
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
			created, err := getCreationTime(s)
			if err != nil {
				continue
			}
			out = append(out, &ExtDataset{
				Dataset:  s,
				BaseName: strings.Split(s.Name, "@")[0],
				Created:  created,
			})
		}
	}
	sort.Sort(byCreated(out))
	return out, nil
}

var errNoTime = errors.New("no time specified")

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
