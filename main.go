package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/BurntSushi/toml"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	snapshotsapi "github.com/containerd/containerd/api/services/snapshots/v1"
	"github.com/containerd/containerd/contrib/snapshotservice"
	"github.com/containerd/containerd/log"
	snbase "github.com/containerd/stargz-snapshotter/snapshot"
	"github.com/containerd/stargz-snapshotter/stargz/handler"
)

const (
	defaultAddress  = "/run/containerd-cvmfs-snapshotter-grpc/containerd-cvmfs-snapshotter-grpc.sock"
	defaultLogLevel = logrus.InfoLevel
	defaultRootDir  = "/var/lib/containerd-cvmfs-snapshotter-grpc"
)

var (
	address    = flag.String("address", defaultAddress, "address for the snapshotter's GRPC server")
	configPath = flag.String("config", "", "path to the configuration file")
	logLevel   = flag.String("log-level", defaultLogLevel.String(), "set the logging level [trace, debug, info, warn, error, fatal, panic]")
	rootDir    = flag.String("root", defaultRootDir, "path to the root directory for this snapshotter")
)

type filesystem struct {
	repository    string
	mountedLayers map[string]string
}

type Config struct {
	Repository string `toml:"repository" default:"unpacked.cern.ch"`
}

func NewFilesystem(config *Config) (snbase.FileSystem, error) {
	repository := config.Repository
	if repository == "" {
		repository = "unpacked.cern.ch"
	}
	return &filesystem{repository: repository, mountedLayers: make(map[string]string)}, nil
}

func (fs *filesystem) Mount(ctx context.Context, mountpoint string, labels map[string]string) error {
	digest, ok := labels[handler.TargetDigestLabel]
	log.G(ctx).WithField("layer", digest).Info("asked to mount layer")
	if !ok {
		err := fmt.Errorf("cvmfs: digest hasn't be passed")
		log.G(ctx).Debug(err.Error())
		return err
	}
	digest = strings.Split(digest, ":")[1]
	firstTwo := digest[0:2]
	path := filepath.Join("/", "cvmfs", fs.repository, ".layers", firstTwo, digest, "layerfs")
	if _, err := os.Stat(path); os.IsNotExist(err) {
		err = fmt.Errorf("layer %s not in the cvmfs repository", digest)
		log.G(ctx).WithError(err).WithField("layer digest", digest).WithField("path", path).Debug("cvmfs: Layer not found")
		return err
	}
	log.G(ctx).WithField("layer digest", digest).Debug("cvmfs: Layer present in CVMFS")
	err := syscall.Mount(path, mountpoint, "", syscall.MS_BIND, "")
	if err != nil {
		log.G(ctx).WithError(err).WithField("layer digest", digest).WithField("mountpoint", mountpoint).Debug("cvmfs: Error in bind mounting the layer.")
		return err
	}
	fs.mountedLayers[mountpoint] = path
	return nil
}

func (fs *filesystem) Check(ctx context.Context, mountpoint string) error {
	path, ok := fs.mountedLayers[mountpoint]
	if !ok {
		err := fmt.Errorf("Mountpoint: %s was not mounted", mountpoint)
		log.G(ctx).WithError(err).WithField("mountpoint", mountpoint).Error("cvmfs: the requested mountpoint does not seem to be mounted")
		return err
	}

	_, statErr := os.Stat(path)
	if statErr == nil {
		return nil
	}
	if statErr != nil {
		if os.IsNotExist(statErr) {
			err := fmt.Errorf("Layer from path: %s does not seems to be in the CVMFS repository", path)
			log.G(ctx).WithError(err).WithField("mountpoint", mountpoint).WithField("layer path", path).Error("cvmfs: the mounted layer does not seem to exist.")
			return err
		}
		err := fmt.Errorf("Error in stat-ing the layer: %s", statErr)
		log.G(ctx).WithError(err).WithField("mountpoint", mountpoint).WithField("layer path", path).Error("cvmfs: unknow error in stating the file.")
		return err
	}
	return statErr
}

func main() {
	flag.Parse()
	lvl, err := logrus.ParseLevel(*logLevel)
	if err != nil {
		log.L.WithError(err).Fatal("failed to prepare logger")
	}
	logrus.SetLevel(lvl)
	logrus.SetFormatter(&logrus.TextFormatter{
		TimestampFormat: log.RFC3339NanoFixed,
		FullTimestamp:   true,
	})

	var (
		ctx    = log.WithLogger(context.Background(), log.L)
		config = &Config{}
	)

	// Get configuration from specified file
	if *configPath != "" {
		if _, err := toml.DecodeFile(*configPath, &config); err != nil {
			log.G(ctx).WithError(err).Fatalf("failed to load config file %q", *configPath)
		}
	}

	log.G(ctx).Info("start")

	fs, _ := NewFilesystem(config)
	snap, err := snbase.NewSnapshotter(ctx, "/tmp/foo", fs)
	if err != nil {
		log.G(ctx).Info("error")
		return
	}

	rpc := grpc.NewServer()
	service := snapshotservice.FromSnapshotter(snap)

	snapshotsapi.RegisterSnapshotsServer(rpc, service)

	if err := os.MkdirAll(filepath.Dir(*address), 0700); err != nil {
		log.G(ctx).WithError(err).Fatalf("failed to create directory %q", filepath.Dir(*address))
	}

	if err := os.RemoveAll(*address); err != nil {
		log.G(ctx).WithError(err).Fatalf("failed to remove %q", *address)
	}

	l, err := net.Listen("unix", *address)
	if err != nil {
		log.G(ctx).WithError(err).Fatalf("error on listen socket %q", *address)
	}
	go func() {
		if err := rpc.Serve(l); err != nil {
			log.G(ctx).WithError(err).Fatalf("error on serving via socket %q", *address)
		}
	}()

	waitForExitSignal()
	log.G(ctx).Info("exit")
}

func waitForExitSignal() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
}
