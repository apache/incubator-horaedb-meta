// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package main

import (
	"context"
	_ "embed"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/CeresDB/ceresmeta/pkg/coderr"
	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server"
	"github.com/CeresDB/ceresmeta/server/config"
	"github.com/pelletier/go-toml/v2"
	"go.uber.org/zap"
)

//go:generate sh -c "printf %s $(git rev-parse HEAD) > commit.version"
//go:embed commit.version
var commitID string

//go:generate sh -c "printf %s $(git rev-parse --abbrev-ref HEAD) > branch.version"
//go:embed branch.version
var branchName string

//go:generate sh -c "printf %s $(git tag -l --sort=-creatordate | head -n 1) > tag.version"
//go:embed tag.version
var latestTag string

//go:generate sh -c "printf %s $(date '+%A %W %Y %X') > time.version"
//go:embed time.version
var buildDate string

func buildVersion() string {
	return fmt.Sprintf("CeresMeta Server\nVersion:%s\nGit commit:%s\nGit branch:%s\nBuild date:%s", latestTag, commitID, branchName, buildDate)
}

func panicf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	panic(msg)
}

func main() {
	// Match version input
	for _, v := range os.Args {
		if v == "--version" || v == "-V" {
			version := buildVersion()
			println(version)
			return
		}
	}

	cfgParser, err := config.MakeConfigParser()
	if err != nil {
		panicf("fail to generate config builder, err:%v", err)
	}

	cfg, err := cfgParser.Parse(os.Args[1:])
	if coderr.Is(err, coderr.PrintHelpUsage) {
		return
	}

	if err != nil {
		panicf("fail to parse config from command line params, err:%v", err)
	}

	if err := cfg.ValidateAndAdjust(); err != nil {
		panicf("invalid config, err:%v", err)
	}

	if err := cfgParser.ParseConfigFromToml(); err != nil {
		panicf("fail to parse config from toml file, err:%v", err)
	}

	if err := cfgParser.ParseConfigFromEnv(); err != nil {
		panicf("fail to parse config from environment variable, err:%v", err)
	}

	cfgByte, err := toml.Marshal(cfg)
	if err != nil {
		panicf("fail to marshal server config, err:%v", err)
	}

	if err = os.MkdirAll(cfg.DataDir, os.ModePerm); err != nil {
		panicf("fail to create data dir, data_dir:%v, err:%v", cfg.DataDir, err)
	}

	logger, err := log.InitGlobalLogger(&cfg.Log)
	if err != nil {
		panicf("fail to init global logger, err:%v", err)
	}
	defer logger.Sync() //nolint:errcheck
	// TODO: Do adjustment to config for preparing joining existing cluster.
	log.Info("server start with config", zap.String("config", string(cfgByte)))

	srv, err := server.CreateServer(cfg)
	if err != nil {
		log.Error("fail to create server", zap.Error(err))
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	var sig os.Signal
	go func() {
		sig = <-sc
		cancel()
	}()

	if err := srv.Run(ctx); err != nil {
		log.Error("fail to run server", zap.Error(err))
		return
	}

	<-ctx.Done()
	log.Info("got signal to exit", zap.Any("signal", sig))

	srv.Close()
}
