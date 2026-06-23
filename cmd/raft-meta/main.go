package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/hashicorp/go-hclog"
	"raft-meta/internal/config"
	"raft-meta/internal/raftnode"
	"raft-meta/internal/server"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	sub := os.Args[1]
	fs := flag.NewFlagSet(sub, flag.ExitOnError)
	configPath := fs.String("config", "", "path to config yaml")
	fs.Parse(os.Args[2:])
	if *configPath == "" {
		fmt.Fprintln(os.Stderr, "error: -config required")
		os.Exit(2)
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}

	switch sub {
	case "init":
		if err := server.Init(cfg); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
	case "start":
		if err := server.Run(cfg); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
	case "reset":
		if err := raftnode.Reset(cfg.DataDir); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
		fmt.Println("data directory cleared:", cfg.DataDir)
	case "recover":
		if err := raftnode.RecoverClusterSingle(cfg, hclog.New(&hclog.LoggerOptions{Name: "recover", Level: hclog.Info})); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
		fmt.Println("cluster recovered to single-node:", cfg.NodeID)
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: raft-meta {init|start|reset|recover} -config <path>")
}
