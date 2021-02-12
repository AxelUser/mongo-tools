package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"

	"github.com/mongodb/mongo-tools/common/log"
	"github.com/mongodb/mongo-tools/common/util"
	"github.com/mongodb/mongo-tools/mongotransit"
)

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "config.yaml", "Path to MongoTransit config file")

	var cycleRuns int
	flag.IntVar(&cycleRuns, "runs", 0, "How many iteration of data transition to run")

	var initialPhaseRaw string
	flag.StringVar(&initialPhaseRaw, "phase", string(mongotransit.PHASE_EXPORT), fmt.Sprintf("Phase from which data transitions starts: %s or %s", mongotransit.PHASE_EXPORT, mongotransit.PHASE_IMPORT))

	flag.Parse()

	opt, err := mongotransit.ReadConfigFile(configPath)
	if err != nil {
		log.Logvf(log.Always, "failed to read config file %s: %v", configPath, err)
		os.Exit(util.ExitFailure)
	}

	ctx, cancel := context.WithCancel(context.Background())

	cancelCh := make(chan os.Signal, 1)
	signal.Notify(cancelCh, os.Interrupt)

	go func() {
		select {
		case <-cancelCh:
			log.Logvf(log.Always, "cancellation was requested")
			cancel()
			return
		case <-ctx.Done():
		}
	}()

	err = mongotransit.Run(ctx, *opt, cycleRuns, mongotransit.Phase(initialPhaseRaw))
	if err != nil {
		log.Logvf(log.Always, "Failed: %v", err.Error())
		os.Exit(util.ExitFailure)
	}
	os.Exit(util.ExitSuccess)
}
