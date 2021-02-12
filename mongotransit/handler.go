package mongotransit

import (
	"context"
	"os"
	"path"
	"time"

	"github.com/mongodb/mongo-tools/common/log"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	progressBarLength   = 24
	progressBarWaitTime = time.Second * 3
)

type result struct {
	processed IterativeRestoreState
	failure   error
}

// Run starts the whole transition mechanism
func Run(ctx context.Context, opts Options, iterations int, phase Phase) error {
	log.Logv(log.Always, "started transition handler")
	shardClient, err := mongo.Connect(ctx, options.Client().ApplyURI(opts.ShardedCluster))
	if err != nil {
		return errors.Wrapf(err, "failed to connect to sharded cluster")
	}
	defer shardClient.Disconnect(ctx)

	replicaClient, err := mongo.Connect(ctx, options.Client().ApplyURI(opts.ReplicaSet))
	if err != nil {
		return errors.Wrapf(err, "failed to connect to replica set")
	}
	defer replicaClient.Disconnect(ctx)

	iterationsCh := make(chan int)
	go func(initital int) {
		if initital == 0 {
			// eternal loop
			for true {
				select {
				case <-ctx.Done():
					close(iterationsCh)
					return
				default:
					iterationsCh <- 0
				}
			}
		} else {
			for i := 0; i < int(initital); i++ {
				iterationsCh <- i
			}
			close(iterationsCh)
		}
	}(iterations)

	for range iterationsCh {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		succeded, failures, err := CheckIterDumpProgress(ctx, *shardClient, *replicaClient, opts)
		if err != nil {
			return errors.Wrapf(err, "failed to check collections")
		}

		exported, err := handleExportStage(ctx, succeded, failures, opts)
		if err != nil {
			return errors.Wrapf(err, "failed to export data from replica set")
		}

		err = ImportAll(ctx, opts.ShardedCluster, exported)
		if err != nil {
			return errors.Wrapf(err, "failed to import data into sharded cluster")
		}

		// TODO sync removed documents
	}

	return nil
}

func handleExportStage(ctx context.Context, approvedIterative []IterativeRestoreState, failedIterative []CollectionOption, opts Options) ([]ExportedCollection, error) {

	// preparing directory for exporting data
	exportDir := path.Join(opts.OutputDir, "dumps")
	log.Logvf(log.Always, "preparing dump's directory %s", exportDir)
	os.RemoveAll(exportDir)
	err := os.MkdirAll(exportDir, os.ModePerm)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create directories for dumps")
	}

	// preparing options for export
	plan := ExportPlan{
		ConnectionString: opts.ReplicaSet,
		Output:           exportDir,
		Iterative:        approvedIterative,
		Full: func() (fullExport []CollectionOption) {
			for _, c := range opts.Collections {
				if c.IterativeExport == nil {
					fullExport = append(fullExport, c)
				}
			}
			for _, c := range failedIterative {
				fullExport = append(fullExport, c)
			}
			return
		}(),
	}

	// export
	result, err := ExportAll(ctx, plan)
	if err != nil {
		return nil, err
	}

	return result, nil
}
