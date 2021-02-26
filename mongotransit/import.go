package mongotransit

import (
	"context"
	"fmt"
	"runtime"
	"strings"

	"github.com/mongodb/mongo-tools/common/log"
	"github.com/mongodb/mongo-tools/mongoimport"
)

// ImportAll function restores dumps from specified directory
func ImportAll(ctx context.Context, connectionString string, collections []ExportedCollection, workersPerCPU int, batchSize int) error {

	if len(collections) == 0 {
		log.Logv(log.Always, "no collections for import")
		return nil
	}

	log.Logvf(log.Always, "starting collection imports")

	log.Logvf(log.Always, "importing %d collections: %s", len(collections), func() string {
		var names []string
		for _, c := range collections {
			names = append(names, string(c.Name))
		}

		return strings.Join(names, ", ")
	}())

	var options []mongoimport.Options
	for _, collection := range collections {
		restoreOptions, err := prepareOptions(connectionString, collection, workersPerCPU, batchSize)
		if err != nil {
			return err
		}

		options = append(options, restoreOptions)
	}

	doneCh := make(chan bool, len(options))
	for _, opts := range options {
		go func(importOptions mongoimport.Options) {
			importTool, err := mongoimport.New(importOptions)
			if err != nil {
				log.Logvf(log.Always, "error occurred while importing collection %s: %v", importOptions.Collection, err)
			}

			imported, failures, err := importTool.ImportDocuments()

			if err != nil {
				log.Logvf(log.Always, "error occurred while importing collection %s: %v", importOptions.Collection, err)
			}

			if importTool.ToolOptions.WriteConcern.Acknowledged() {
				log.Logvf(log.Always, "%s: %v document(s) imported successfully. %v document(s) failed to import", importOptions.Collection, imported, failures)
			} else {
				log.Logvf(log.Always, "%s: import completed", importOptions.Collection)
			}

			doneCh <- true
		}(opts)
	}

	for range options {
		select {
		case <-doneCh:
		case <-ctx.Done():
			return nil
		}
	}

	return nil
}

func prepareOptions(connectionString string, exportedCollection ExportedCollection, workersPerCPU int, batchSize int) (mongoimport.Options, error) {
	restoreOptions := []string{
		connectionString,
		fmt.Sprintf("--file=%s", exportedCollection.FilePath),
		fmt.Sprintf("-d=%s", exportedCollection.DB),
		fmt.Sprintf("-c=%s", exportedCollection.Name),
		fmt.Sprintf("--numDecodingWorkers=%d", runtime.NumCPU()*workersPerCPU),
		fmt.Sprintf("--numInsertionWorkers=%d", runtime.NumCPU()*workersPerCPU),
		fmt.Sprintf("--batchSize=%d", batchSize),
		"--bypassDocumentValidation",
		"--mode=upsert",
	}

	if exportedCollection.UpsertFields != nil && len(exportedCollection.UpsertFields) > 0 {
		var upsertFields = strings.Join(exportedCollection.UpsertFields, ",")
		restoreOptions = append(restoreOptions, fmt.Sprintf("--upsertFields=%s", upsertFields))
	}

	return mongoimport.ParseOptions(restoreOptions, "", "")
}
