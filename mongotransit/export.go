package mongotransit

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"time"

	"github.com/mongodb/mongo-tools/common/log"
	"github.com/mongodb/mongo-tools/common/progress"
	"github.com/mongodb/mongo-tools/mongoexport"
	"github.com/pkg/errors"
)

type exportResult struct {
	exportedCollection *ExportedCollection
	err                error
}

type ExportPlan struct {
	ConnectionString string
	Output           string
	Full             []CollectionOption
	Iterative        []IterativeRestoreState
}

type ExportedCollection struct {
	CollectionOption
	Count    int64
	FilePath string
}

type exportOptions struct {
	collection CollectionOption
	export     mongoexport.Options
}

// ExportAll perform export of all collections in plan.
func ExportAll(ctx context.Context, plan ExportPlan) ([]ExportedCollection, error) {
	var export []exportOptions

	for _, fce := range plan.Full {
		opts, err := prepareFullDumpOptions(plan.ConnectionString, fce.DB, fce.Name, plan.Output)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to create full export options for collection %s", fce.Name)
		}

		export = append(export, exportOptions{fce, opts})
	}

	for _, irs := range plan.Iterative {
		opts, err := prepareIterativeExportOptions(plan.ConnectionString, plan.Output, irs)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to create iterative export options for collection %s", irs.Name)
		}

		export = append(export, exportOptions{irs.CollectionOption, opts})
	}

	// kick off the progress bar manager
	progressManager := progress.NewBarWriter(log.Writer(0), progressBarWaitTime, progressBarLength, false)
	progressManager.Start()
	defer progressManager.Stop()

	resCh := make(chan exportResult, len(export))
	for _, opts := range export {
		go func(expOpts exportOptions) {
			log.Logvf(log.Always, "starting export for collection %s", expOpts.collection.Name)
			exported, err := runExport(ctx, progressManager, expOpts.export)
			if err != nil {
				resCh <- exportResult{err: err}
			} else {
				resCh <- exportResult{exportedCollection: &ExportedCollection{expOpts.collection, exported, expOpts.export.OutputFile}}
			}
		}(opts)
	}

	var exported []ExportedCollection
	for range export {
		select {
		case r := <-resCh:
			if r.err != nil {
				log.Logv(log.Always, r.err.Error())
			} else {
				log.Logvf(log.Always, "exported %d document(s) from collection %s", r.exportedCollection.Count, r.exportedCollection.Name)
				exported = append(exported, *r.exportedCollection)
			}
		case <-ctx.Done():
			return nil, errors.New("export cancelled")
		}

	}

	return exported, nil
}

func runExport(ctx context.Context, progressManager *progress.BarWriter, opts mongoexport.Options) (int64, error) {
	// verify uri options and log them
	opts.URI.LogUnsupportedOptions()

	exporter, err := mongoexport.New(opts)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to create exported for collection %s", opts.Collection)
	}
	exporter.ProgressManager = progressManager

	writer, err := GetOutputWriter(opts.OutputFile)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to open export writer for collection %s", opts.Collection)
	}
	defer writer.Close()

	resultCh := make(chan int64)
	errorCh := make(chan error)

	go func() {
		result, err := exporter.Export(writer)
		if err != nil {
			if opts.InputOptions.Query == "" {
				errorCh <- errors.Wrapf(err, "failed to create full export options for collection %s", opts.Collection)
				return
			}

			log.Logvf(log.Always, "iterative export of collection %s failed -> fallback to full export", opts.Collection)
			opts, err = prepareFullDumpOptions(opts.ConnectionString, Database(opts.DB), Collection(opts.Collection), opts.OutputFile)
			if err != nil {
				errorCh <- errors.Wrapf(err, "failed to create full export options for collection %s", opts.Collection)
				return
			}

			res, err := runExport(ctx, progressManager, opts)
			if err != nil {
				errorCh <- err
			} else {
				resultCh <- res
			}
		} else {
			resultCh <- result
		}
	}()

	select {
	case r := <-resultCh:
		return r, nil
	case err := <-errorCh:
		return 0, err
	case <-ctx.Done():
		return 0, fmt.Errorf("%s collection export cancelled", opts.Collection)
	}
}

func prepareFullDumpOptions(connectionString string, database Database, collection Collection, outputDir string) (mongoexport.Options, error) {
	outPath := path.Join(outputDir, fmt.Sprintf("%s.%s.json", database, collection))
	var exportOpts = []string{
		connectionString,
		fmt.Sprintf("-d=%s", database),
		fmt.Sprintf("-c=%s", collection),
		fmt.Sprintf("-o=%s", outPath),
		"--jsonFormat=canonical",
	}

	return mongoexport.ParseOptions(exportOpts, "", "")
}

func prepareIterativeExportOptions(connectionString string, outputDir string, iterOpt IterativeRestoreState) (mongoexport.Options, error) {
	outPath := path.Join(outputDir, fmt.Sprintf("%s.%s.json", iterOpt.DB, iterOpt.Name))
	var exportOpts = []string{
		connectionString,
		fmt.Sprintf("-d=%s", iterOpt.DB),
		fmt.Sprintf("-c=%s", iterOpt.Name),
		fmt.Sprintf("-q=%s", getQuery(iterOpt.Checkpoint, iterOpt.IterativeExport.Field)),
		fmt.Sprintf("-o=%s", outPath),
		"--jsonFormat=canonical",
	}

	return mongoexport.ParseOptions(exportOpts, "", "")
}

func getQuery(checkpoint time.Time, fieldName string) string {
	// loading some extra records with $gte condition for making a safe overflow
	return fmt.Sprintf(`{ "%s": { "$gte": { "$date": "%s" } } }`, fieldName, checkpoint.UTC().Format(time.RFC3339))
}

// GetOutputWriter opens and returns an io.WriteCloser for the output
// options or nil if none is set. The caller is responsible for closing it.
func GetOutputWriter(filePath string) (io.WriteCloser, error) {
	file, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}
	log.Logvf(log.Always, "created file %s", filePath)
	return file, err
}
