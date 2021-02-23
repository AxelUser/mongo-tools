package mongotransit

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/mongodb/mongo-tools/common/log"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type collectionCheckResult struct {
	collection CollectionOption
	state      IterativeRestoreState
	err        error
}

// CheckIterDumpProgress gets last checkpoints and lags for collections, which support iterative dump.
func CheckIterDumpProgress(ctx context.Context, scClient mongo.Client, rsClient mongo.Client, opt Options) (succeded []IterativeRestoreState, failed []CollectionOption, err error) {
	log.Logv(log.Always, "stated check for iterative dump")
	databases, err := scClient.ListDatabaseNames(ctx, bson.M{})
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't list databases at sharded cluster")
	}

	var collectionForIterativeExport []CollectionOption

	for _, collection := range opt.Collections {
		if collection.IterativeExport != nil {
			if !contains(databases, string(collection.DB)) {
				return nil, nil, fmt.Errorf("cluster doesn't contain database '%s'", collection.DB)
			}
			collectionForIterativeExport = append(collectionForIterativeExport, collection)
		}
	}

	if len(collectionForIterativeExport) == 0 {
		log.Logv(log.Always, "no collections for iterative dump")
		return succeded, failed, nil
	}

	log.Logvf(log.Always, "checking %d collection(s): %s", len(collectionForIterativeExport), func() string {
		var names []string
		for _, c := range collectionForIterativeExport {
			names = append(names, string(c.Name))
		}
		return strings.Join(names, ", ")
	}())

	results := make(chan collectionCheckResult, len(collectionForIterativeExport))
	for _, collection := range collectionForIterativeExport {
		go checkCollection(ctx, scClient, rsClient, collection, results)
	}

	for range collectionForIterativeExport {
		select {
		case r := <-results:
			if r.err != nil {
				if r.collection.IterativeExport.Force {
					return nil, nil, errors.Wrapf(r.err, "failed to check collection %s, which is forced to be iterative", r.collection.Name)
				}
				log.Logvf(log.Always, "failed to check collection %s: %v", r.collection.Name, r.err)
				failed = append(failed, r.collection)
			} else {
				succeded = append(succeded, r.state)
			}
		case <-ctx.Done():
			return nil, nil, errors.New("concistency check was cancelled")
		}
	}

	return succeded, failed, err
}

func checkCollection(ctx context.Context, scClient mongo.Client, rsClient mongo.Client, collection CollectionOption, resultsCh chan<- collectionCheckResult) {
	log.Logvf(log.Always, "checking collection %s for possibility of iterative export", collection.Name)
	collections, err := scClient.Database(string(collection.DB)).ListCollectionNames(ctx, bson.M{})
	if err != nil {
		resultsCh <- collectionCheckResult{collection: collection, err: errors.Wrapf(err, "couldn't list collections at database %s", collection.DB)}
		return
	}

	if !contains(collections, string(collection.Name)) {
		resultsCh <- collectionCheckResult{collection: collection, err: errors.Wrapf(err, "collection %s doesn't exist in database %s", collection.Name, collection.DB)}
		return
	}

	checkpoint, err := getCheckpoint(ctx, scClient, collection)
	if err != nil {
		resultsCh <- collectionCheckResult{collection: collection, err: errors.Wrapf(err, "failed to get checkpoint for collection %s", collection.Name)}
		return
	}

	lag, err := countLag(ctx, rsClient, collection, checkpoint)
	if err != nil {
		resultsCh <- collectionCheckResult{collection: collection, err: errors.Wrapf(err, "failed to count lag between RS and SC for collection %s", collection.Name)}
		return
	}

	log.Logvf(log.Always, "collection %s has checkpoint %s and lag %d", collection.Name, checkpoint.UTC().Format(time.RFC3339), lag)
	resultsCh <- collectionCheckResult{collection: collection, state: IterativeRestoreState{
		CollectionOption: collection,
		Checkpoint:       checkpoint,
		Lag:              lag,
	}}
}

func getCheckpoint(ctx context.Context, client mongo.Client, collection CollectionOption) (time.Time, error) {

	cursor, err := client.Database(string(collection.DB)).Collection(string(collection.Name)).Find(ctx, bson.M{
		collection.IterativeExport.Field: bson.M{
			"$exists": true,
		},
	}, options.Find().SetSort(bson.M{collection.IterativeExport.Field: -1}).SetLimit(1))

	if err != nil {
		return time.Time{}, errors.Wrapf(err, "failed to get maximum %s from collection '%s'", collection.IterativeExport.Field, collection.Name)
	}

	var result []bson.M

	cursor.All(ctx, &result)

	if len(result) == 0 {
		return time.Time{}, fmt.Errorf("no records with %s field in collection '%v'", collection.IterativeExport.Field, collection.Name)
	}
	var checkpoint = result[0][collection.IterativeExport.Field].(primitive.DateTime).Time()
	return checkpoint, nil
}

func countLag(ctx context.Context, client mongo.Client, collection CollectionOption, checkpoint time.Time) (int64, error) {
	return client.Database(string(collection.DB)).Collection(string(collection.Name)).CountDocuments(ctx, bson.M{
		collection.IterativeExport.Field: bson.M{
			"$gt": checkpoint,
		},
	})
}

func contains(s []string, str string) bool {
	for _, v := range s {
		if v == str {
			return true
		}
	}

	return false
}
