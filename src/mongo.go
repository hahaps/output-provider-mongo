package src

import (
	"context"
	"github.com/go-basic/uuid"
	"github.com/hahaps/common-provider/src/output"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"strings"
	"time"
)

const DefaultTimeout time.Duration = 30
const DefaultConnectURI string = "mongodb://localhost:27017"
const DefaultMaxPoolSize uint64 = 100
const DefaultDatabase string = "CloudTracker"

type MongoStore struct {
	db *mongo.Database
	output.Provider
}

func (ms *MongoStore) Init(setting map[string]interface{}) (err error) {
	timeout := DefaultTimeout
	if tm, ok := setting["timout"]; ok {
		if timeout, ok = tm.(time.Duration); !ok {
			return err
		}
	}
	connectURI := DefaultConnectURI
	if cn, ok := setting["connect_uri"]; ok {
		if connectURI, ok = cn.(string); !ok {
			return err
		}
	}
	maxPoolSize := DefaultMaxPoolSize
	if mp, ok := setting["max_pool_size"]; ok {
		if maxPoolSize, ok = mp.(uint64); !ok {
			return err
		}
	}
	database := DefaultDatabase
	if db, ok := setting["database"]; ok {
		if database, ok = db.(string); !ok {
			return err
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	o := options.Client().ApplyURI(connectURI)
	o.SetMaxPoolSize(maxPoolSize)
	client, err := mongo.Connect(ctx, o)
	if err != nil {
		return err
	}
	ms.db = client.Database(database)
	return nil
}

func (MongoStore)Push(params output.Params, replay *output.Replay) (err error) {
	mg := &MongoStore{}
	err = mg.Init(params.Setting)
	ctx := context.TODO()
	collection := mg.db.Collection(params.Resource)
	replay.Status = 200
	for _, resource := range params.Input {
		var results []map[string]interface{}
		resource["Timestamp"] = params.Timestamp
		query := parseMapToBson(params.Query)
		query["Index"] = resource["Index"].(string)
		cur, err := collection.Find(ctx, query)
		if err != nil {
			replay.Status = 500
			return err
		}
		err = cur.All(ctx, &results)
		if err != nil {
			return err
		}
		if len(results) > 0 {
			if !checkUpdated(results, resource["Checksum"].(string)) {
				_, err := collection.UpdateMany(ctx, query, bson.M{
					"$set": resource,
				})
				if err != nil {
					replay.Status = 500
					return err
				}
			} else {
				_, err := collection.UpdateMany(ctx, query, bson.M{
					"$set": bson.M{
						"Timestamp": params.Timestamp,
						"Deleted": 0,
					},
				})
				if err != nil {
					replay.Status = 500
					return err
				}
			}
		} else {
			_, err := collection.InsertOne(ctx, resource)
			if err != nil {
				replay.Status = 500
				return err
			}
		}
	}
	return err
}

func (MongoStore)UpdateDeleted(params output.Params, replay *int32) (err error) {
	mg := &MongoStore{}
	err = mg.Init(params.Setting)
	ctx := context.TODO()
	collection := mg.db.Collection(params.Resource)
	*replay = 200
	// Update resources state to deleted if not match to sync timestamp.
	query := parseMapToBson(params.Query)
	query["Timestamp"] = bson.M{
		"$ne": params.Timestamp,
	}
	_, err = collection.UpdateMany(ctx, query, bson.M{
		"$set": bson.M{
			"Deleted": 1,
		},
	})
	if err != nil {
		*replay = 500
		return err
	}
	return err
}

func (MongoStore)UpdateSyncJob(params output.JobParams, jobId *string) (err error) {
	var results []map[string]interface{}
	mg := &MongoStore{}
	err = mg.Init(params.Setting)
	ctx := context.TODO()
	collection := mg.db.Collection(params.Resource)
	query := bson.M{}
	query["Index"] = params.SyncJob.Index
	cur, err := collection.Find(ctx, query)
	if err != nil {
		return err
	}
	err = cur.All(ctx, &results)
	if err != nil {
		return err
	}
	if len(results) > 0 {
		*jobId = params.SyncJob.Index
		_, err = collection.UpdateOne(ctx, bson.M{
			"Index": params.SyncJob.Index,
		}, bson.M{
			"$set": bson.M{
				"Status": params.SyncJob.Status,
				"EndAt": params.SyncJob.EndAt,
			},
		})
		if err != nil {
			return err
		}
	} else {
		*jobId = uuid.New()
		_, err = collection.InsertOne(ctx, map[string]interface{}{
			"Index": *jobId,
			"Status": params.SyncJob.Status,
			"Type": params.SyncJob.Type,
			"Resource": params.SyncJob.Resource,
			"Value": params.SyncJob.Value,
			"StartAt": params.SyncJob.StartAt,
			"EndAt": params.SyncJob.EndAt,
		})
		if err != nil {
			return err
		}
	}
	return err
}

func parseMapToBson(args map[string]interface{}) bson.M {
	result := bson.M{}
	for key, val := range args {
		result[key] = val
	}
	return result
}

func checkUpdated(results []map[string]interface{}, checksum string) bool {
	for _, item := range results {
		if preChecksum, ok := item["Checksum"]; ok {
			ck := preChecksum.(string)
			return strings.Compare(checksum, ck) == 0
		}
	}
	return false
}
