package mgo

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	commandDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "mongodb_duration_seconds",
			Help:    "Histogram of latencies for Mongodb collection requests.",
			Buckets: []float64{.001, .005, .01, .05, .1, .2, .4, 1, 3, 8, 20, 60, 120},
		},
		[]string{"database", "host", "collection", "command"},
	)
)

func init() {
	prometheus.MustRegister(commandDuration)
}

// Collection is a handle to a MongoDB collection. It wraps the mongo.Collection struct to add metrics to its commands.
type Collection struct {
	db         *Database
	collection *mongo.Collection
}

// Clone creates a copy of the Collection configured with the given CollectionOptions.
func (coll *Collection) Clone(opts ...*options.CollectionOptions) (*Collection, error) {
	collection, err := coll.collection.Clone(opts...)
	if err != nil {
		return nil, err
	}
	return &Collection{db: coll.db, collection: collection}, nil
}

// Name returns the name of the collection.
func (coll *Collection) Name() string {
	return coll.collection.Name()
}

// Database returns the Database that was used to create the Collection.
func (coll *Collection) Database() *Database {
	return coll.db
}

// BulkWrite wraps and exposes mongo.BulkWrite with metrics
func (coll *Collection) BulkWrite(ctx context.Context, models []mongo.WriteModel,
	opts ...*options.BulkWriteOptions) (*mongo.BulkWriteResult, error) {
	start := time.Now()
	defer commandDuration.WithLabelValues(coll.db.dbname, coll.db.hostname, coll.Name(), "BulkWrite").Observe(time.Since(start).Seconds())
	return coll.collection.BulkWrite(ctx, models, opts...)
}

// InsertOne wraps and exposes mongo.InsertOne with metrics
func (coll *Collection) InsertOne(ctx context.Context, document interface{},
	opts ...*options.InsertOneOptions) (*mongo.InsertOneResult, error) {
	start := time.Now()
	defer commandDuration.WithLabelValues(coll.db.dbname, coll.db.hostname, coll.Name(), "InsertOne").Observe(time.Since(start).Seconds())
	return coll.collection.InsertOne(ctx, document, opts...)
}

// InsertMany wraps and exposes mongo.InsertMany with metrics
func (coll *Collection) InsertMany(ctx context.Context, documents []interface{},
	opts ...*options.InsertManyOptions) (*mongo.InsertManyResult, error) {
	start := time.Now()
	defer commandDuration.WithLabelValues(coll.db.dbname, coll.db.hostname, coll.Name(), "InsertMany").Observe(time.Since(start).Seconds())
	return coll.collection.InsertMany(ctx, documents, opts...)
}

// DeleteOne wraps and exposes mongo.DeleteOne with metrics
func (coll *Collection) DeleteOne(ctx context.Context, filter interface{},
	opts ...*options.DeleteOptions) (*mongo.DeleteResult, error) {
	start := time.Now()
	defer commandDuration.WithLabelValues(coll.db.dbname, coll.db.hostname, coll.Name(), "DeleteOne").Observe(time.Since(start).Seconds())
	return coll.collection.DeleteOne(ctx, filter, opts...)
}

// DeleteMany wraps and exposes mongo.DeleteMany with metrics
func (coll *Collection) DeleteMany(ctx context.Context, filter interface{},
	opts ...*options.DeleteOptions) (*mongo.DeleteResult, error) {
	start := time.Now()
	defer commandDuration.WithLabelValues(coll.db.dbname, coll.db.hostname, coll.Name(), "DeleteMany").Observe(time.Since(start).Seconds())
	return coll.collection.DeleteMany(ctx, filter, opts...)
}

// UpdateByID wraps and exposes mongo.UpdateByID with metrics
func (coll *Collection) UpdateByID(ctx context.Context, id interface{}, update interface{},
	opts ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
	start := time.Now()
	defer commandDuration.WithLabelValues(coll.db.dbname, coll.db.hostname, coll.Name(), "UpdateByID").Observe(time.Since(start).Seconds())
	return coll.UpdateByID(ctx, id, update, opts...)
}

// UpdateOne wraps and exposes mongo.UpdateOne with metrics
func (coll *Collection) UpdateOne(ctx context.Context, filter interface{}, update interface{},
	opts ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
	start := time.Now()
	defer commandDuration.WithLabelValues(coll.db.dbname, coll.db.hostname, coll.Name(), "UpdateOne").Observe(time.Since(start).Seconds())
	return coll.collection.UpdateOne(ctx, filter, update, opts...)
}

// UpdateMany wraps and exposes mongo.UpdateMany with metrics
func (coll *Collection) UpdateMany(ctx context.Context, filter interface{}, update interface{},
	opts ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
	start := time.Now()
	defer commandDuration.WithLabelValues(coll.db.dbname, coll.db.hostname, coll.Name(), "UpdateMany").Observe(time.Since(start).Seconds())
	return coll.collection.UpdateMany(ctx, filter, update, opts...)
}

// ReplaceOne wraps and exposes mongo.ReplaceOne with metrics
func (coll *Collection) ReplaceOne(ctx context.Context, filter interface{},
	replacement interface{}, opts ...*options.ReplaceOptions) (*mongo.UpdateResult, error) {
	start := time.Now()
	defer commandDuration.WithLabelValues(coll.db.dbname, coll.db.hostname, coll.Name(), "ReplaceOne").Observe(time.Since(start).Seconds())
	return coll.collection.ReplaceOne(ctx, filter, replacement, opts...)
}

// Aggregate wraps and exposes mongo.Aggregate with metrics
func (coll *Collection) Aggregate(ctx context.Context, pipeline interface{},
	opts ...*options.AggregateOptions) (*mongo.Cursor, error) {
	start := time.Now()
	defer commandDuration.WithLabelValues(coll.db.dbname, coll.db.hostname, coll.Name(), "Aggregate").Observe(time.Since(start).Seconds())
	return coll.collection.Aggregate(ctx, pipeline, opts...)
}

// CountDocuments wraps and exposes mongo.CountDocuments with metrics
func (coll *Collection) CountDocuments(ctx context.Context, filter interface{},
	opts ...*options.CountOptions) (int64, error) {
	start := time.Now()
	defer commandDuration.WithLabelValues(coll.db.dbname, coll.db.hostname, coll.Name(), "CountDocuments").Observe(time.Since(start).Seconds())
	return coll.collection.CountDocuments(ctx, filter, opts...)
}

// EstimatedDocumentCount wraps and exposes mongo.EstimatedDocumentCount with metrics
func (coll *Collection) EstimatedDocumentCount(ctx context.Context,
	opts ...*options.EstimatedDocumentCountOptions) (int64, error) {
	start := time.Now()
	defer commandDuration.WithLabelValues(coll.db.dbname, coll.db.hostname, coll.Name(), "EstimatedDocumentCount").Observe(time.Since(start).Seconds())
	return coll.collection.EstimatedDocumentCount(ctx, opts...)
}

// Distinct wraps and exposes mongo.Distinct with metrics
func (coll *Collection) Distinct(ctx context.Context, fieldName string, filter interface{},
	opts ...*options.DistinctOptions) ([]interface{}, error) {
	start := time.Now()
	defer commandDuration.WithLabelValues(coll.db.dbname, coll.db.hostname, coll.Name(), "Distinct").Observe(time.Since(start).Seconds())
	return coll.collection.Distinct(ctx, fieldName, filter, opts...)
}

// Find wraps and exposes mongo.Find with metrics
func (coll *Collection) Find(ctx context.Context, filter interface{},
	opts ...*options.FindOptions) (*mongo.Cursor, error) {
	start := time.Now()
	defer commandDuration.WithLabelValues(coll.db.dbname, coll.db.hostname, coll.Name(), "Find").Observe(time.Since(start).Seconds())
	return coll.collection.Find(ctx, filter, opts...)
}

// FindOne wraps and exposes mongo.FindOne with metrics
func (coll *Collection) FindOne(ctx context.Context, filter interface{},
	opts ...*options.FindOneOptions) *mongo.SingleResult {
	start := time.Now()
	defer commandDuration.WithLabelValues(coll.db.dbname, coll.db.hostname, coll.Name(), "FindOne").Observe(time.Since(start).Seconds())
	return coll.collection.FindOne(ctx, filter, opts...)
}

// FindOneAndDelete wraps and exposes mongo.FindOneAndDelete with metrics
func (coll *Collection) FindOneAndDelete(ctx context.Context, filter interface{},
	opts ...*options.FindOneAndDeleteOptions) *mongo.SingleResult {
	start := time.Now()
	defer commandDuration.WithLabelValues(coll.db.dbname, coll.db.hostname, coll.Name(), "FindOneAndDelete").Observe(time.Since(start).Seconds())
	return coll.collection.FindOneAndDelete(ctx, filter, opts...)
}

// FindOneAndReplace wraps and exposes mongo.FindOneAndReplace with metrics
func (coll *Collection) FindOneAndReplace(ctx context.Context, filter interface{},
	replacement interface{}, opts ...*options.FindOneAndReplaceOptions) *mongo.SingleResult {
	start := time.Now()
	defer commandDuration.WithLabelValues(coll.db.dbname, coll.db.hostname, coll.Name(), "FindOneAndReplace").Observe(time.Since(start).Seconds())
	return coll.collection.FindOneAndReplace(ctx, filter, replacement, opts...)
}

// FindOneAndUpdate wraps and exposes mongo.FindOneAndUpdate with metrics
func (coll *Collection) FindOneAndUpdate(ctx context.Context, filter interface{},
	update interface{}, opts ...*options.FindOneAndUpdateOptions) *mongo.SingleResult {
	start := time.Now()
	defer commandDuration.WithLabelValues(coll.db.dbname, coll.db.hostname, coll.Name(), "FindOneAndUpdate").Observe(time.Since(start).Seconds())
	return coll.collection.FindOneAndUpdate(ctx, filter, update, opts...)
}

// Watch exposes mongo.Watch
func (coll *Collection) Watch(ctx context.Context, pipeline interface{},
	opts ...*options.ChangeStreamOptions) (*mongo.ChangeStream, error) {
	return coll.collection.Watch(ctx, pipeline, opts...)
}

// Indexes exposes mongo.Indexes
func (coll *Collection) Indexes() mongo.IndexView {
	return coll.collection.Indexes()
}

// Drop exposes mongo.Drop
func (coll *Collection) Drop(ctx context.Context) error {
	return coll.collection.Drop(ctx)
}
