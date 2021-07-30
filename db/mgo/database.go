package mgo

import (
	"context"
	"net/url"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"go.mongodb.org/mongo-driver/event"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	openConnections = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "mongodb_open_connections",
			Help: "Gauge of Open Connections of the Mongodb",
		},
		[]string{"database", "host"},
	)
	inUse = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "mongodb_in_use",
			Help: "Gauge of Connections that are currently in use of the Mongodb",
		},
		[]string{"database", "host"},
	)
	dbCommandDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "mongodb_command_duration_seconds",
			Help:    "Histogram of latencies for Mongodb requests.",
			Buckets: []float64{.001, .005, .01, .05, .1, .2, .4, 1, 3, 8, 20, 60, 120},
		},
		[]string{"database", "host", "command", "status"},
	)
)

func init() {
	prometheus.MustRegister(openConnections, inUse, dbCommandDuration)
}

// Database is a handle to a MongoDB database. It wraps the mongo.Database struct to add metrics to its commands.
type Database struct {
	dbname   string
	hostname string
	db       *mongo.Database
}

// Connect creates a new mongo connection based on config configurations.
func Connect(dbname, rawurl string, opts ...*options.ClientOptions) (*Database, error) {
	uri, err := url.Parse(rawurl)
	if err != nil {
		return nil, err
	}

	m := &Database{
		dbname:   dbname,
		hostname: uri.Hostname(),
	}

	opt := options.Client()
	opt.ApplyURI(uri.String())
	opt.Monitor = &event.CommandMonitor{
		Succeeded: func(i context.Context, succeededEvent *event.CommandSucceededEvent) {
			dbCommandDuration.WithLabelValues(m.dbname, m.hostname, succeededEvent.CommandName, "Succeeded").Observe(time.Duration(succeededEvent.DurationNanos).Seconds())
		},
		Failed: func(i context.Context, failedEvent *event.CommandFailedEvent) {
			dbCommandDuration.WithLabelValues(m.dbname, m.hostname, failedEvent.CommandName, "Failed").Observe(time.Duration(failedEvent.DurationNanos).Seconds())
		},
	}
	opt.PoolMonitor = &event.PoolMonitor{
		Event: func(poolEvent *event.PoolEvent) {
			switch poolEvent.Type {
			case event.ConnectionCreated:
				openConnections.WithLabelValues(m.dbname, m.hostname).Inc()
			case event.ConnectionClosed:
				openConnections.WithLabelValues(m.dbname, m.hostname).Dec()
			case event.GetSucceeded:
				inUse.WithLabelValues(m.dbname, m.hostname).Inc()
			case event.ConnectionReturned:
				inUse.WithLabelValues(m.dbname, m.hostname).Dec()
			}
		},
	}
	opts = append([]*options.ClientOptions{opt}, opts...)

	client, err := mongo.NewClient(opts...)
	if err != nil {
		return nil, err
	}

	// creates a new context to add to the client's connection. We do not need to return a CancelFunc().
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	if err := client.Connect(ctx); err != nil {
		return nil, err
	}

	m.db = client.Database(m.dbname)
	return m, nil
}

// Ping is a no-op used to test whether a server is responding to commands. This command will return immediately even if the server is write-locked
// see: https://docs.mongodb.com/manual/reference/command/ping/
func (m *Database) Ping() error {
	// creates a new context to add to the client's connection. We do not need to return a CancelFunc().
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err := m.db.Client().Ping(ctx, nil)
	if err != nil {
		return err
	}
	return nil
}

// Collection gets a handle for a collection with the given name configured with the given CollectionOptions.
func (m *Database) Collection(name string, opts ...*options.CollectionOptions) *Collection {
	collection := m.db.Collection(name, opts...)
	return &Collection{db: m, collection: collection}
}

// Client returns the Client the Database was created from.
func (m *Database) Client() *mongo.Client {
	return m.db.Client()
}
