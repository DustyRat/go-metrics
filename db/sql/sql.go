package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/xo/dburl"
)

var (
	maxOpenDesc = prometheus.NewDesc(
		"sql_stats_connections_max_open",
		"Maximum number of open connections to the database.",
		nil,
		nil,
	)
	openDesc = prometheus.NewDesc(
		"sql_stats_connections_open",
		"The number of established connections both in use and idle.",
		nil,
		nil,
	)
	inUseDesc = prometheus.NewDesc(
		"sql_stats_connections_in_use",
		"The number of connections currently in use.",
		nil,
		nil,
	)
	idleDesc = prometheus.NewDesc(
		"sql_stats_connections_idle",
		"The number of idle connections.",
		nil,
		nil,
	)
	waitedForDesc = prometheus.NewDesc(
		"sql_stats_connections_waited_for",
		"The total number of connections waited for.",
		nil,
		nil,
	)
	blockedSecondsDesc = prometheus.NewDesc(
		"sql_stats_connections_blocked_seconds",
		"The total time blocked waiting for a new connection.",
		nil,
		nil,
	)
	closedMaxIdleDesc = prometheus.NewDesc(
		"sql_stats_connections_closed_max_idle",
		"The total number of connections closed due to SetMaxIdleConns.",
		nil,
		nil,
	)
	closedMaxLifetimeDesc = prometheus.NewDesc(
		"sql_stats_connections_closed_max_lifetime",
		"The total number of connections closed due to SetConnMaxLifetime.",
		nil,
		nil,
	)
	commandDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "sql_command_duration_seconds",
			Help:    "Histogram of latencies for SQL requests.",
			Buckets: []float64{.001, .005, .01, .05, .1, .2, .4, 1, 3, 8, 20, 60, 120},
		},
		[]string{"driver", "database", "host", "command", "query"},
	)
)

func init() {
	prometheus.MustRegister(commandDuration)
}

// SQL represents the sql connection struct.
type SQL struct {
	db *sql.DB

	// url      *url.URL
	driver   string
	dbname   string
	hostname string

	register prometheus.Registerer
}

// New return new SQL struct with given sql connection, driver database name and host
func New(db *sql.DB, driver, dbname, hostname string) (*SQL, error) {
	if db == nil {
		return nil, errors.New("no sql databse connection provided")
	}

	s := &SQL{
		db:       db,
		driver:   driver,
		dbname:   dbname,
		hostname: hostname,
	}
	s.register = prometheus.WrapRegistererWith(prometheus.Labels{
		"driver":   s.driver,
		"database": s.dbname,
		"hostname": s.hostname,
	}, prometheus.DefaultRegisterer)
	s.register.MustRegister(s)
	return s, nil
}

// Connect creates a new sql connection from url connection string
// Example urls:
//	postgres://user:pass@localhost/dbname
// 	pg://user:pass@localhost/dbname?sslmode=disable
// 	mysql://user:pass@localhost/dbname
// 	mysql:/var/run/mysqld/mysqld.sock
// 	sqlserver://user:pass@remote-host.com/dbname
// 	mssql://user:pass@remote-host.com/dbname
// 	mssql://user:pass@remote-host.com/instance/dbname
// 	ms://user:pass@remote-host.com:1433/instance/dbname?keepAlive=10
// 	oracle://user:pass@somehost.com/sid
// 	sap://user:pass@localhost/dbname
// 	sqlite:/path/to/file.db
// 	file:myfile.sqlite3?loc=auto
// 	odbc+postgres://user:pass@localhost:5432/dbname?option1=
func Connect(rawurl string) (*SQL, error) {
	uri, err := dburl.Parse(rawurl)
	if err != nil {
		return nil, err
	}
	parts := strings.Split(uri.Normalize("|", "", 0), "|")
	dbname := parts[3]

	db, err := sql.Open(uri.Driver, uri.String())
	if err != nil {
		return nil, err
	}

	s := &SQL{
		db:       db,
		driver:   uri.Driver,
		dbname:   dbname,
		hostname: uri.Hostname(),
	}
	s.register = prometheus.WrapRegistererWith(prometheus.Labels{
		"driver":   s.driver,
		"database": s.dbname,
		"host":     s.hostname,
	}, prometheus.DefaultRegisterer)
	s.register.MustRegister(s)
	return s, nil
}

// SetMaxOpenConns wraps and exposes sql.SetMaxOpenConns
func (s *SQL) SetMaxOpenConns(n int) {
	s.db.SetMaxOpenConns(n)
}

// SetMaxIdleConns wraps and exposes sql.SetMaxIdleConns
func (s *SQL) SetMaxIdleConns(n int) {
	s.db.SetMaxIdleConns(n)
}

// SetConnMaxLifetime wraps and exposes sql.SetConnMaxLifetime
func (s *SQL) SetConnMaxLifetime(d time.Duration) {
	s.db.SetConnMaxLifetime(d)
}

// SetConnMaxIdleTime wraps and exposes sql.SetConnMaxIdleTime
func (s *SQL) SetConnMaxIdleTime(d time.Duration) {
	s.db.SetConnMaxIdleTime(d)
}

// Close wraps and exposes sql.Close
func (s *SQL) Close() error {
	if s.register != nil {
		s.register.Unregister(s)
	}
	return s.db.Close()
}

// Stats wraps and exposes sql.Stats
func (s *SQL) Stats() sql.DBStats {
	return s.db.Stats()
}

// PrepareContext wraps and exposes sql.PrepareContext
func (s *SQL) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return s.db.PrepareContext(ctx, query)
}

// Prepare wraps and exposes sql.Prepare
func (s *SQL) Prepare(query string) (*sql.Stmt, error) {
	return s.db.Prepare(query)
}

// BeginTx wraps and exposes sql.BeginTx
func (s *SQL) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return s.db.BeginTx(ctx, opts)
}

// Begin wraps and exposes sql.Begin
func (s *SQL) Begin() (*sql.Tx, error) {
	return s.db.Begin()
}

// Driver wraps and exposes sql.Driver
func (s *SQL) Driver() driver.Driver {
	return s.db.Driver()
}

// Conn wraps and exposes sql.Conn
func (s *SQL) Conn(ctx context.Context) (*sql.Conn, error) {
	return s.db.Conn(ctx)
}

// Ping wraps and exposes sql.Ping with metrics
func (s *SQL) Ping() error {
	return s.PingContext(context.Background())
}

// PingContext wraps and exposes sql.PingContext with metrics
func (s *SQL) PingContext(ctx context.Context) error {
	start := time.Now()
	defer commandDuration.WithLabelValues(s.driver, s.dbname, s.hostname, "ping", "").Observe(time.Since(start).Seconds())
	return s.db.PingContext(ctx)
}

// Exec wraps and exposes sql.Exec with metrics
func (s *SQL) Exec(query string, args ...interface{}) (sql.Result, error) {
	return s.ExecContext(context.Background(), query, args...)
}

// ExecContext wraps and exposes sql.ExecContext with metrics
func (s *SQL) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	start := time.Now()
	defer commandDuration.WithLabelValues(s.driver, s.dbname, s.hostname, "exec", query).Observe(time.Since(start).Seconds())
	return s.db.ExecContext(ctx, query, args...)
}

// Query wraps and exposes sql.Query with metrics
func (s *SQL) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return s.QueryContext(context.Background(), query, args...)
}

// QueryContext wraps and exposes sql.QueryContext with metrics
func (s *SQL) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	start := time.Now()
	defer commandDuration.WithLabelValues(s.driver, s.dbname, s.hostname, "query", query).Observe(time.Since(start).Seconds())
	return s.db.QueryContext(ctx, query, args...)
}

// QueryRow wraps and exposes sql.QueryRow with metrics
func (s *SQL) QueryRow(query string, args ...interface{}) *sql.Row {
	return s.QueryRowContext(context.Background(), query, args...)
}

// QueryRowContext wraps and exposes sql.QueryRowContext with metrics
func (s *SQL) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	start := time.Now()
	defer commandDuration.WithLabelValues(s.driver, s.dbname, s.hostname, "query_row", query).Observe(time.Since(start).Seconds())
	return s.db.QueryRowContext(ctx, query, args...)
}

// Describe implements the prometheus.Collector interface.
func (s *SQL) Describe(ch chan<- *prometheus.Desc) {
	ch <- maxOpenDesc
	ch <- openDesc
	ch <- inUseDesc
	ch <- idleDesc
	ch <- waitedForDesc
	ch <- blockedSecondsDesc
	ch <- closedMaxIdleDesc
	ch <- closedMaxLifetimeDesc
}

// Collect implements the prometheus.Collector interface.
func (s *SQL) Collect(ch chan<- prometheus.Metric) {
	stats := s.Stats()

	ch <- prometheus.MustNewConstMetric(
		maxOpenDesc,
		prometheus.GaugeValue,
		float64(stats.MaxOpenConnections),
	)
	ch <- prometheus.MustNewConstMetric(
		openDesc,
		prometheus.GaugeValue,
		float64(stats.OpenConnections),
	)
	ch <- prometheus.MustNewConstMetric(
		inUseDesc,
		prometheus.GaugeValue,
		float64(stats.InUse),
	)
	ch <- prometheus.MustNewConstMetric(
		idleDesc,
		prometheus.GaugeValue,
		float64(stats.Idle),
	)
	ch <- prometheus.MustNewConstMetric(
		waitedForDesc,
		prometheus.CounterValue,
		float64(stats.WaitCount),
	)
	ch <- prometheus.MustNewConstMetric(
		blockedSecondsDesc,
		prometheus.CounterValue,
		stats.WaitDuration.Seconds(),
	)
	ch <- prometheus.MustNewConstMetric(
		closedMaxIdleDesc,
		prometheus.CounterValue,
		float64(stats.MaxIdleClosed),
	)
	ch <- prometheus.MustNewConstMetric(
		closedMaxLifetimeDesc,
		prometheus.CounterValue,
		float64(stats.MaxLifetimeClosed),
	)
}
