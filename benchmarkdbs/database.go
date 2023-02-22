package benchmarkdbs

import (
	"github.com/prometheus/client_golang/prometheus"
)

type ConnectionInfo struct {
	User     string
	Password string
	HostName string
	Port     string
	DBName   string
	Engine   string
	PoolSize int

	AutoMigrate        bool
	SqlMigrationFolder string
}

type Metrics struct {
	DatabaseRequestDuration *prometheus.HistogramVec
	DatabaseErrorRequests   *prometheus.CounterVec
}

type DBInterface interface {
	Get(s string) (string, error)
	Insert(s string) error
	Ping() error
}

// Connect does the db magic connection
func Connect(connectionInfo ConnectionInfo) DBInterface {
	databaseRequestDuration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "database_request_duration_seconds",
		Help:    "Histogram for the runtime of a simple primary key get function.",
		Buckets: prometheus.LinearBuckets(0.01, 0.02, 75),
	}, []string{"query"})

	databaseErrorReuests := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "database_error_requests",
			Help: "The total number of failed requests",
		},
		[]string{"method"},
	)

	prometheus.MustRegister(databaseRequestDuration)
	prometheus.MustRegister(databaseErrorReuests)

	var dbInterface DBInterface

	switch connectionInfo.Engine {
	case "mysql":
		dbInterface = connectMySQL(connectionInfo, Metrics{
			DatabaseRequestDuration: databaseRequestDuration,
			DatabaseErrorRequests:   databaseErrorReuests,
		})
	case "postgres":
		return connectPostgreSQL(connectionInfo, Metrics{
			DatabaseRequestDuration: databaseRequestDuration,
			DatabaseErrorRequests:   databaseErrorReuests,
		})
	}
	return dbInterface
}
