package main

import (
	"net/http"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
)

func main() {

	sqlQueryDuration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "longterm_query_duration_seconds",
		Help:    "Histogram for the query durations",
		Buckets: prometheus.LinearBuckets(0.00, 0.002, 75),
	}, []string{"function"})

	sqlQueryErrors := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longtime_query_errors",
			Help: "The total number of failed requests",
		},
		[]string{"function"},
	)

	prometheus.MustRegister(sqlQueryDuration)
	prometheus.MustRegister(sqlQueryErrors)

	poolsize := 10
	db := connectMySQL(ConnectionInfo{
		User:               "root",
		Password:           "asdf",
		HostName:           "127.0.0.1",
		Port:               "3306",
		DBName:             "foo",
		PoolSize:           poolsize,
		AutoMigrate:        true,
		SqlMigrationFolder: "./migrations",
	},
		Metrics{
			DatabaseRequestDuration: sqlQueryDuration,
			DatabaseErrorRequests:   sqlQueryErrors,
		},
	)

	router := mux.NewRouter()
	router.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{
		Addr: "0.0.0.0:8080",
		// Good practice to set timeouts to avoid Slowloris attacks.
		Handler: router,
	}
	// Run our server in a goroutine so that it doesn't block.
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			logrus.Println(err)
		}
	}()
	// In order to use our pool of workers we need to send
	// them work and collect their results. We make 2
	// channels for this.
	const numJobs = 500000
	jobs := make(chan uuid.UUID, numJobs)
	results := make(chan int, numJobs)
	// This starts up 3 workers, initially blocked
	// because there are no jobs yet.
	for w := 1; w <= poolsize; w++ {
		go worker(jobs, db)
	}

	// Here we send 5 `jobs` and then `close` that
	// channel to indicate that's all the work we have.
	for j := 1; j <= numJobs; j++ {
		jobs <- uuid.New()
	}
	close(jobs)

	// Finally we collect all the results of the work.
	// This also ensures that the worker goroutines have
	// finished. An alternative way to wait for multiple
	// goroutines is to use a [WaitGroup](waitgroups).
	for a := 1; a <= numJobs; a++ {
		<-results
	}
	logrus.Info("done")
}

// Here's the worker, of which we'll run several
// concurrent instances. These workers will receive
// work on the `jobs` channel and send the corresponding
// results on `results`. We'll sleep a second per job to
// simulate an expensive task.
func worker(jobs <-chan uuid.UUID, e *ExecuteMySQL) {
	for j := range jobs {
		e.AddUser(j.String())
	}
}
