package main

import (
	"net/http"
	"os"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"github.com/tnosaj/benchmark/benchmarkdbs"
)

func main() {

	const poolsize = 5
	const numJobs = 500000

	jobs := make(chan uuid.UUID, numJobs)
	results := make(chan int, numJobs)

	db := benchmarkdbs.Connect(
		benchmarkdbs.ConnectionInfo{
			User:               os.Getenv("DBUSER"),
			Password:           os.Getenv("DBPASSWORD"),
			HostName:           os.Getenv("DBHOSTNAME"),
			Port:               os.Getenv("DBPORT"),
			DBName:             os.Getenv("DBNAME"),
			Engine:             os.Getenv("DBENGINE"),
			PoolSize:           poolsize,
			AutoMigrate:        true,
			SqlMigrationFolder: "./migrations",
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

	for w := 1; w <= poolsize; w++ {
		go worker(jobs, db)
	}

	for j := 1; j <= numJobs; j++ {
		jobs <- uuid.New()
	}
	close(jobs)

	for a := 1; a <= numJobs; a++ {
		<-results
	}
	logrus.Info("done")
}

func worker(jobs <-chan uuid.UUID, dbif benchmarkdbs.DBInterface) {
	for j := range jobs {
		dbif.Insert(j.String())
	}
}
