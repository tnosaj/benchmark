package benchmarkdbs

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"

	"github.com/sirupsen/logrus"
)

// ExecutePostSQL contains the connection and metrics to track executions
type ExecutePostSQL struct {
	Con     *sql.DB
	Metrics Metrics
}

func (e ExecutePostSQL) Insert(s string) error {

	logrus.Debugf("AddUser: %s", s)
	timer := prometheus.NewTimer(e.Metrics.DatabaseRequestDuration.WithLabelValues("add"))

	res, err := e.Con.Exec(fmt.Sprintf("INSERT INTO users (user_id)values('%s')", s))
	if err != nil {
		e.Metrics.DatabaseErrorRequests.WithLabelValues("add").Inc()
		logrus.Errorf("Failed to insert with error %s", err)
		return err
	} else {
		logrus.Debugf("query returned %s", res)
	}
	timer.ObserveDuration()
	return nil

}

// ExecDDLStatement will execute a statement 's' as a DDL
func (e ExecutePostSQL) Get(s string) (string, error) {
	logrus.Debugf("GetUserByUserId: %s", s)

	stmt := fmt.Sprintf("select user_id from users where user_id='%s';", s)
	var returnedUser string

	q := e.Con.QueryRow(stmt)
	if err := q.Scan(&returnedUser); err != nil {
		//l.Metrics.SqlQueryErrors.WithLabelValues("GetBySessionid").Inc()
		return "", fmt.Errorf("query %q failed: %q", stmt, err)
	}
	return returnedUser, nil
}

// Ping checks if the db is up
func (e ExecutePostSQL) Ping() error {
	logrus.Debugf("will execut ping")

	if err := e.Con.Ping(); err != nil {
		logrus.Debugf("Failed to ping database: %s", err)
		return err
	}
	return nil
}

func connectPostgreSQL(connectionInfo ConnectionInfo, metrics Metrics) *ExecutePostSQL {
	logrus.Debugf("will connect to postgres")

	var psqlInfo string
	psqlInfo = psqlInfo + "sslmode=disable"
	//
	// moved this to the end because I was
	// having issues with passwords with
	// special chars messing with ssl settings
	//
	psqlInfo = psqlInfo + " " + psqlInfoFromConnectionInfo(connectionInfo)
	c, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Fatalf("failed to open PostgreSQL connection: %s", err)
	}

	c.SetMaxIdleConns(connectionInfo.PoolSize)
	c.SetMaxOpenConns(connectionInfo.PoolSize)
	c.SetConnMaxLifetime(0)
	if err = c.Ping(); err != nil {
		logrus.Fatalf("Could not ping postgres with error %s", err)
	} else {
		logrus.Info("Sucessfully connected to postgres")
	}
	if connectionInfo.AutoMigrate {
		err = autoMigratePostgres(c, connectionInfo.SqlMigrationFolder)
		if err != nil {
			logrus.Fatalf("Could not migrate with error %s", err)
		}
	}

	return &ExecutePostSQL{Con: c, Metrics: metrics}
}

func psqlInfoFromConnectionInfo(connectionInfo ConnectionInfo) string {
	return fmt.Sprintf("user=%s password=%s host=%s port=%s dbname=%s",
		connectionInfo.User,
		connectionInfo.Password,
		connectionInfo.HostName,
		connectionInfo.Port,
		connectionInfo.DBName,
	)
}

func autoMigratePostgres(conn *sql.DB, folder string) error {
	logrus.Debug("automatically migrating postgres")
	driver, err := postgres.WithInstance(conn, &postgres.Config{
		MigrationsTable: "users_schema_migrations",
	})
	if err != nil {
		logrus.Errorf("Failed to create migration connection %s", err)
		return err
	}
	m, err := migrate.NewWithDatabaseInstance(
		fmt.Sprintf("file://%s/postgres", folder),
		"postgres", driver,
	)
	if err != nil {
		logrus.Errorf("Failed to initialize migration connection %s", err)
		return err
	}

	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		logrus.Errorf("Failed to migrate db: %s", err)
		return err
	}
	logrus.Debug("successfully migrated postgres")

	return nil
}

func (l *ExecutePostSQL) Shutdown(context context.Context) {
	logrus.Info("Shuttingdown longterm postgres server")
}
