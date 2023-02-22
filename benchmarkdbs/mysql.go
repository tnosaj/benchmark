package benchmarkdbs

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	"github.com/golang-migrate/migrate/v4"
	mysqlmigrate "github.com/golang-migrate/migrate/v4/database/mysql"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/sirupsen/logrus"
)

// ExecuteMySQL contains the connection and metrics to track executions
type ExecuteMySQL struct {
	Con     *sql.DB
	Metrics Metrics
}

func (e ExecuteMySQL) Insert(s string) error {
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
func (e ExecuteMySQL) Get(s string) (string, error) {
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
func (e ExecuteMySQL) Ping() error {
	logrus.Debugf("will execut ping")

	if err := e.Con.Ping(); err != nil {
		logrus.Debugf("Failed to ping database: %s", err)
		return err
	}
	return nil
}

func connectMySQL(connectionInfo ConnectionInfo, metrics Metrics) *ExecuteMySQL {
	logrus.Debugf("will connect to mysql")

	DSN := dsnFromConnectionInfo(connectionInfo)

	c, err := sql.Open("mysql", DSN)
	if err != nil {
		log.Fatalf("failed to open MySQL connection: %s", err)
	}

	c.SetMaxIdleConns(connectionInfo.PoolSize)
	c.SetMaxOpenConns(connectionInfo.PoolSize)
	c.SetConnMaxLifetime(0) // reuse forever

	if err = c.Ping(); err != nil {
		logrus.Fatalf("Could not ping mysql with error %s", err)
	} else {
		logrus.Info("Sucessfully connected to mysql")
	}
	if connectionInfo.AutoMigrate {
		err = autoMigrateMysql(c, connectionInfo.SqlMigrationFolder)
		if err != nil {
			logrus.Fatalf("Could not migrate with error %s", err)
		}
	}

	return &ExecuteMySQL{Con: c, Metrics: metrics}
}

func dsnFromConnectionInfo(connectionInfo ConnectionInfo) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?interpolateParams=true",
		connectionInfo.User,
		connectionInfo.Password,
		connectionInfo.HostName,
		connectionInfo.Port,
		connectionInfo.DBName,
	)
}

func autoMigrateMysql(conn *sql.DB, folder string) error {
	logrus.Debug("automatically migrating mysql")
	driver, err := mysqlmigrate.WithInstance(conn, &mysqlmigrate.Config{
		MigrationsTable: "users_schema_migrations",
	})
	if err != nil {
		logrus.Errorf("Failed to create migration connection %s", err)
		return err
	}
	m, _ := migrate.NewWithDatabaseInstance(
		fmt.Sprintf("file://%s/mysql/", folder),
		"mysql",
		driver,
	)

	if err != nil {
		logrus.Errorf("Failed to initialize migration connection %s", err)
		return err
	}

	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		logrus.Errorf("Failed to migrate db: %s", err)
		return err
	}
	logrus.Debug("succesfully migrated mysql")
	return nil
}

func (l *ExecuteMySQL) Shutdown(context context.Context) {
	logrus.Info("Shuttingdown longterm mysql server")
}
