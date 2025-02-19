package services

import (
	"context"
	"database/sql"
	"fmt"
	"jaya-transport-service/config"

	_ "github.com/jackc/pgx/v5"
)

type TimescaleClient struct {
	DB *sql.DB
}

func NewTimescaleClient(ctx context.Context, conf config.TimescaleDBConfig) (*TimescaleClient, error) {
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s",
		conf.User, conf.Password, conf.Host, conf.Port, conf.DBName, conf.SSLMode)
	db, err := sql.Open("pgx", connStr)
	if err != nil {
		return nil, fmt.Errorf("error opening database: %w", err)
	}

	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("error pinging database: %w", err)
	}

	return &TimescaleClient{DB: db}, nil
}
