package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"time"

	"github.com/dahai80/fusion-ops/internal/store"
)

func main() {
	dsn := os.Getenv("FUSION_OPS_DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://127.0.0.1:5432/fusion_ops?sslmode=disable"
	}
	dir := flag.String("dir", "migrations", "migrations directory")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool, err := store.Connect(ctx, dsn)
	if err != nil {
		slog.Error("migrate: database unavailable", "error", err)
		os.Exit(1)
	}
	defer pool.Close()
	if err := store.Migrate(ctx, pool, *dir); err != nil {
		slog.Error("migrate: failed", "error", err)
		os.Exit(1)
	}
	slog.Info("migrate: all migrations applied")
}
