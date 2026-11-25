package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/dahai80/fusion-ops/internal/broker"
	"github.com/dahai80/fusion-ops/internal/store"
)

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))

	dsn := envOr("FUSION_OPS_DATABASE_URL", "postgres://127.0.0.1:5432/fusion_ops?sslmode=disable")
	addr := envOr("FUSION_BROKER_LISTEN", "127.0.0.1:11401")

	// Provisioning mode: bind a device credential and exit. Tokens pass via
	// env (FUSION_BROKER_PROVISION_TOKEN) so they never sit in argv/ps.
	if deviceID := os.Getenv("FUSION_BROKER_PROVISION_DEVICE"); deviceID != "" {
		token := os.Getenv("FUSION_BROKER_PROVISION_TOKEN")
		if token == "" {
			slog.Error("provision: FUSION_BROKER_PROVISION_TOKEN required")
			os.Exit(1)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		pool, err := store.Connect(ctx, dsn)
		if err != nil {
			slog.Error("provision: database unavailable", "error", err)
			os.Exit(1)
		}
		defer pool.Close()
		caps := []string{}
		if c := os.Getenv("FUSION_BROKER_PROVISION_CAPS"); c != "" {
			caps = strings.Split(c, ",")
		}
		if err := broker.NewStore(pool).Provision(ctx, deviceID, token, os.Getenv("FUSION_BROKER_PROVISION_VERSION"), caps); err != nil {
			slog.Error("provision failed", "error", err)
			os.Exit(1)
		}
		slog.Info("provision complete", "device_id", deviceID)
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := store.Connect(ctx, dsn)
	if err != nil {
		slog.Error("startup: database unavailable", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	conn, err := pool.Acquire(ctx)
	if err != nil {
		slog.Error("startup: no connection for privilege probe", "error", err)
		os.Exit(1)
	}
	if err := store.ProbePrivileges(ctx, conn); err != nil {
		slog.Error("startup: privilege probe failed", "error", err)
		os.Exit(1)
	}
	conn.Release()

	hub := broker.NewHub(broker.NewStore(pool))
	if err := hub.Store().ResetAllOffline(ctx); err != nil {
		slog.Error("startup: registry offline reset failed", "error", err)
		os.Exit(1)
	}

	srv := broker.NewServer(hub, broker.DefaultMailboxTTL)
	if err := srv.Run(ctx, addr); err != nil {
		slog.Error("broker exited with error", "error", err)
		os.Exit(1)
	}
	slog.Info("broker stopped")
}
