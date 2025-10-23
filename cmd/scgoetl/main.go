package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/ArisCorporation/sc-goetl/internal/app"
	"github.com/ArisCorporation/sc-goetl/internal/config"
	"github.com/ArisCorporation/sc-goetl/internal/utils"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	cfg, err := config.Parse(os.Args[1:])
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			fmt.Fprintf(os.Stderr, "failed to parse configuration: %v\n", err)
		}
		os.Exit(2)
	}

	if err := app.Run(ctx, cfg); err != nil {
		utils.Error("ETL failed", err)
		os.Exit(1)
	}
}
