/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/streamfold/otel-loadgen/internal/telemetry"
	"github.com/streamfold/otel-loadgen/internal/worker"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// tracesCmd represents the traces command
var tracesCmd = &cobra.Command{
	Use:   "traces",
	Short: "Generate OTLP trace spans",
	Run: func(cmd *cobra.Command, args []string) {
		if err := runTracesCmd(); err != nil {
			log.Fatal(err)
		}
	},
}

var spansPerResource int

func init() {
	genCmd.AddCommand(tracesCmd)

	tracesCmd.Flags().IntVar(&spansPerResource, "spans-per-resource", 100, "How many trace spans per resource to generate")
}

func runTracesCmd() error {
	zl, err := zap.NewDevelopment(zap.IncreaseLevel(zapcore.InfoLevel))
	if err != nil {
		return err
	}
	
	endpoint, err := parseOtlpEndpoint()
	if err != nil {
		return err
	}
	
	workerCfg := worker.Config{
		NumWorkers:      numWorkers,
		ReportInterval:  reportInterval,
		PushInterval:    pushInterval,
		ControlEndpoint: controlEndpoint,
	}

	workers, err := worker.New(workerCfg, zl, newClient())
	if err != nil {
		return err
	}

	traceWorker := telemetry.NewTracesWorker(zl, endpoint, true, otlpResourcesPerBatch, spansPerResource)

	if err := workers.Add("OTLP Traces", traceWorker); err != nil {
		return err
	}
	
	zl.Info("Load generator has been started")
	workers.Start()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(
		signalChan,
		syscall.SIGHUP,  // kill -SIGHUP XXXX
		syscall.SIGINT,  // kill -SIGINT XXXX or Ctrl+c
		syscall.SIGQUIT, // kill -SIGQUIT XXXX
	)
	
	if duration.Milliseconds() != 0 {
		t := time.NewTimer(duration)
		select {
		case <- t.C:
			zl.Info("reached test duration", zap.Duration("duration", duration))
		case sig := <-signalChan:
			zl.Info("killed with signal", zap.String("signal", sig.String()))
		}
	} else {
		sig := <-signalChan
		zl.Info("killed with signal", zap.String("signal", sig.String()))
	}
	zl.Info("shutting down")

	workers.Stop()
	return nil
}