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
	"github.com/streamfold/otel-loadgen/internal/genai"
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
var enableGenAI bool
var genAICorpusPath string
var useHTTP bool

func init() {
	genCmd.AddCommand(tracesCmd)

	tracesCmd.Flags().IntVar(&spansPerResource, "spans-per-resource", 100, "How many trace spans per resource to generate")
	tracesCmd.Flags().BoolVar(&enableGenAI, "gen-ai", false, "Enable gen_ai span attributes using corpus data")
	tracesCmd.Flags().StringVar(&genAICorpusPath, "gen-ai-corpus", "contrib/apigen-mt_5k.json.gz", "Path to the gen_ai corpus file (supports .gz)")
	tracesCmd.Flags().BoolVar(&useHTTP, "http", false, "Use HTTP/JSON instead of gRPC for OTLP export")
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

	headers, err := parseCustomHeaders()
	if err != nil {
		return err
	}

	// Load gen_ai corpus if enabled
	var corpus *genai.Corpus
	if enableGenAI {
		zl.Info("Loading gen_ai corpus", zap.String("path", genAICorpusPath))
		corpus, err = genai.LoadCorpus(genAICorpusPath)
		if err != nil {
			return err
		}
		zl.Info("Loaded gen_ai corpus", zap.Int("entries", corpus.Size()))
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

	traceWorker := telemetry.NewTracesWorker(zl, endpoint, !useHTTP, otlpResourcesPerBatch, spansPerResource, corpus, headers)

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