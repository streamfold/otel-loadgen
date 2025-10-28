/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/streamfold/otel-loadgen/internal/control"
	"github.com/streamfold/otel-loadgen/internal/msg_tracker"
	"github.com/streamfold/otel-loadgen/internal/sink"
	"go.uber.org/zap"
)

// sinkCmd represents the sink command
var sinkCmd = &cobra.Command{
	Use:   "sink",
	Short: "Run a sink server for incoming telemetry",
	Run: func(cmd *cobra.Command, args []string) {
		if err := runSink(); err != nil {
			log.Fatal(err)
		}
	},
}

var sinkAddr string
var controlAddr string

func init() {
	rootCmd.AddCommand(sinkCmd)

	sinkCmd.Flags().StringVar(&sinkAddr, "addr", "localhost:5317", "address to listen on")
	sinkCmd.Flags().StringVar(&controlAddr, "control-addr", "localhost:5000", "control server address")
}

func runSink() error {
	zl, err := zap.NewDevelopment()
	if err != nil {
		return err
	}

	mt := msg_tracker.NewTracker()

	// Start the sink server
	s, err := sink.New(sinkAddr, zl)
	if err != nil {
		return err
	}

	if err := s.Start(); err != nil {
		return err
	}

	zl.Info("Sink server has been started", zap.String("addr", s.Addr()))

	// Start the control server
	c := control.New(controlAddr, mt, zl)
	if err := c.Start(); err != nil {
		s.Stop()
		return err
	}

	zl.Info("Control server has been started", zap.String("addr", c.Addr()))

	signalChan := make(chan os.Signal, 1)
	signal.Notify(
		signalChan,
		syscall.SIGHUP,  // kill -SIGHUP XXXX
		syscall.SIGINT,  // kill -SIGINT XXXX or Ctrl+c
		syscall.SIGQUIT, // kill -SIGQUIT XXXX
	)

	select {
	case sig := <-signalChan:
		zl.Info("killed with signal", zap.String("signal", sig.String()))
	}
	zl.Info("shutting down")

	// Stop both servers
	c.Stop()
	s.Stop()

	return nil
}
