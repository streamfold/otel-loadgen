/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "otel-loadgen",
	Short: "OpenTelemetry load generator",
	// Uncomment the following line if your bare application
	// has an action associated with it:
	// Run: func(cmd *cobra.Command, args []string) { },
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

var otlpEndpoint string
var otlpResourcesPerBatch int

var duration time.Duration
var reportInterval time.Duration
var pushInterval time.Duration

var numWorkers int

func init() {
	rootCmd.PersistentFlags().StringVar(&otlpEndpoint, "otlp-endpoint", "localhost:4317", "OTLP endpoint for exporting logs, metrics, and traces")
	rootCmd.PersistentFlags().IntVar(&otlpResourcesPerBatch, "otlp-resources-per-batch", 1, "OTLP number of resources per batch")
	
	rootCmd.PersistentFlags().DurationVar(&duration, "duration", 0, "How long to run generator for, defaults to forever")
	rootCmd.PersistentFlags().DurationVar(&reportInterval, "report-interval", 3 * time.Second, "Interval to report statistics")
	rootCmd.PersistentFlags().DurationVar(&pushInterval, "push-interval", 50 * time.Millisecond, "Interval between push of batches")
	
	rootCmd.PersistentFlags().IntVar(&numWorkers, "workers", 1, "How many concurrent workers to run")	
}


func defaultTransportDialContext(dialer *net.Dialer) func(context.Context, string, string) (net.Conn, error) {
	return dialer.DialContext
}

func newClient() *http.Client {
	client := &http.Client{
		Transport: &http.Transport{
			DialContext: defaultTransportDialContext(&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}),
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          100,
			MaxIdleConnsPerHost:   100,
			MaxConnsPerHost:       100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
		Timeout: 3 * time.Second,
	}

	return client
}

func parseOtlpEndpoint() (*url.URL, error) {
	if !strings.HasPrefix(otlpEndpoint, "http://") && !strings.HasPrefix(otlpEndpoint, "https://") {
		otlpEndpoint = fmt.Sprintf("http://%s", otlpEndpoint)
	}
	
	return url.Parse(otlpEndpoint)
}