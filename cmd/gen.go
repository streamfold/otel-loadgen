/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

// genCmd represents the gen command
var genCmd = &cobra.Command{
	Use:   "gen",
	Run: func(cmd *cobra.Command, args []string) {
		log.Fatal("Choose a subcommand: traces")
	},
}

var otlpEndpoint string
var otlpResourcesPerBatch int

var duration time.Duration
var reportInterval time.Duration
var pushInterval time.Duration

var controlEndpoint string

var numWorkers int

func init() {
	rootCmd.AddCommand(genCmd)
	
	genCmd.PersistentFlags().StringVar(&otlpEndpoint, "otlp-endpoint", "localhost:4317", "OTLP endpoint for exporting logs, metrics, and traces")
	genCmd.PersistentFlags().IntVar(&otlpResourcesPerBatch, "otlp-resources-per-batch", 1, "OTLP number of resources per batch")
	
	genCmd.PersistentFlags().DurationVar(&duration, "duration", 0, "How long to run generator for, defaults to forever")
	genCmd.PersistentFlags().DurationVar(&reportInterval, "report-interval", 3 * time.Second, "Interval to report statistics")
	genCmd.PersistentFlags().DurationVar(&pushInterval, "push-interval", 50 * time.Millisecond, "Interval between push of batches")
	
	genCmd.PersistentFlags().IntVar(&numWorkers, "workers", 1, "How many concurrent workers to run")
	
	genCmd.PersistentFlags().StringVar(&controlEndpoint, "control-endpoint", "", "Endpoint of control server")
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