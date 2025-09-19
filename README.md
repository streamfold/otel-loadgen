# OpenTelemetry Load Generator

A command-line tool for generating OpenTelemetry (OTLP) telemetry to test and stress-test OpenTelemetry collectors, backends, and observability systems.

## Features

- Generate OTLP trace spans with configurable parameters
- Support for concurrent workers to simulate high-throughput scenarios
- Configurable batch sizes and push intervals
- Built-in statistics reporting
- Signal handling for graceful shutdown
- HTTP/2 support with optimized connection pooling

## Command Line Arguments

### Global Flags

| Flag                         | Default          | Description                                           |
| ---------------------------- | ---------------- | ----------------------------------------------------- |
| `--otlp-endpoint`            | `localhost:4317` | OTLP endpoint for exporting logs, metrics, and traces |
| `--otlp-resources-per-batch` | `1`              | Number of resources per batch                         |
| `--duration`                 | `0` (forever)    | How long to run the generator (e.g., `5m`, `1h30m`)   |
| `--report-interval`          | `3s`             | Interval to report statistics                         |
| `--push-interval`            | `50ms`           | Interval between batch pushes                         |
| `--workers`                  | `1`              | Number of concurrent workers to run                   |

### Traces Command

Generate OTLP trace spans:

```bash
otel-loadgen traces [flags]
```

#### Traces-specific Flags

| Flag                   | Default | Description                                    |
| ---------------------- | ------- | ---------------------------------------------- |
| `--spans-per-resource` | `100`   | Number of trace spans per resource to generate |

## Build and Run

### Prerequisites

- Go 1.24.1 or later
- Make (optional, for using Makefile)

### Building from Source

```bash
# Build the binary
make build

# The binary will be created at dist/otel-loadgen
```

### Running from Docker container

```bash
docker run -ti streamfold/otel-loadgen traces [...]
```

### Running

```bash
# Generate traces indefinitely with default settings
./dist/otel-loadgen traces

# Generate traces for 5 minutes with custom endpoint
./dist/otel-loadgen traces --otlp-endpoint http://collector:4317 --duration 5m

# High-throughput test with multiple workers
./dist/otel-loadgen traces \
  --workers 10 \
  --spans-per-resource 1000 \
  --push-interval 10ms \
  --otlp-resources-per-batch 5
```

## License

See [LICENSE](LICENSE) file for details.
