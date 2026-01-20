# OpenTelemetry Load Generator

A command-line tool for generating OpenTelemetry (OTLP) telemetry to test and stress-test OpenTelemetry collectors, backends, and observability systems.

## Features

- Support for concurrent workers to simulate high-throughput scenarios
- Configurable batch sizes and push intervals
- Built-in statistics reporting
- Sink server for receiving and tracking telemetry
- Control server for coordinating distributed load testing

## Commands

### Generator Command (`gen traces`)

Generate OTLP trace spans to send to an OTLP endpoint:

```bash
otel-loadgen gen traces [flags]
```

#### Generator Flags

| Flag                         | Default          | Description                                           |
| ---------------------------- | ---------------- | ----------------------------------------------------- |
| `--otlp-endpoint`            | `localhost:4317` | OTLP endpoint for exporting logs, metrics, and traces |
| `--otlp-resources-per-batch` | `1`              | Number of resources per batch                         |
| `--spans-per-resource`       | `100`            | Number of trace spans per resource to generate        |
| `--duration`                 | `0` (forever)    | How long to run the generator (e.g., `5m`, `1h30m`)   |
| `--report-interval`          | `3s`             | Interval to report statistics                         |
| `--push-interval`            | `50ms`           | Interval between batch pushes                         |
| `--workers`                  | `1`              | Number of concurrent workers to run                   |
| `--control-endpoint`         | (none)           | Endpoint of control server for distributed testing    |
| `--header`                   | (none)           | Custom headers to send (format: `Key=Value`, repeatable) |
| `--http`                     | `false`          | Use HTTP/JSON instead of gRPC for OTLP export         |

### Sink Command (`sink`)

Run a sink server that receives telemetry and tracks message delivery:

```bash
otel-loadgen sink [flags]
```

#### Sink Flags

| Flag                | Default           | Description                                    |
| ------------------- | ----------------- | ---------------------------------------------- |
| `--addr`            | `localhost:5317`  | Address to listen on for incoming telemetry    |
| `--control-addr`    | `localhost:5000`  | Control server address for reporting stats     |
| `--report-interval` | `3s`              | Interval to report delivery statistics         |

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
# Run trace generator
docker run -ti streamfold/otel-loadgen gen traces [...]

# Run sink server
docker run -ti streamfold/otel-loadgen sink [...]
```

## Usage Examples

### Basic Trace Generation

```bash
# Generate traces indefinitely with default settings
./dist/otel-loadgen gen traces

# Generate traces for 5 minutes with custom endpoint
./dist/otel-loadgen gen traces --otlp-endpoint http://collector:4317 --duration 5m

# High-throughput test with multiple workers
./dist/otel-loadgen gen traces \
  --workers 10 \
  --spans-per-resource 1000 \
  --push-interval 10ms \
  --otlp-resources-per-batch 5

# Send traces with custom authorization header
./dist/otel-loadgen gen traces \
  --otlp-endpoint http://collector:4317 \
  --header "Authorization=Bearer <token>" \
  --header "X-Custom-Header=value"
```

### Sink Server

```bash
# Run a sink server to receive telemetry
./dist/otel-loadgen sink

# Run sink server with custom addresses
./dist/otel-loadgen sink \
  --addr localhost:5317 \
  --control-addr localhost:5000 \
  --report-interval 5s
```

### Distributed Load Testing

```bash
# Terminal 1: Start the sink server (receiver)
./dist/otel-loadgen sink --addr localhost:5317 --control-addr localhost:5000

# Terminal 2: Start Rotel, exporting OTLP to localhost:5317
rotel start --otlp-exporter-endpoint localhost:5317

# Terminal 3: Start generator 
./dist/otel-loadgen gen traces \
  --otlp-endpoint http://localhost:4317 \
  --control-endpoint http://localhost:5000 \
  --workers 5 \
  --duration 10m
```

## License

See [LICENSE](LICENSE) file for details.
