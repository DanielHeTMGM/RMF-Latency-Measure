# RMF-Latency-Measure
Measure the latency between RMF `trade_time` in each message and the moment this Go consumer processes it.

## What it does
- Connects to RabbitMQ over AMQP.
- Consumes from `QUEUE_NAME` with configurable prefetch and auto-ack.
- Parses `trade_time` in the format `2006-01-02 15:04:05.000000` (UTC).
- Computes latency and maintains a rolling sample window for percentiles.
- Logs periodic stats and writes JSON reports.

## Reports
- Log line includes `count`, `window`, `parse_fail`, and `min/avg/p50/p95/p99/max`.
- JSON output path (UTC): `./data/latency_report/YYYY-MM-DD/HHMMSS.json`
- JSON fields: `timestamp_unix_ns`, `count`, `window`, `parse_fail`, `min_ns`, `avg_ns`, `p50_ns`, `p95_ns`, `p99_ns`, `max_ns`.

## Configuration (.env)
The app loads `.env` from the repo root if present. All values are required.

- `RABBITMQ_HOST`: host or IP
- `RABBITMQ_PORT`: port number (e.g. `5672`)
- `RABBITMQ_USER`
- `RABBITMQ_PASS`
- `QUEUE_NAME`
- `PREFETCH`: consumer QoS prefetch count
- `AUTO_ACK`: `true`/`false`
- `REPORT_EVERY_S`: seconds between reports
- `SAMPLE_SIZE`: rolling sample size for percentiles
- `LOG_EACH`: `true`/`false`, logs each trade line

Example `.env`:
```
RABBITMQ_HOST=localhost
RABBITMQ_PORT=5672
RABBITMQ_USER=guest
RABBITMQ_PASS=guest
QUEUE_NAME=rmf_trades_xauusd
PREFETCH=50
AUTO_ACK=true
REPORT_EVERY_S=5
SAMPLE_SIZE=50000
LOG_EACH=false
```

## Dev commands
```
go run .
go get github.com/rabbitmq/amqp091-go
go mod tidy
go fmt ./...
```
