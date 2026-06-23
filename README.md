# RMF-Latency-Measure

Measure end-to-end latency of RMF trade messages from RabbitMQ, broken down into three stages: publish delay, broker delivery, and total latency. Trades are grouped by MT4/MT5 based on `taker_name`.

## What it does

- Connects to RabbitMQ over AMQP.
- Consumes from `QUEUE_NAME` with configurable prefetch and auto-ack.
- Skips the first 1 minute of messages for warm-up.
- Parses each message as an `RMFEnvelope` containing trade fields.
- Groups trades into **MT4** or **MT5** based on whether `taker_name` contains "MT5".
- Computes three latency metrics per group (see below).
- Maintains a rolling sample window for percentile calculations (p50, p95, p99).
- Logs periodic stats to stdout and writes JSON reports to disk once the sample window is full.

## Latency Metrics

Three latency measurements are tracked independently for MT4 and MT5:

```
trade_time ──[PubDelay]──> timestamp_in_ms ──[HeaderTs]──> consumeTs
             (RMF处理耗时)                    (MQ投递耗时)
└─────────────────────[TradeTime]─────────────────────────┘
                       (端到端总延迟)
```

| Metric | Formula | Meaning |
|---|---|---|
| **TradeTime** | `consumeTs - trade_time` | End-to-end latency from when the trade occurred to when the Go consumer received the message. |
| **HeaderTs** | `consumeTs - timestamp_in_ms` | RabbitMQ delivery latency — from when the message was published to the broker to when the consumer received it. |
| **PubDelay** | `timestamp_in_ms - trade_time` | Publish delay — from when the trade occurred to when the message was published to RabbitMQ. Reflects RMF processing time. |

- `trade_time`: parsed from the JSON body field `trade.trade_time` (format `2006-01-02 15:04:05.000000`, UTC).
- `timestamp_in_ms`: extracted from the AMQP message header `timestamp_in_ms` (milliseconds since epoch). This is set by the publisher when it sends the message to RabbitMQ.
- `consumeTs`: the wall-clock time when the Go consumer receives the delivery.

**HeaderTs** and **PubDelay** are only computed when the `timestamp_in_ms` header is present. **TradeTime** is always computed.

## RMF Message Format

```json
{
  "feedVersion": "...",
  "trade": {
    "core_symbol": "XAUUSD",
    "broker_id": "...",
    "is_warehoused": false,
    "trade_time": "2026-01-29 13:17:30.123456",
    "trade_id": 12345,
    "order_id": 67890,
    "taker_name": "MT4-Live",
    "taker_login": "100001",
    "taker_executed_price": "2650.50",
    "core_order_side": "BUY",
    "taker_mt_order_type": "MARKET",
    "maker_filled_volume": "1.00"
  }
}
```

## Reports

### Log output

Three lines per group (MT4/MT5) every `REPORT_EVERY_S` seconds:

```
[MT4][TradeTime] count=101079 window=50000 parse_fail=0 json_fail=0 trade_time_fail=0 | min=5.518ms avg=101.795ms p50=17.971ms p95=687.219ms p99=969.146ms max=1.280s last=...
[MT4][HeaderTs]  count=101079 window=50000              | min=1.154ms avg=9.635ms p50=3.699ms p95=31.648ms p99=95.398ms max=336.065ms last=...
[MT4][PubDelay]  count=101079 window=50000              | min=3.237ms avg=92.159ms p50=10.914ms p95=671.457ms p99=960.103ms max=1.277s last=...
```

### JSON reports

Written to `data/{group}/latency_report/YYYY-MM-DD/HHMMSS.json` (UTC) once the rolling sample window reaches `SAMPLE_SIZE`. Separate directories for MT4 and MT5.

```
data/
├── MT4/
│   └── latency_report/
│       └── 2026-01-29/
│           ├── 131730.json
│           └── ...
├── MT5/
│   └── latency_report/
│       └── ...
└── latency_report/          # legacy (pre-grouping)
```

JSON fields per report:

```json
{
  "timestamp_unix_ns": 1769692650574680730,
  "group": "MT4",
  "count": 101079,
  "parse_fail": 0,
  "json_parse_fail": 0,
  "trade_time_parse_fail": 0,
  "trade_time": {
    "window": 50000,
    "min_ns": 5518030,
    "avg_ns": 101795491.63,
    "p50_ns": 17971544,
    "p95_ns": 687219263,
    "p99_ns": 969146932,
    "max_ns": 1280907204
  },
  "header_ts": {
    "window": 50000,
    "min_ns": 1154175,
    "avg_ns": 9635955.67,
    "p50_ns": 3699370,
    "p95_ns": 31648772,
    "p99_ns": 95398108,
    "max_ns": 336065060
  },
  "publish_delay": {
    "window": 50000,
    "min_ns": 3237000,
    "avg_ns": 92159535.97,
    "p50_ns": 10914000,
    "p95_ns": 671457000,
    "p99_ns": 960103000,
    "max_ns": 1277508000
  }
}
```

## Configuration (.env)

The app loads `.env` from the repo root if present. All values are required.

| Variable | Description |
|---|---|
| `RABBITMQ_HOST` | Host or IP |
| `RABBITMQ_PORT` | Port number (e.g. `5672`) |
| `RABBITMQ_USER` | AMQP username |
| `RABBITMQ_PASS` | AMQP password |
| `QUEUE_NAME` | Queue to consume from |
| `PREFETCH` | Consumer QoS prefetch count |
| `AUTO_ACK` | `true`/`false` — whether to auto-acknowledge messages |
| `REPORT_EVERY_S` | Seconds between periodic log reports |
| `SAMPLE_SIZE` | Rolling sample window size for percentile calculations |
| `LOG_EACH` | `true`/`false` — if true, logs individual trades with publish delay > 700ms |

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
go build
go get github.com/rabbitmq/amqp091-go
go mod tidy
go fmt ./...
```
