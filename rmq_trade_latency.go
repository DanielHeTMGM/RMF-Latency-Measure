// rmf_latency_probe.go
//
// Measure latency between RMF trade_time and the time Go consumes the message.
// Also extracts selected fields from the RMF payload.
//
// Env vars:
//   RABBITMQ_HOST (default: localhost)
//   RABBITMQ_PORT (default: 5671) // TLS
//   RABBITMQ_USER (default: guest)
//   RABBITMQ_PASS (default: guest)
//   QUEUE_NAME    (default: rmf_trades_xauusd)
//   PREFETCH       (default: 50)
//   AUTO_ACK       (default: true)
//   REPORT_EVERY_S (default: 5)
//   SAMPLE_SIZE    (default: 50000)
//   LOG_EACH       (default: false)  // if true, prints each trade line with latency
//
// Notes:
// - trade_time format is "2006-01-02 15:04:05.000000" (microseconds)

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"log"
	"math"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const rmfTimeLayout = "2006-01-02 15:04:05.000000"

type RMFEnvelope struct {
	FeedVersion string   `json:"feedVersion"`
	Trade       RMFTrade `json:"trade"`
}

type RMFTrade struct {
	CoreSymbol         string `json:"core_symbol"`
	BrokerID           string `json:"broker_id"`
	IsWarehoused       bool   `json:"is_warehoused"`
	TradeTime          string `json:"trade_time"`
	TradeID            int64  `json:"trade_id"`
	OrderID            int64  `json:"order_id"`
	TakerName          string `json:"taker_name"`
	TakerLogin         string `json:"taker_login"`
	TakerExecutedPrice string `json:"taker_executed_price"`
	CoreOrderSide      string `json:"core_order_side"`
	TakerMtOrderType   string `json:"taker_mt_order_type"`
	MakerFilledVolume  string `json:"maker_filled_volume"`
}

type Stats struct {
	Count  uint64
	SumNs  float64
	MinNs  int64
	MaxNs  int64
	LastNs int64

	Samples []int64
	Cap     int
	Idx     int
	Filled  bool
}

func NewStats(sampleCap int) *Stats {
	return &Stats{
		MinNs:   math.MaxInt64,
		MaxNs:   math.MinInt64,
		Samples: make([]int64, sampleCap),
		Cap:     sampleCap,
	}
}

func (s *Stats) Add(latNs int64) {
	s.Count++
	s.SumNs += float64(latNs)
	if latNs < s.MinNs {
		s.MinNs = latNs
	}
	if latNs > s.MaxNs {
		s.MaxNs = latNs
	}
	s.LastNs = latNs
	s.Samples[s.Idx] = latNs
	s.Idx++
	if s.Idx >= s.Cap {
		s.Idx = 0
		s.Filled = true
	}
}

func (s *Stats) Snapshot() (count uint64, avgNs float64, minNs, maxNs, lastNs int64, p50, p95, p99 int64, n int) {
	count = s.Count
	if s.Count > 0 {
		avgNs = s.SumNs / float64(s.Count)
	}
	minNs, maxNs, lastNs = s.MinNs, s.MaxNs, s.LastNs

	var window []int64
	if s.Filled {
		window = make([]int64, s.Cap)
		copy(window, s.Samples)
		n = s.Cap
	} else {
		window = make([]int64, s.Idx)
		copy(window, s.Samples[:s.Idx])
		n = s.Idx
	}
	if n == 0 {
		return count, avgNs, minNs, maxNs, lastNs, 0, 0, 0, 0
	}

	sort.Slice(window, func(i, j int) bool { return window[i] < window[j] })
	p50 = percentile(window, 50)
	p95 = percentile(window, 95)
	p99 = percentile(window, 99)
	return count, avgNs, minNs, maxNs, lastNs, p50, p95, p99, n
}

func percentile(sorted []int64, pct int) int64 {
	if len(sorted) == 0 {
		return 0
	}
	if pct <= 0 {
		return sorted[0]
	}
	if pct >= 100 {
		return sorted[len(sorted)-1]
	}
	rank := int(math.Ceil(float64(pct) / 100.0 * float64(len(sorted))))
	if rank < 1 {
		rank = 1
	}
	if rank > len(sorted) {
		rank = len(sorted)
	}
	return sorted[rank-1]
}

func main() {
	if err := loadDotEnv(".env"); err != nil {
		log.Fatal(err)
	}

	amqpURL := buildAMQPURL()
	queue := getenv("QUEUE_NAME")
	prefetch := getenvInt("PREFETCH")
	autoAck := getenvBool("AUTO_ACK")
	reportEvery := time.Duration(getenvInt("REPORT_EVERY_S")) * time.Second
	sampleSize := getenvInt("SAMPLE_SIZE")
	logEach := getenvBool("LOG_EACH")

	conn, err := amqp.Dial(amqpURL)
	must(err)
	defer conn.Close()

	ch, err := conn.Channel()
	must(err)
	defer ch.Close()

	must(ch.Qos(prefetch, 0, false))

	startingTime := time.Now()
	deliveries, err := ch.Consume(
		queue,
		"rmf_latency_probe",
		autoAck,
		false,
		false,
		false,
		nil,
	)
	must(err)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// MT4 stats
	mt4HeaderStats := NewStats(sampleSize)
	mt4TradeTimeStats := NewStats(sampleSize)
	mt4PublishDelayStats := NewStats(sampleSize)
	var mt4ParseFail, mt4JSONParseFail, mt4TradeTimeParseFail uint64

	// MT5 stats
	mt5HeaderStats := NewStats(sampleSize)
	mt5TradeTimeStats := NewStats(sampleSize)
	mt5PublishDelayStats := NewStats(sampleSize)
	var mt5ParseFail, mt5JSONParseFail, mt5TradeTimeParseFail uint64

	ticker := time.NewTicker(reportEvery)
	defer ticker.Stop()

	log.Printf("RMF latency probe started: queue=%s prefetch=%d autoAck=%v report_every=%s",
		queue, prefetch, autoAck, reportEvery)

	for {
		select {
		case <-ctx.Done():
			printReport("MT4", mt4HeaderStats, mt4TradeTimeStats, mt4PublishDelayStats, mt4ParseFail, mt4JSONParseFail, mt4TradeTimeParseFail, sampleSize)
			printReport("MT5", mt5HeaderStats, mt5TradeTimeStats, mt5PublishDelayStats, mt5ParseFail, mt5JSONParseFail, mt5TradeTimeParseFail, sampleSize)
			log.Println("Exiting.")
			return

		case <-ticker.C:
			printReport("MT4", mt4HeaderStats, mt4TradeTimeStats, mt4PublishDelayStats, mt4ParseFail, mt4JSONParseFail, mt4TradeTimeParseFail, sampleSize)
			printReport("MT5", mt5HeaderStats, mt5TradeTimeStats, mt5PublishDelayStats, mt5ParseFail, mt5JSONParseFail, mt5TradeTimeParseFail, sampleSize)

		case d, ok := <-deliveries:
			if !ok {
				log.Println("Deliveries channel closed.")
				return
			}

			if time.Since(startingTime) < time.Minute*1 {
				//Skip messages received within the first 1 minutes to allow warm-up
				if !autoAck {
					_ = d.Ack(false)
				}
				continue
			}
			consumeTs := time.Now()

			var env RMFEnvelope
			if err := json.Unmarshal(d.Body, &env); err != nil {
				// Can't determine group without parsing, increment both
				mt4ParseFail++
				mt4JSONParseFail++
				if !autoAck {
					_ = d.Nack(false, false)
				}
				continue
			}

			// Determine MT4 vs MT5 group based on taker_name
			isMT5 := strings.Contains(env.Trade.TakerName, "MT5")

			// Select appropriate stats based on group
			var headerStats, tradeTimeStats, publishDelayStats *Stats
			var parseFail, jsonParseFail, tradeTimeParseFail *uint64
			if isMT5 {
				headerStats = mt5HeaderStats
				tradeTimeStats = mt5TradeTimeStats
				publishDelayStats = mt5PublishDelayStats
				parseFail = &mt5ParseFail
				jsonParseFail = &mt5JSONParseFail
				tradeTimeParseFail = &mt5TradeTimeParseFail
			} else {
				headerStats = mt4HeaderStats
				tradeTimeStats = mt4TradeTimeStats
				publishDelayStats = mt4PublishDelayStats
				parseFail = &mt4ParseFail
				jsonParseFail = &mt4JSONParseFail
				tradeTimeParseFail = &mt4TradeTimeParseFail
			}
			_ = jsonParseFail // suppress unused warning

			// Parse trade_time from body
			tradeTime, err := time.ParseInLocation(rmfTimeLayout, env.Trade.TradeTime, time.UTC)
			if err != nil {
				*parseFail++
				*tradeTimeParseFail++
				if !autoAck {
					_ = d.Nack(false, false)
				}
				continue
			}

			// Extract timestamp_in_ms from headers (message creation time)
			var headerTs time.Time
			var hasHeaderTs bool
			if tsMs, ok := d.Headers["timestamp_in_ms"].(int64); ok {
				headerTs = time.Unix(0, tsMs*int64(time.Millisecond))
				hasHeaderTs = true
			}

			// Calculate both latencies
			tradeTimeLatNs := consumeTs.UnixNano() - tradeTime.UnixNano()
			tradeTimeStats.Add(tradeTimeLatNs)

			var headerLatNs int64
			var publishDelayNs int64
			if hasHeaderTs {
				headerLatNs = consumeTs.UnixNano() - headerTs.UnixNano()
				headerStats.Add(headerLatNs)

				publishDelayNs = headerTs.UnixNano() - tradeTime.UnixNano()
				publishDelayStats.Add(publishDelayNs)
			}

			if logEach {
				// Print only the fields you care about + latencies
				t := env.Trade
				if hasHeaderTs && publishDelayNs > 700000000 {
					// log.Printf("header_lat=%s trade_time_lat=%s publish_delay=%s header_ts=%s trade_time=%s core_symbol=%s broker_id=%s warehoused=%v taker_login=%s taker_px=%s side=%s mt_type=%s filled_vol=%s",
					// 	time.Duration(headerLatNs),
					// 	time.Duration(tradeTimeLatNs),
					// 	time.Duration(publishDelayNs),
					// 	headerTs.Format(rmfTimeLayout),
					// 	t.TradeTime,
					// 	t.CoreSymbol,
					// 	t.BrokerID,
					// 	t.IsWarehoused,
					// 	t.TakerLogin,
					// 	t.TakerExecutedPrice,
					// 	t.CoreOrderSide,
					// 	t.TakerMtOrderType,
					// 	t.MakerFilledVolume,
					// )

					log.Printf("header_ts=%s trade_time=%s trade_id=%d order_id=%d core_symbol=%s broker_id=%s warehoused=%v taker_login=%s taker_px=%s side=%s mt_type=%s filled_vol=%s",
						headerTs.Format(rmfTimeLayout),
						t.TradeTime,
						t.TradeID,
						t.OrderID,
						t.CoreSymbol,
						t.BrokerID,
						t.IsWarehoused,
						t.TakerLogin,
						t.TakerExecutedPrice,
						t.CoreOrderSide,
						t.TakerMtOrderType,
						t.MakerFilledVolume,
					)
				} else {
					// log.Printf("trade_time_lat=%s trade_time=%s core_symbol=%s broker_id=%s warehoused=%v taker_login=%s taker_px=%s side=%s mt_type=%s filled_vol=%s",
					// 	time.Duration(tradeTimeLatNs),
					// 	t.TradeTime,
					// 	t.CoreSymbol,
					// 	t.BrokerID,
					// 	t.IsWarehoused,
					// 	t.TakerLogin,
					// 	t.TakerExecutedPrice,
					// 	t.CoreOrderSide,
					// 	t.TakerMtOrderType,
					// 	t.MakerFilledVolume,
					// )
				}
			}

			if !autoAck {
				_ = d.Ack(false)
			}
		}
	}
}

func printReport(group string, headerStats *Stats, tradeTimeStats *Stats, publishDelayStats *Stats, parseFail uint64, jsonParseFail uint64, tradeTimeParseFail uint64, sampleSize int) {
	ttCount, ttAvgNs, ttMinNs, ttMaxNs, ttLastNs, ttP50, ttP95, ttP99, ttN := tradeTimeStats.Snapshot()
	if ttCount == 0 {
		log.Printf("[%s] count=0 parse_fail=%d json_fail=%d trade_time_fail=%d", group, parseFail, jsonParseFail, tradeTimeParseFail)
		return
	}

	log.Printf(
		"[%s][TradeTime] count=%d window=%d parse_fail=%d json_fail=%d trade_time_fail=%d | min=%s avg=%s p50=%s p95=%s p99=%s max=%s last=%s",
		group, ttCount, ttN, parseFail, jsonParseFail, tradeTimeParseFail,
		time.Duration(ttMinNs),
		time.Duration(int64(ttAvgNs)),
		time.Duration(ttP50),
		time.Duration(ttP95),
		time.Duration(ttP99),
		time.Duration(ttMaxNs),
		time.Duration(ttLastNs),
	)

	hCount, hAvgNs, hMinNs, hMaxNs, hLastNs, hP50, hP95, hP99, hN := headerStats.Snapshot()
	if hCount > 0 {
		log.Printf(
			"[%s][HeaderTs]  count=%d window=%d              | min=%s avg=%s p50=%s p95=%s p99=%s max=%s last=%s",
			group, hCount, hN,
			time.Duration(hMinNs),
			time.Duration(int64(hAvgNs)),
			time.Duration(hP50),
			time.Duration(hP95),
			time.Duration(hP99),
			time.Duration(hMaxNs),
			time.Duration(hLastNs),
		)
	}

	pdCount, pdAvgNs, pdMinNs, pdMaxNs, pdLastNs, pdP50, pdP95, pdP99, pdN := publishDelayStats.Snapshot()
	if pdCount > 0 {
		log.Printf(
			"[%s][PubDelay]  count=%d window=%d              | min=%s avg=%s p50=%s p95=%s p99=%s max=%s last=%s",
			group, pdCount, pdN,
			time.Duration(pdMinNs),
			time.Duration(int64(pdAvgNs)),
			time.Duration(pdP50),
			time.Duration(pdP95),
			time.Duration(pdP99),
			time.Duration(pdMaxNs),
			time.Duration(pdLastNs),
		)
	}

	// Only save report if we have enough samples
	if ttN < sampleSize {
		return
	}

	// Save report to file
	now := time.Now().UTC()
	ts := now.UnixNano()
	report := struct {
		TimestampUnixNs int64  `json:"timestamp_unix_ns"`
		Group           string `json:"group"`
		Count           uint64 `json:"count"`
		ParseFail       uint64 `json:"parse_fail"`
		JSONParseFail   uint64 `json:"json_parse_fail"`
		TradeTimeFail   uint64 `json:"trade_time_parse_fail"`
		TradeTime       struct {
			Window int     `json:"window"`
			MinNs  int64   `json:"min_ns"`
			AvgNs  float64 `json:"avg_ns"`
			P50Ns  int64   `json:"p50_ns"`
			P95Ns  int64   `json:"p95_ns"`
			P99Ns  int64   `json:"p99_ns"`
			MaxNs  int64   `json:"max_ns"`
		} `json:"trade_time"`
		HeaderTs struct {
			Window int     `json:"window"`
			MinNs  int64   `json:"min_ns"`
			AvgNs  float64 `json:"avg_ns"`
			P50Ns  int64   `json:"p50_ns"`
			P95Ns  int64   `json:"p95_ns"`
			P99Ns  int64   `json:"p99_ns"`
			MaxNs  int64   `json:"max_ns"`
		} `json:"header_ts"`
		PublishDelay struct {
			Window int     `json:"window"`
			MinNs  int64   `json:"min_ns"`
			AvgNs  float64 `json:"avg_ns"`
			P50Ns  int64   `json:"p50_ns"`
			P95Ns  int64   `json:"p95_ns"`
			P99Ns  int64   `json:"p99_ns"`
			MaxNs  int64   `json:"max_ns"`
		} `json:"publish_delay"`
	}{
		TimestampUnixNs: ts,
		Group:           group,
		Count:           ttCount,
		ParseFail:       parseFail,
		JSONParseFail:   jsonParseFail,
		TradeTimeFail:   tradeTimeParseFail,
	}
	report.TradeTime.Window = ttN
	report.TradeTime.MinNs = ttMinNs
	report.TradeTime.AvgNs = ttAvgNs
	report.TradeTime.P50Ns = ttP50
	report.TradeTime.P95Ns = ttP95
	report.TradeTime.P99Ns = ttP99
	report.TradeTime.MaxNs = ttMaxNs

	if hCount > 0 {
		report.HeaderTs.Window = hN
		report.HeaderTs.MinNs = hMinNs
		report.HeaderTs.AvgNs = hAvgNs
		report.HeaderTs.P50Ns = hP50
		report.HeaderTs.P95Ns = hP95
		report.HeaderTs.P99Ns = hP99
		report.HeaderTs.MaxNs = hMaxNs
	}

	if pdCount > 0 {
		report.PublishDelay.Window = pdN
		report.PublishDelay.MinNs = pdMinNs
		report.PublishDelay.AvgNs = pdAvgNs
		report.PublishDelay.P50Ns = pdP50
		report.PublishDelay.P95Ns = pdP95
		report.PublishDelay.P99Ns = pdP99
		report.PublishDelay.MaxNs = pdMaxNs
	}

	// Save to group-specific directory: data/{group}/latency_report/{date}/
	dir := filepath.Join("data", group, "latency_report", now.Format("2006-01-02"))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		log.Printf("[%s] report save failed: %v", group, err)
		return
	}
	path := filepath.Join(dir, now.Format("150405")+".json")
	f, err := os.Create(path)
	if err != nil {
		log.Printf("[%s] report save failed: %v", group, err)
		return
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(report); err != nil {
		log.Printf("[%s] report save failed: %v", group, err)
	}
	if err := f.Close(); err != nil {
		log.Printf("[%s] report save failed: %v", group, err)
	}
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func loadDotEnv(path string) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}
		if strings.HasPrefix(line, "export ") {
			line = strings.TrimSpace(line[len("export "):])
		}
		line = stripInlineComment(line)
		if line == "" {
			continue
		}
		key, val, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		val = strings.TrimSpace(val)
		if len(val) >= 2 {
			if (val[0] == '"' && val[len(val)-1] == '"') || (val[0] == '\'' && val[len(val)-1] == '\'') {
				val = val[1 : len(val)-1]
			}
		}
		if err := os.Setenv(key, val); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

func stripInlineComment(s string) string {
	inSingle := false
	inDouble := false
	for i, r := range s {
		switch r {
		case '\'':
			if !inDouble {
				inSingle = !inSingle
			}
		case '"':
			if !inSingle {
				inDouble = !inDouble
			}
		case '#', ';':
			if !inSingle && !inDouble {
				return strings.TrimSpace(s[:i])
			}
		}
	}
	return strings.TrimSpace(s)
}

// TODO: if no env var, fatal it
func getenv(k string) string {
	v := os.Getenv(k)
	if v == "" {
		log.Fatalf("missing env var: %s", k)
	}
	return v
}

func getenvInt(k string) int {
	v := os.Getenv(k)
	if v == "" {
		log.Fatalf("missing env var: %s", k)
	}
	x, err := strconv.Atoi(v)
	if err != nil {
		log.Fatalf("invalid int value for env var %s: %v", k, err)
	}
	return x
}

func getenvBool(k string) bool {
	v := os.Getenv(k)
	if v == "" {
		log.Fatalf("missing env var: %s", k)
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		log.Fatalf("invalid bool value for env var %s: %v", k, err)
	}
	return b
}

func buildAMQPURL() string {
	host := getenv("RABBITMQ_HOST")
	port := getenv("RABBITMQ_PORT")
	user := getenv("RABBITMQ_USER")
	pass := getenv("RABBITMQ_PASS")
	vhost := "/"
	scheme := "amqp"

	u := url.URL{
		Scheme: scheme,
		Host:   host + ":" + port,
		Path:   vhost,
		User:   url.UserPassword(user, pass),
	}
	return u.String()
}
