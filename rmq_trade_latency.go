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

	stats := NewStats(sampleSize)
	ticker := time.NewTicker(reportEvery)
	defer ticker.Stop()

	var parseFail uint64

	log.Printf("RMF latency probe started: queue=%s prefetch=%d autoAck=%v report_every=%s",
		queue, prefetch, autoAck, reportEvery)

	for {
		select {
		case <-ctx.Done():
			printReport(stats, parseFail, sampleSize)
			log.Println("Exiting.")
			return

		case <-ticker.C:
			printReport(stats, parseFail, sampleSize)

		case d, ok := <-deliveries:
			if !ok {
				log.Println("Deliveries channel closed.")
				return
			}

			consumeTs := time.Now()

			var env RMFEnvelope
			if err := json.Unmarshal(d.Body, &env); err != nil {
				parseFail++
				if !autoAck {
					_ = d.Nack(false, false)
				}
				continue
			}

			tt, err := time.ParseInLocation(rmfTimeLayout, env.Trade.TradeTime, time.UTC)
			if err != nil {
				parseFail++
				if !autoAck {
					_ = d.Nack(false, false)
				}
				continue
			}

			latNs := consumeTs.UnixNano() - tt.UnixNano()
			stats.Add(latNs)

			if logEach {
				// Print only the fields you care about + latency
				t := env.Trade
				log.Printf("lat=%s trade_time=%s core_symbol=%s broker_id=%s warehoused=%v taker_login=%s taker_px=%s side=%s mt_type=%s filled_vol=%s",
					time.Duration(latNs),
					t.TradeTime,
					t.CoreSymbol,
					t.BrokerID,
					t.IsWarehoused,
					t.TakerLogin,
					t.TakerExecutedPrice,
					t.CoreOrderSide,
					t.TakerMtOrderType,
					t.MakerFilledVolume,
				)
			}

			if !autoAck {
				_ = d.Ack(false)
			}
		}
	}
}

func printReport(s *Stats, parseFail uint64, sampleSize int) {
	count, avgNs, minNs, maxNs, lastNs, p50, p95, p99, n := s.Snapshot()
	if count == 0 {
		log.Printf("count=0 parse_fail=%d", parseFail)
		return
	}
	log.Printf(
		"count=%d window=%d parse_fail=%d | min=%s avg=%s p50=%s p95=%s p99=%s max=%s last=%s",
		count, n, parseFail,
		time.Duration(minNs),
		time.Duration(int64(avgNs)),
		time.Duration(p50),
		time.Duration(p95),
		time.Duration(p99),
		time.Duration(maxNs),
		time.Duration(lastNs),
	)

	// Only save report if we have enough samples
	if n < sampleSize {
		return
	}

	// Save report to file
	now := time.Now().UTC()
	ts := now.UnixNano()
	report := struct {
		TimestampUnixNs int64   `json:"timestamp_unix_ns"`
		Count           uint64  `json:"count"`
		Window          int     `json:"window"`
		ParseFail       uint64  `json:"parse_fail"`
		MinNs           int64   `json:"min_ns"`
		AvgNs           float64 `json:"avg_ns"`
		P50Ns           int64   `json:"p50_ns"`
		P95Ns           int64   `json:"p95_ns"`
		P99Ns           int64   `json:"p99_ns"`
		MaxNs           int64   `json:"max_ns"`
	}{
		TimestampUnixNs: ts,
		Count:           count,
		Window:          n,
		ParseFail:       parseFail,
		MinNs:           minNs,
		AvgNs:           avgNs,
		P50Ns:           p50,
		P95Ns:           p95,
		P99Ns:           p99,
		MaxNs:           maxNs,
	}

	dir := filepath.Join("data", "latency_report", now.Format("2006-01-02"))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		log.Printf("report save failed: %v", err)
		return
	}
	path := filepath.Join(dir, now.Format("150405")+".json")
	f, err := os.Create(path)
	if err != nil {
		log.Printf("report save failed: %v", err)
		return
	}
	if err := json.NewEncoder(f).Encode(report); err != nil {
		log.Printf("report save failed: %v", err)
	}
	if err := f.Close(); err != nil {
		log.Printf("report save failed: %v", err)
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
