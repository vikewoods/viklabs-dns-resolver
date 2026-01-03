package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"sync"
	"time"

	"github.com/miekg/dns"
	"github.com/redis/go-redis/v9"
)

// Upstream DNS servers
var upstreamServers = []string{
	"1.1.1.1:53", // Cloudflare
	"8.8.8.8:53", // Google
}

// Global DNS client with connection pooling
var dnsClient = &dns.Client{
	Net:            "udp",
	ReadTimeout:    2 * time.Second,
	WriteTimeout:   2 * time.Second,
	SingleInflight: true,
}

var msgPool = &sync.Pool{
	New: func() interface{} { return new(dns.Msg) },
}

// Redis client
var redisClient *redis.Client
var ctx = context.Background()

func main() {
	// Configure Go runtime for maximum performance
	configureRuntime()

	// Get configuration from environment variables
	listenAddr := getEnv("LISTEN_ADDR", "0.0.0.0:5454")
	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")
	redisPassword := getEnv("REDIS_PASSWORD", "")
	redisPoolSize := getEnvInt("REDIS_POOL_SIZE", 100)

	// Initialize Redis with optimized settings
	redisClient = redis.NewClient(&redis.Options{
		Addr:            redisAddr,
		Password:        redisPassword,
		DB:              0,
		DialTimeout:     3 * time.Second,
		ReadTimeout:     1 * time.Second,
		WriteTimeout:    1 * time.Second,
		PoolSize:        redisPoolSize,
		MinIdleConns:    10,
		MaxRetries:      1,
		PoolTimeout:     2 * time.Second,
		ConnMaxIdleTime: 5 * time.Minute,
	})

	// Test Redis connection
	if err := redisClient.Ping(ctx).Err(); err != nil {
		log.Fatalf("[***] Failed to connect to Redis: %v", err)
	}
	log.Println("[**] Connected to Redis successfully")

	// Handle all DNS zones with our handler
	dns.HandleFunc(".", handleDNSRequest)

	// UDP server with optimized settings
	udpServer := &dns.Server{
		Addr:      listenAddr,
		Net:       "udp",
		ReusePort: true,
		UDPSize:   65535,
	}

	// TCP server with optimized settings
	tcpServer := &dns.Server{
		Addr:      listenAddr,
		Net:       "tcp",
		ReusePort: true,
	}

	// Start UDP server
	go func() {
		log.Printf("[*] Starting DNS server on UDP %s", listenAddr)
		if err := udpServer.ListenAndServe(); err != nil {
			log.Fatalf("Failed to start UDP server: %v", err)
		}
	}()

	// Start TCP server
	go func() {
		log.Printf("[*] Starting DNS server on TCP %s", listenAddr)
		if err := tcpServer.ListenAndServe(); err != nil {
			log.Fatalf("Failed to start TCP server: %v", err)
		}
	}()

	// Log runtime configuration
	logRuntimeInfo()

	// Mem stats logger
	go logMemStats()

	log.Println("[*] DNS resolver is running. Press Ctrl+C to stop.")
	select {}
}

// configureRuntime optimizes Go runtime settings for maximum performance
func configureRuntime() {
	// Get GOMAXPROCS from environment or use all CPUs
	maxProcs := getEnvInt("GOMAXPROCS", runtime.NumCPU())
	runtime.GOMAXPROCS(maxProcs)

	// Set GC target percentage (lower = more frequent GC, less memory usage)
	// Higher = less frequent GC, more memory usage, better performance
	gcPercent := getEnvInt("GOGC", 200)
	debug.SetGCPercent(gcPercent)

	// Set memory limit if specified (e.g., "512MiB", "2GiB")
	if memLimit := os.Getenv("GOMEMLIMIT"); memLimit != "" {
		debug.SetMemoryLimit(parseMemoryLimit(memLimit))
	}
}

// logRuntimeInfo logs current runtime configuration
func logRuntimeInfo() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	log.Printf("[**] Runtime Configuration:")
	log.Printf("    - GOMAXPROCS: %d (available CPUs: %d)", runtime.GOMAXPROCS(0), runtime.NumCPU())
	log.Printf("    - GOGC: %d%%", debug.SetGCPercent(-1))
	debug.SetGCPercent(debug.SetGCPercent(-1))
	log.Printf("    - Goroutines: %d", runtime.NumGoroutine())
	log.Printf("    - Memory Allocated: %.2f MB", float64(m.Alloc)/1024/1024)
}

// handleDNSRequest handles incoming DNS queries with Redis caching
func handleDNSRequest(w dns.ResponseWriter, r *dns.Msg) {
	startTime := time.Now()

	if len(r.Question) == 0 {
		sendError(w, r, dns.RcodeFormatError)
		return
	}

	q := r.Question[0]
	cacheKey := getCacheKey(q)

	// Check Redis cache first
	cachedResp, cacheHit := getFromRedisCache(cacheKey)
	if cacheHit && cachedResp != nil {
		cachedResp.Id = r.Id
		_ = w.WriteMsg(cachedResp)
		log.Printf("[+] CACHE_HIT | %s | %s | %dms",
			dns.TypeToString[q.Qtype],
			q.Name,
			time.Since(startTime).Milliseconds())
		return
	}

	// Forward to upstream
	resp, err := forwardToUpstream(r)
	if err != nil {
		log.Printf("[-] ERROR | %s | %s | %v", dns.TypeToString[q.Qtype], q.Name, err)
		sendError(w, r, dns.RcodeServerFailure)
		return
	}

	// Save to Redis cache (non-blocking)
	go saveToRedisCache(cacheKey, resp)

	// Send response
	resp.Id = r.Id
	if err := w.WriteMsg(resp); err != nil {
		log.Printf("[---] ERROR | Failed to write response: %v", err)
		return
	}

	log.Printf("[+] UPSTREAM | %s | %s | %dms",
		dns.TypeToString[q.Qtype],
		q.Name,
		time.Since(startTime).Milliseconds())
}

// forwardToUpstream sends the DNS query to upstream servers with retries
func forwardToUpstream(req *dns.Msg) (*dns.Msg, error) {
	var lastErr error

	for _, upstream := range upstreamServers {
		resp, _, err := dnsClient.Exchange(req, upstream)
		if err != nil {
			lastErr = err
			log.Printf("[--] WARN | Upstream %s failed: %v", upstream, err)
			continue
		}

		// Check if response is valid
		if resp != nil && resp.Rcode != dns.RcodeServerFailure && resp.Rcode != dns.RcodeRefused {
			return resp, nil
		}
	}

	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("all upstream servers failed")
}

// Redis Cache Functions
func getCacheKey(q dns.Question) string {
	return fmt.Sprintf("dns:%s:%s", q.Name, dns.TypeToString[q.Qtype])
}

func getFromRedisCache(key string) (*dns.Msg, bool) {
	data, err := redisClient.Get(ctx, key).Bytes()
	if err != nil {
		return nil, false // Cache miss or error
	}

	msg := new(dns.Msg)
	if err := msg.Unpack(data); err != nil {
		log.Printf("[--] WARN | Failed to unpack cached DNS message: %v", err)
		return nil, false
	}

	return msg, true
}

func saveToRedisCache(key string, msg *dns.Msg) {
	// Calculate TTL from DNS response
	ttl := 5 * time.Minute

	if len(msg.Answer) > 0 {
		ttl = time.Duration(msg.Answer[0].Header().Ttl) * time.Second
		// Cap TTL between 30s and 60 min
		if ttl > 1*time.Hour {
			ttl = 1 * time.Hour
		}
		if ttl < 30*time.Second {
			ttl = 30 * time.Second
		}
	}

	// Pack DNS message to binary
	data, err := msg.Pack()
	if err != nil {
		log.Printf("[--] WARN | Failed to pack DNS message for caching: %v", err)
		return
	}

	// Save to Redis with TTL
	if err := redisClient.Set(ctx, key, data, ttl).Err(); err != nil {
		log.Printf("[--] WARN | Failed to save to Redis cache: %v", err)
	}
}

func sendError(w dns.ResponseWriter, r *dns.Msg, rcode int) {
	m := msgPool.Get().(*dns.Msg)
	m.SetReply(r)
	m.Rcode = rcode
	_ = w.WriteMsg(m)
	msgPool.Put(m)
}

// Helper functions
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intVal, err := strconv.Atoi(value); err == nil {
			return intVal
		}
	}
	return defaultValue
}

func parseMemoryLimit(limit string) int64 {
	// Simple parser for memory limits like "512MiB", "2GiB"
	var multiplier int64 = 1
	numStr := limit

	if len(limit) > 3 {
		suffix := limit[len(limit)-3:]
		numStr = limit[:len(limit)-3]

		switch suffix {
		case "KiB":
			multiplier = 1024
		case "MiB":
			multiplier = 1024 * 1024
		case "GiB":
			multiplier = 1024 * 1024 * 1024
		}
	}

	if num, err := strconv.ParseInt(numStr, 10, 64); err == nil {
		return num * multiplier
	}

	return -1 // No limit
}

func logMemStats() {
	ticker := time.NewTicker(15 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		log.Printf("[**] Memory Stats: Alloc=%.2fMB, TotalAlloc=%.2fMB, Sys=%.2fMB, NumGC=%d, Goroutines=%d",
			float64(m.Alloc)/1024/1024,
			float64(m.TotalAlloc)/1024/1024,
			float64(m.Sys)/1024/1024,
			m.NumGC,
			runtime.NumGoroutine())
	}
}
