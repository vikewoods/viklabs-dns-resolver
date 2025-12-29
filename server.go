package main

import (
	"log"
	"sync"
	"time"

	"github.com/miekg/dns"
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

// Simple in-memory cache
// TODO: Add Redis cache support
type CacheEntry struct {
	Response  *dns.Msg
	ExpiresAt time.Time
}

var (
	cache      = make(map[string]*CacheEntry)
	cacheMutex sync.RWMutex
	cacheTTL   = 5 * time.Minute // Default cache TTL
)

func main() {
	// Bind to IP or Network Interface
	listenAddr := "192.168.190.25:5454"

	// Handle all DNS zones with our handler
	dns.HandleFunc(".", handleDNSRequest)

	// Start cache cleanup goroutine
	go cleanupCache()

	// UDP server
	udpServer := &dns.Server{
		Addr:      listenAddr,
		Net:       "udp",
		ReusePort: true,
		UDPSize:   65535,
	}

	// TCP server
	tcpServer := &dns.Server{
		Addr:      listenAddr,
		Net:       "tcp",
		ReusePort: true,
	}

	// Start UDP server
	go func() {
		log.Printf("Starting DNS server on UDP %s", listenAddr)
		if err := udpServer.ListenAndServe(); err != nil {
			log.Fatalf("Failed to start UDP server: %v", err)
		}
	}()

	// Start TCP server
	go func() {
		log.Printf("Starting DNS server on TCP %s", listenAddr)
		if err := tcpServer.ListenAndServe(); err != nil {
			log.Fatalf("Failed to start TCP server: %v", err)
		}
	}()

	log.Println("DNS resolver is running. Press Ctrl+C to stop.")
	select {}
}

// handleDNSRequest handles incoming DNS queries with caching
func handleDNSRequest(w dns.ResponseWriter, r *dns.Msg) {
	if len(r.Question) == 0 {
		sendError(w, r, dns.RcodeFormatError)
		return
	}

	q := r.Question[0]
	cacheKey := getCacheKey(q)

	// Check cache first
	if cachedResp := getFromCache(cacheKey); cachedResp != nil {
		cachedResp.Id = r.Id // Match request ID
		_ = w.WriteMsg(cachedResp)
		return
	}

	// Forward to upstream
	resp, err := forwardToUpstream(r)
	if err != nil {
		log.Printf("Upstream error for %s: %v", q.Name, err)
		sendError(w, r, dns.RcodeServerFailure)
		return
	}

	// Cache the response
	saveToCache(cacheKey, resp)

	// Send response
	resp.Id = r.Id
	if err := w.WriteMsg(resp); err != nil {
		log.Printf("Failed to write response: %v", err)
	}
}

// forwardToUpstream sends the DNS query to upstream servers with retries
func forwardToUpstream(req *dns.Msg) (*dns.Msg, error) {
	var lastErr error

	for _, upstream := range upstreamServers {
		resp, _, err := dnsClient.Exchange(req, upstream)
		if err != nil {
			lastErr = err
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
	return nil, dns.ErrId
}

// Cache helper functions
func getCacheKey(q dns.Question) string {
	return q.Name + ":" + dns.TypeToString[q.Qtype]
}

func getFromCache(key string) *dns.Msg {
	cacheMutex.RLock()
	defer cacheMutex.RUnlock()

	entry, exists := cache[key]
	if !exists || time.Now().After(entry.ExpiresAt) {
		return nil
	}

	return entry.Response.Copy()
}

func saveToCache(key string, msg *dns.Msg) {
	cacheMutex.Lock()
	defer cacheMutex.Unlock()

	// Use the minimum TTL from the response or default
	ttl := cacheTTL
	if len(msg.Answer) > 0 {
		ttl = time.Duration(msg.Answer[0].Header().Ttl) * time.Second
		if ttl > 5*time.Minute {
			ttl = 5 * time.Minute
		}
		if ttl < 30*time.Second {
			ttl = 30 * time.Second
		}
	}

	cache[key] = &CacheEntry{
		Response:  msg.Copy(),
		ExpiresAt: time.Now().Add(ttl),
	}
}

func cleanupCache() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		cacheMutex.Lock()
		now := time.Now()
		for key, entry := range cache {
			if now.After(entry.ExpiresAt) {
				delete(cache, key)
			}
		}
		cacheMutex.Unlock()
	}
}

func sendError(w dns.ResponseWriter, r *dns.Msg, rcode int) {
	m := new(dns.Msg)
	m.SetReply(r)
	m.Rcode = rcode
	_ = w.WriteMsg(m)
}
