package bus

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/easydev-org/easybus-sdk/protocol"
)

// monitoredServer boots a nats-server with a monitoring port so tests can read
// JetStream's consumer count (the leak signal). Skips when unavailable.
type monitoredServer struct {
	url     string
	monPort int
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func startMonitored(t *testing.T) monitoredServer {
	t.Helper()
	bin := os.Getenv("ABC_NATS_SERVER_BIN")
	if bin == "" {
		bin = "nats-server"
	}
	cp, mp := freePort(t), freePort(t)
	cmd := exec.Command(bin, "-js", "-sd", t.TempDir(), "-p", fmt.Sprint(cp), "-m", fmt.Sprint(mp))
	if err := cmd.Start(); err != nil {
		t.Skipf("nats-server unavailable: %v", err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	for i := 0; i < 50; i++ {
		if consumersAt(t, mp) >= 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	return monitoredServer{url: fmt.Sprintf("nats://127.0.0.1:%d", cp), monPort: mp}
}

func consumersAt(t *testing.T, monPort int) int {
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/jsz", monPort))
	if err != nil {
		return -1
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var v struct {
		Consumers int `json:"consumers"`
	}
	if json.Unmarshal(raw, &v) != nil {
		return -1
	}
	return v.Consumers
}

// TestNoConsumerLeakAcrossSessionSubscriptions guards against a JetStream
// consumer leak: the per-turn / per-WatchSession subscriptions must be plain
// core subscriptions, and canceling them must not leave consumers behind.
func TestNoConsumerLeakAcrossSessionSubscriptions(t *testing.T) {
	srv := startMonitored(t)
	b, err := Connect(srv.url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	ctx := context.Background()
	_ = b.EnsurePresenceBucket(ctx)
	time.Sleep(300 * time.Millisecond)
	base := consumersAt(t, srv.monPort)

	for i := 0; i < 30; i++ {
		if _, stop, err := b.WatchInterrupts(ctx, "t1", "s1"); err == nil {
			stop()
		}
		if _, cancel, err := b.SubscribeSessionEvents(ctx, "t1", "s1", 0); err == nil {
			cancel()
		}
	}
	time.Sleep(500 * time.Millisecond)
	if after := consumersAt(t, srv.monPort); after > base {
		t.Errorf("consumer leak: %d -> %d", base, after)
	}
}

// TestMailboxConsumerDoesNotAccumulate guards the durable mailbox consumer: it
// is created ONCE per replica; repeated consume/stop cycles must not pile up.
func TestMailboxConsumerDoesNotAccumulate(t *testing.T) {
	srv := startMonitored(t)
	b, err := Connect(srv.url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	ctx := context.Background()
	_ = b.EnsurePresenceBucket(ctx)
	time.Sleep(300 * time.Millisecond)
	base := consumersAt(t, srv.monPort)

	for i := 0; i < 5; i++ {
		stop, err := b.ConsumeMailbox(ctx, func(MailboxMessage) error { return nil })
		if err != nil {
			t.Fatalf("consume %d: %v", i, err)
		}
		stop()
	}
	time.Sleep(500 * time.Millisecond)
	after := consumersAt(t, srv.monPort)
	// At most the single durable consumer remains; never one per cycle.
	if after > base+1 {
		t.Errorf("durable consumer leak: %d -> %d", base, after)
	}
}

// TestPresenceBucketAndDiscover guards the presence workaround: the bucket must
// be created (a sub-100ms TTL silently fails) and Discover must NOT panic.
func TestPresenceBucketAndDiscover(t *testing.T) {
	srv := startMonitored(t)
	b, err := Connect(srv.url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	if err := b.EnsurePresenceBucket(context.Background()); err != nil {
		t.Fatalf("EnsurePresenceBucket: %v", err)
	}
	// Discover drives ensurePresence -> KvWatch(presence bucket); it must not
	// panic (the abc-protocol-go Once bug) now that the bucket exists.
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Discover panicked: %v", r)
		}
	}()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = b.Agent().Discover(context.Background(), 100)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Discover did not return")
	}
	_ = protocol.ChSessionEvents
}
