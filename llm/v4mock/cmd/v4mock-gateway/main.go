// Command v4mock-gateway runs the deterministic v4 mock gateway as a standalone
// HTTP server, so an EXTERNAL client (e.g. the TS agent) can be pointed at it
// for cross-implementation equivalence checks.
package main

import (
	"log"
	"net/http"
	"os"

	"github.com/easydev-org/easyagent/llm/v4mock"
)

func main() {
	addr := os.Getenv("MOCK_ADDR")
	if addr == "" {
		addr = ":19000"
	}
	// v4mock.New binds an httptest server; extract its handler by using a plain
	// mux directly instead.
	mux := v4mock.Handler()
	log.Printf("v4 mock gateway listening on %s (mount at /v4/ai)", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}
