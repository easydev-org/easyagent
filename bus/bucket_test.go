package bus

import (
	"testing"

	"github.com/easydev-org/easybus-sdk/protocol"
)

// TestConfigBucketIsTheOneExtensionsWatch pins a cross-process contract that was
// silently broken.

// Extensions read their configuration from protocol.ConfigKVBucket and parse
// `t.<tenant>.<extId>.<name>`. The agent wrote to its own bucket
// ("easyagent-config"), so the two never met: the console saved a value, the agent
// stored it where nothing was looking, and every extension kept answering "not
// configured" — most visibly after a restart, when no in-memory copy remained.
//
// The bug was invisible from either side alone: saving worked, reading back
// through the agent worked, and the extension simply disagreed.
func TestConfigBucketIsTheOneExtensionsWatch(t *testing.T) {
	if ConfigBucket != protocol.ConfigKVBucket {
		t.Fatalf("ConfigBucket = %q, but extensions watch %q — no extension would ever receive a config snapshot",
			ConfigBucket, protocol.ConfigKVBucket)
	}
}
