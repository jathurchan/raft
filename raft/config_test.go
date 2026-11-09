package raft

import "testing"

func TestRaftFeatureFlags_WithExplicitFlags(t *testing.T) {
	flags := FeatureFlags{EnableReadIndex: true, EnableLeaderLease: false}

	if got := flags.WithExplicitFlags(); got != flags {
		t.Errorf("WithExplicitFlags() = %+v, want unchanged %+v", got, flags)
	}
}
