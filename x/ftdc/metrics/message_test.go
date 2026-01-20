package metrics

import (
	"testing"
)

func TestProcessTreeDoesNotHaveDuplicates(t *testing.T) {
	t.Parallel()

	procs := CollectProcessInfoWithChildren(1)
	seen := make(map[int32]struct{})

	for _, p := range procs {
		seen[p.Pid] = struct{}{}
	}

	if len(procs) != len(seen) {
		t.Error("elements should be equal")
	}
}
