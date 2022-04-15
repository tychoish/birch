package metrics

import (
	"github.com/shirou/gopsutil/process"
	"github.com/tychoish/birch"
)

func marshalMemExtra(*process.MemoryInfoExStat) *birch.Element { return nil }
