package metrics

import (
	"github.com/deciduosity/birch"
	"github.com/shirou/gopsutil/process"
)

func marshalMemExtra(*process.MemoryInfoExStat) *birch.Element { return nil }
