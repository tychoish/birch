package metrics

import (
	"github.com/cdr/grip/message"
	"github.com/deciduosity/birch"
	"github.com/shirou/gopsutil/net"
)

func marshalNetStat(netstat *net.IOCountersStat) *birch.Document {
	return birch.DC.Elements(
		birch.EC.String("name", netstat.Name),
		birch.EC.Int64("bytesSent", int64(netstat.BytesSent)),
		birch.EC.Int64("bytesRecv", int64(netstat.BytesRecv)),
		birch.EC.Int64("packetsSent", int64(netstat.PacketsSent)),
		birch.EC.Int64("packetsRecv", int64(netstat.PacketsRecv)),
		birch.EC.Int64("errin", int64(netstat.Errin)),
		birch.EC.Int64("errout", int64(netstat.Errout)),
		birch.EC.Int64("dropin", int64(netstat.Dropin)),
		birch.EC.Int64("dropout", int64(netstat.Dropout)),
		birch.EC.Int64("fifoin", int64(netstat.Fifoin)),
		birch.EC.Int64("fifoout", int64(netstat.Fifoout)))
}

func marshalCPU(cpu *message.StatCPUTimes) *birch.Document {
	return birch.DC.Elements(
		birch.EC.Int64("user", cpu.User),
		birch.EC.Int64("system", cpu.System),
		birch.EC.Int64("idle", cpu.Idle),
		birch.EC.Int64("nice", cpu.Nice),
		birch.EC.Int64("iowait", cpu.Iowait),
		birch.EC.Int64("irq", cpu.Irq),
		birch.EC.Int64("softirq", cpu.Softirq),
		birch.EC.Int64("steal", cpu.Steal),
		birch.EC.Int64("guest", cpu.Guest),
		birch.EC.Int64("guestNice", cpu.GuestNice))
}

func (r *Runtime) MarshalDocument() (*birch.Document, error) {
	doc := birch.DC.Elements(
		birch.EC.Int("id", r.ID),
		birch.EC.Time("ts", r.Timestamp),
		birch.EC.Int("pid", r.PID))

	if r.Golang != nil {
		doc.Append(birch.EC.SubDocumentFromElements("golang",
			birch.EC.Int64("memory.objects.heap", int64(r.Golang.HeapObjects)),
			birch.EC.Int64("memory.summary.alloc", int64(r.Golang.Alloc)),
			birch.EC.Int64("memory.summary.system", int64(r.Golang.HeapSystem)),
			birch.EC.Int64("memory.heap.idle", int64(r.Golang.HeapIdle)),
			birch.EC.Int64("memory.heap.used", int64(r.Golang.HeapInUse)),
			birch.EC.Int64("memory.counters.mallocs", r.Golang.Mallocs),
			birch.EC.Int64("memory.counters.frees", r.Golang.Frees),
			birch.EC.Int64("gc.rate", r.Golang.GC),
			birch.EC.Duration("gc.pause.duration.last", r.Golang.GCPause),
			birch.EC.Duration("gc.pause.duration.latency", r.Golang.GCLatency),
			birch.EC.Int64("goroutines.total", r.Golang.Goroutines),
			birch.EC.Int64("cgo.calls", r.Golang.CgoCalls)))
	}

	if r.System != nil {
		sys := birch.DC.Elements(
			birch.EC.Int("num_cpu", r.System.NumCPU),
			birch.EC.Double("cpu_percent", r.System.CPUPercent),
			birch.EC.SubDocument("cpu", marshalCPU(&r.System.CPU)),
			birch.EC.SubDocumentFromElements("vmstat",
				birch.EC.Int64("total", int64(r.System.VMStat.Total)),
				birch.EC.Int64("available", int64(r.System.VMStat.Available)),
				birch.EC.Int64("used", int64(r.System.VMStat.Used)),
				birch.EC.Int64("usedPercent", int64(r.System.VMStat.UsedPercent)),
				birch.EC.Int64("free", int64(r.System.VMStat.Free)),
				birch.EC.Int64("active", int64(r.System.VMStat.Active)),
				birch.EC.Int64("inactive", int64(r.System.VMStat.Inactive)),
				birch.EC.Int64("wired", int64(r.System.VMStat.Wired)),
				birch.EC.Int64("laundry", int64(r.System.VMStat.Laundry)),
				birch.EC.Int64("buffers", int64(r.System.VMStat.Buffers)),
				birch.EC.Int64("cached", int64(r.System.VMStat.Cached)),
				birch.EC.Int64("writeback", int64(r.System.VMStat.Writeback)),
				birch.EC.Int64("dirty", int64(r.System.VMStat.Dirty)),
				birch.EC.Int64("writebacktmp", int64(r.System.VMStat.WritebackTmp)),
				birch.EC.Int64("shared", int64(r.System.VMStat.Shared)),
				birch.EC.Int64("slab", int64(r.System.VMStat.Slab)),
				birch.EC.Int64("sreclaimable", int64(r.System.VMStat.SReclaimable)),
				birch.EC.Int64("sunreclaim", int64(r.System.VMStat.SUnreclaim)),
				birch.EC.Int64("pagetables", int64(r.System.VMStat.PageTables)),
				birch.EC.Int64("swapcached", int64(r.System.VMStat.SwapCached)),
				birch.EC.Int64("commitlimit", int64(r.System.VMStat.CommitLimit)),
				birch.EC.Int64("commitedas", int64(r.System.VMStat.CommittedAS)),
				birch.EC.Int64("hightotal", int64(r.System.VMStat.HighTotal)),
				birch.EC.Int64("highfree", int64(r.System.VMStat.HighFree)),
				birch.EC.Int64("lowtotal", int64(r.System.VMStat.LowTotal)),
				birch.EC.Int64("lowfree", int64(r.System.VMStat.LowFree)),
				birch.EC.Int64("swaptotal", int64(r.System.VMStat.SwapTotal)),
				birch.EC.Int64("swapfree", int64(r.System.VMStat.SwapFree)),
				birch.EC.Int64("mapped", int64(r.System.VMStat.Mapped)),
				birch.EC.Int64("vmalloctotal", int64(r.System.VMStat.VMallocTotal)),
				birch.EC.Int64("vmallocused", int64(r.System.VMStat.VMallocUsed)),
				birch.EC.Int64("vmallocchunk", int64(r.System.VMStat.VMallocChunk)),
				birch.EC.Int64("hugepagestotal", int64(r.System.VMStat.HugePagesTotal)),
				birch.EC.Int64("hugepagesfree", int64(r.System.VMStat.HugePagesFree)),
				birch.EC.Int64("hugepagessize", int64(r.System.VMStat.HugePageSize))),
			birch.EC.SubDocument("netstat", marshalNetStat(&r.System.NetStat)))
		{
			ua := birch.MakeArray(len(r.System.Usage))
			for _, usage := range r.System.Usage {
				ua.Append(birch.VC.DocumentFromElements(
					birch.EC.String("path", usage.Path),
					birch.EC.String("fstype", usage.Fstype),
					birch.EC.Int64("total", int64(usage.Total)),
					birch.EC.Int64("free", int64(usage.Free)),
					birch.EC.Int64("used", int64(usage.Used)),
					birch.EC.Double("usedPercent", usage.UsedPercent),
					birch.EC.Int64("inodesTotal", int64(usage.InodesTotal)),
					birch.EC.Int64("inodesFree", int64(usage.InodesFree)),
					birch.EC.Double("inodesUsedPercent", usage.InodesUsedPercent)))
			}
			sys.Append(birch.EC.Array("usage", ua))
		}
		{
			ioa := birch.MakeArray(len(r.System.IOStat))
			for _, iostat := range r.System.IOStat {
				ioa.Append(birch.VC.DocumentFromElements(
					birch.EC.String("name", iostat.Name),
					birch.EC.String("serialNumber", iostat.SerialNumber),
					birch.EC.String("label", iostat.Label),
					birch.EC.Int64("readCount", int64(iostat.ReadCount)),
					birch.EC.Int64("mergedReadCount", int64(iostat.MergedReadCount)),
					birch.EC.Int64("writeCount", int64(iostat.WriteCount)),
					birch.EC.Int64("mergedWriteCount", int64(iostat.MergedWriteCount)),
					birch.EC.Int64("readBytes", int64(iostat.ReadBytes)),
					birch.EC.Int64("writeBytes", int64(iostat.WriteBytes)),
					birch.EC.Int64("readTime", int64(iostat.ReadTime)),
					birch.EC.Int64("writeTime", int64(iostat.WriteTime)),
					birch.EC.Int64("iopsInProgress", int64(iostat.IopsInProgress)),
					birch.EC.Int64("ioTime", int64(iostat.IoTime)),
					birch.EC.Int64("weightedIO", int64(iostat.WeightedIO)),
				))
			}
			sys.Append(birch.EC.Array("iostat", ioa))
		}
		{
			parts := birch.MakeArray(len(r.System.Partitions))
			for _, part := range r.System.Partitions {
				parts.Append(birch.VC.DocumentFromElements(
					birch.EC.String("device", part.Device),
					birch.EC.String("mountpoint", part.Mountpoint),
					birch.EC.String("fstype", part.Fstype),
					birch.EC.String("opts", part.Opts),
				))
			}
			sys.Append(birch.EC.Array("partitions", parts))
		}

		doc.Append(birch.EC.SubDocument("system", sys))
	}

	if r.Process != nil {
		proc := birch.DC.Elements(
			birch.EC.Int32("pid", r.Process.Pid),
			birch.EC.Int32("parentPid", r.Process.Parent),
			birch.EC.Int("threads", r.Process.Threads),
			birch.EC.String("command", r.Process.Command),
			birch.EC.SubDocument("cpu", marshalCPU(&r.Process.CPU)),
			birch.EC.SubDocumentFromElements("io",
				birch.EC.Int64("readCount", int64(r.Process.IoStat.ReadCount)),
				birch.EC.Int64("writeCount", int64(r.Process.IoStat.WriteCount)),
				birch.EC.Int64("readBytes", int64(r.Process.IoStat.ReadBytes)),
				birch.EC.Int64("writeBytes", int64(r.Process.IoStat.WriteBytes))),
			birch.EC.SubDocumentFromElements("mem",
				birch.EC.Int64("rss", int64(r.Process.Memory.RSS)),
				birch.EC.Int64("vms", int64(r.Process.Memory.VMS)),
				birch.EC.Int64("hwm", int64(r.Process.Memory.HWM)),
				birch.EC.Int64("data", int64(r.Process.Memory.Data)),
				birch.EC.Int64("stack", int64(r.Process.Memory.Stack)),
				birch.EC.Int64("locked", int64(r.Process.Memory.Locked)),
				birch.EC.Int64("swap", int64(r.Process.Memory.Swap))),
		)

		proc.AppendOmitEmpty(marshalMemExtra(&r.Process.MemoryPlatform))
		na := birch.MakeArray(len(r.Process.NetStat))
		for _, netstat := range r.Process.NetStat {
			na.Append(birch.VC.Document(marshalNetStat(&netstat)))
		}
		proc.Append(birch.EC.Array("net", na))
		doc.Append(birch.EC.SubDocument("process", proc))
	}

	return doc, nil
}
