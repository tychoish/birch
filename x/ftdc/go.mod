module github.com/tychoish/birch/x/ftdc

go 1.24

toolchain go1.24.0

require (
	github.com/fsnotify/fsnotify v1.6.0
	github.com/tychoish/birch v0.3.1
	github.com/tychoish/fun v0.14.0
	github.com/tychoish/grip/x/metrics v0.0.0-20230408192639-ef555fcdf0fd
)

require (
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/shirou/gopsutil v3.21.11+incompatible // indirect
	github.com/stretchr/testify v1.8.1 // indirect
	github.com/tklauser/go-sysconf v0.3.11 // indirect
	github.com/tklauser/numcpus v0.6.0 // indirect
	github.com/tychoish/grip v0.4.2 // indirect
	github.com/yusufpapurcu/wmi v1.2.2 // indirect
	golang.org/x/sys v0.4.0 // indirect
)

replace github.com/tychoish/birch => ../../../birch/
