package quota

import "net"

// netListen 抽象监听地址，便于测试与潜在的传输替换。
func netListen(addr string) (net.Listener, error) { return net.Listen("tcp", addr) }
