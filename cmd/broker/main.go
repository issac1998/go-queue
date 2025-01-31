package main

import (
	"encoding/binary"
	"net"

	"example.com/m/v2/go-queue/internal/protocol"
)

const (
	PRODUCE_REQUEST        = 0x01    // 生产请求类型标识
	MaxMessageSize         = 1 << 20 // 单条消息最大1MB
	CompressionNone        = 0x00    // 无压缩
	CurrentProtocolVersion = 1       // 协议版本
)

func main() {
	ln, _ := net.Listen("tcp", ":9092")
	for {
		conn, _ := ln.Accept()
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	for {
		// 1. 读取请求头（包含请求类型和长度）
		var reqType int32
		binary.Read(conn, binary.BigEndian, &reqType)

		// 2. 根据请求类型分发处理
		switch reqType {
		case PRODUCE_REQUEST:
			protocol.HandleProduceRequest(conn)
		case FETCH_REQUEST:
			protocol.handleFetchRequest(conn)
		}
	}
}
