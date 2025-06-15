package main

import (
	"encoding/binary"
	"net"

	"go-queue/internal/protocol"
)

const (
	PRODUCE_REQUEST = 0 // 生产请求类型标识
	FETCH_REQUEST   = 1
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
		case FETCH_REQUEST:
			// TODO
			protocol.HandleFetchRequest(conn)
		case PRODUCE_REQUEST:
			protocol.HandleProduceRequest(conn)

			// protocol.handleFetchRequest(conn)
		}
	}
}
