package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"go-queue/internal/metadata"
	"go-queue/internal/storage"
	"io"
)

type FetchRequest struct {
	Topic     string
	Partition int32
	Offset    int64 // 起始消费偏移量
	MaxBytes  int32 // 最大拉取字节数
}

// FetchResponse 消费响应结构
type FetchResponse struct {
	Topic      string
	Partition  int32
	ErrorCode  int16    // 错误码 (0=成功)
	Messages   [][]byte // 实际消息内容
	NextOffset int64    // 下一条消息的偏移量
}

// --- 协议编解码 ---

// ReadFetchRequest 解码消费请求
func ReadFetchRequest(r io.Reader) (*FetchRequest, error) {
	req := &FetchRequest{}

	// 1. 读取协议版本
	var version int16
	if err := binary.Read(r, binary.BigEndian, &version); err != nil {
		return nil, fmt.Errorf("failed to read protocol version: %v", err)
	}
	if version != CurrentProtocolVersion {
		return nil, fmt.Errorf("unsupported protocol version: %d", version)
	}

	// 2. 读取Topic
	var topicLen int16
	if err := binary.Read(r, binary.BigEndian, &topicLen); err != nil {
		return nil, fmt.Errorf("failed to read topic length: %v", err)
	}
	topicBytes := make([]byte, topicLen)
	if _, err := io.ReadFull(r, topicBytes); err != nil {
		return nil, fmt.Errorf("failed to read topic: %v", err)
	}
	req.Topic = string(topicBytes)

	// 3. 读取Partition、Offset、MaxBytes
	if err := binary.Read(r, binary.BigEndian, &req.Partition); err != nil {
		return nil, fmt.Errorf("failed to read partition: %v", err)
	}
	if err := binary.Read(r, binary.BigEndian, &req.Offset); err != nil {
		return nil, fmt.Errorf("failed to read offset: %v", err)
	}
	if err := binary.Read(r, binary.BigEndian, &req.MaxBytes); err != nil {
		return nil, fmt.Errorf("failed to read max bytes: %v", err)
	}

	// 4. 验证MaxBytes限制
	if req.MaxBytes > MaxFetchBytesLimit {
		req.MaxBytes = MaxFetchBytesLimit
	} else if req.MaxBytes <= 0 {
		req.MaxBytes = DefaultMaxFetchBytes
	}

	return req, nil
}

// WriteFetchResponse 编码消费响应
func (res *FetchResponse) Write(w io.Writer) error {
	// 1. 写入固定头信息
	headerBuf := new(bytes.Buffer)
	binary.Write(headerBuf, binary.BigEndian, int16(len(res.Topic)))
	headerBuf.WriteString(res.Topic)
	binary.Write(headerBuf, binary.BigEndian, res.Partition)
	binary.Write(headerBuf, binary.BigEndian, res.ErrorCode)
	binary.Write(headerBuf, binary.BigEndian, res.NextOffset)

	// 2. 写入消息集合
	var messagesBuf bytes.Buffer
	for _, msg := range res.Messages {
		binary.Write(&messagesBuf, binary.BigEndian, int32(len(msg)))
		messagesBuf.Write(msg)
	}

	// 3. 计算总长度并写入
	totalLen := int32(headerBuf.Len() + messagesBuf.Len() + 4) // 4字节长度头
	if err := binary.Write(w, binary.BigEndian, totalLen); err != nil {
		return err
	}

	// 4. 写入头和消息体
	if _, err := w.Write(headerBuf.Bytes()); err != nil {
		return err
	}
	if _, err := w.Write(messagesBuf.Bytes()); err != nil {
		return err
	}

	return nil
}

// --- 核心消费逻辑 ---

// HandleFetchRequest 处理消费请求
func HandleFetchRequest(conn io.ReadWriteCloser) error {
	defer conn.Close()

	// 1. 解析请求
	req, err := ReadFetchRequest(conn)
	if err != nil {
		return sendFetchError(conn, ErrorInvalidRequest, err.Error())
	}

	// 2. 获取分区
	partition, err := metadata.GetPartition(req.Topic, req.Partition)
	if err != nil {
		return sendFetchError(conn, ErrorUnknownPartition,
			fmt.Sprintf("partition %d not found", req.Partition))
	}

	// 3. 获取分区读锁
	partition.Mu.RLock()
	defer partition.Mu.RUnlock()

	// 4. 定位到指定偏移量
	segment, startPos, err := findSegmentAndPosition(partition, req.Offset)
	if err != nil {
		return sendFetchError(conn, ErrorOffsetOutOfRange, err.Error())
	}

	// 5. 读取消息
	messages, nextOffset, err := readMessagesFromSegment(
		segment,
		startPos,
		int64(req.MaxBytes),
	)
	if err != nil {
		return sendFetchError(conn, ErrorFetchFailed, err.Error())
	}

	// 6. 构造响应
	response := &FetchResponse{
		Topic:      req.Topic,
		Partition:  req.Partition,
		ErrorCode:  ErrorNone,
		Messages:   messages,
		NextOffset: nextOffset,
	}
	return response.Write(conn)
}

// --- 辅助函数 ---

// findSegmentAndPosition 查找包含目标Offset的Segment和起始位置
func findSegmentAndPosition(p *metadata.Partition, offset int64) (*storage.Segment, int64, error) {
	// 二分查找定位到对应的Segment
	for _, seg := range p.Segments {
		if offset >= seg.BaseOffset && offset < seg.BaseOffset+seg.MaxBytes {
			// 在索引中查找精确位置
			pos, err := seg.FindPosition(offset)
			return seg, pos, err
		}
	}
	return nil, 0, errors.New("offset out of range")
}

// readMessagesFromSegment 从指定位置读取消息
func readMessagesFromSegment(
	seg *storage.Segment,
	startPos int64,
	maxBytes int64,
) ([][]byte, int64, error) {
	var messages [][]byte
	currentPos := startPos
	totalBytes := int64(0)

	// 遍历读取直到达到限制或文件末尾
	for totalBytes < maxBytes {
		// 1. 读取消息长度头
		var msgSize int32
		lenBuf := make([]byte, 4)
		if _, err := seg.ReadAt(currentPos, lenBuf); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, 0, err
		}
		msgSize = int32(binary.BigEndian.Uint32(lenBuf))

		currentPos += 4

		// 2. 检查消息大小合法性
		if msgSize <= 0 || int64(msgSize) > (maxBytes-totalBytes) {
			break
		}

		// 3. 读取消息内容
		msg := make([]byte, msgSize)
		if _, err := seg.ReadAt(currentPos, msg); err != nil {
			return nil, 0, err
		}
		messages = append(messages, msg)
		currentPos += int64(msgSize)
		totalBytes += int64(msgSize) + 4 // 包含长度头
	}

	return messages, seg.BaseOffset + currentPos, nil
}

// sendFetchError 发送错误响应
func sendFetchError(w io.Writer, code int16, message string) error {
	response := &FetchResponse{
		ErrorCode: code,
	}
	if err := response.Write(w); err != nil {
		return fmt.Errorf("failed to send error response: %v", err)
	}
	return fmt.Errorf("fetch error %d: %s", code, message)
}
