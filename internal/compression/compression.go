package compression

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"fmt"
	"io"

	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
)

// CompressionType 压缩类型
type CompressionType int8

const (
	None CompressionType = iota
	Gzip
	Zlib
	Snappy
	Zstd
)

// String 返回压缩类型的字符串表示
func (c CompressionType) String() string {
	switch c {
	case None:
		return "none"
	case Gzip:
		return "gzip"
	case Zlib:
		return "zlib"
	case Snappy:
		return "snappy"
	case Zstd:
		return "zstd"
	default:
		return "unknown"
	}
}

// Compressor 压缩器接口
type Compressor interface {
	Compress(data []byte) ([]byte, error)
	Decompress(data []byte) ([]byte, error)
	Type() CompressionType
}

// NoCompression 无压缩
type NoCompression struct{}

func (n *NoCompression) Compress(data []byte) ([]byte, error) {
	return data, nil
}

func (n *NoCompression) Decompress(data []byte) ([]byte, error) {
	return data, nil
}

func (n *NoCompression) Type() CompressionType {
	return None
}

// GzipCompression Gzip压缩
type GzipCompression struct{}

func (g *GzipCompression) Compress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	writer := gzip.NewWriter(&buf)

	if _, err := writer.Write(data); err != nil {
		return nil, fmt.Errorf("gzip compress failed: %v", err)
	}

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("gzip writer close failed: %v", err)
	}

	return buf.Bytes(), nil
}

func (g *GzipCompression) Decompress(data []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("gzip reader create failed: %v", err)
	}
	defer reader.Close()

	result, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("gzip decompress failed: %v", err)
	}

	return result, nil
}

func (g *GzipCompression) Type() CompressionType {
	return Gzip
}

// ZlibCompression Zlib压缩
type ZlibCompression struct{}

func (z *ZlibCompression) Compress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	writer := zlib.NewWriter(&buf)

	if _, err := writer.Write(data); err != nil {
		return nil, fmt.Errorf("zlib compress failed: %v", err)
	}

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("zlib writer close failed: %v", err)
	}

	return buf.Bytes(), nil
}

func (z *ZlibCompression) Decompress(data []byte) ([]byte, error) {
	reader, err := zlib.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("zlib reader create failed: %v", err)
	}
	defer reader.Close()

	result, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("zlib decompress failed: %v", err)
	}

	return result, nil
}

func (z *ZlibCompression) Type() CompressionType {
	return Zlib
}

// SnappyCompression Snappy压缩
type SnappyCompression struct{}

func (s *SnappyCompression) Compress(data []byte) ([]byte, error) {
	return snappy.Encode(nil, data), nil
}

func (s *SnappyCompression) Decompress(data []byte) ([]byte, error) {
	result, err := snappy.Decode(nil, data)
	if err != nil {
		return nil, fmt.Errorf("snappy decompress failed: %v", err)
	}
	return result, nil
}

func (s *SnappyCompression) Type() CompressionType {
	return Snappy
}

// ZstdCompression Zstd压缩
type ZstdCompression struct {
	encoder *zstd.Encoder
	decoder *zstd.Decoder
}

func NewZstdCompression() (*ZstdCompression, error) {
	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		return nil, fmt.Errorf("create zstd encoder failed: %v", err)
	}

	decoder, err := zstd.NewReader(nil)
	if err != nil {
		return nil, fmt.Errorf("create zstd decoder failed: %v", err)
	}

	return &ZstdCompression{
		encoder: encoder,
		decoder: decoder,
	}, nil
}

func (z *ZstdCompression) Compress(data []byte) ([]byte, error) {
	return z.encoder.EncodeAll(data, nil), nil
}

func (z *ZstdCompression) Decompress(data []byte) ([]byte, error) {
	result, err := z.decoder.DecodeAll(data, nil)
	if err != nil {
		return nil, fmt.Errorf("zstd decompress failed: %v", err)
	}
	return result, nil
}

func (z *ZstdCompression) Type() CompressionType {
	return Zstd
}

func (z *ZstdCompression) Close() {
	if z.encoder != nil {
		z.encoder.Close()
	}
	if z.decoder != nil {
		z.decoder.Close()
	}
}

// GetCompressor 根据类型获取压缩器
func GetCompressor(compressionType CompressionType) (Compressor, error) {
	switch compressionType {
	case None:
		return &NoCompression{}, nil
	case Gzip:
		return &GzipCompression{}, nil
	case Zlib:
		return &ZlibCompression{}, nil
	case Snappy:
		return &SnappyCompression{}, nil
	case Zstd:
		return NewZstdCompression()
	default:
		return nil, fmt.Errorf("unsupported compression type: %d", compressionType)
	}
}

// CompressMessage 压缩消息，返回带压缩头的数据
func CompressMessage(data []byte, compressionType CompressionType) ([]byte, error) {
	compressor, err := GetCompressor(compressionType)
	if err != nil {
		return nil, err
	}

	compressed, err := compressor.Compress(data)
	if err != nil {
		return nil, err
	}

	// 添加压缩类型头(1字节) + 原始长度(4字节) + 压缩数据
	result := make([]byte, 5+len(compressed))
	result[0] = byte(compressionType)

	// 写入原始数据长度
	originalLen := uint32(len(data))
	result[1] = byte(originalLen >> 24)
	result[2] = byte(originalLen >> 16)
	result[3] = byte(originalLen >> 8)
	result[4] = byte(originalLen)

	copy(result[5:], compressed)

	return result, nil
}

// DecompressMessage 解压缩消息
func DecompressMessage(data []byte) ([]byte, error) {
	if len(data) < 5 {
		return nil, fmt.Errorf("invalid compressed data: too short")
	}

	compressionType := CompressionType(data[0])

	// 读取原始长度
	originalLen := uint32(data[1])<<24 | uint32(data[2])<<16 | uint32(data[3])<<8 | uint32(data[4])

	compressor, err := GetCompressor(compressionType)
	if err != nil {
		return nil, err
	}

	decompressed, err := compressor.Decompress(data[5:])
	if err != nil {
		return nil, err
	}

	// 验证长度
	if uint32(len(decompressed)) != originalLen {
		return nil, fmt.Errorf("decompressed data length mismatch: expected %d, got %d",
			originalLen, len(decompressed))
	}

	return decompressed, nil
}

// CalculateCompressionRatio 计算压缩比
func CalculateCompressionRatio(originalSize, compressedSize int) float64 {
	if originalSize == 0 {
		return 0
	}
	return float64(compressedSize) / float64(originalSize)
}
