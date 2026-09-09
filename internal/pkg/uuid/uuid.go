// Package uuid 提供应用侧 UUID v4 生成（详设：ID 为应用生成的 UUID，CHAR(36)）。
// 标准库实现，不引入额外依赖。
package uuid

import "crypto/rand"

// New 返回 UUID v4 字符串（36 字符，含连字符）。
func New() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("uuid: crypto/rand 不可用: " + err.Error())
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	const hex = "0123456789abcdef"
	buf := make([]byte, 0, 36)
	for i, v := range b {
		if i == 4 || i == 6 || i == 8 || i == 10 {
			buf = append(buf, '-')
		}
		buf = append(buf, hex[v>>4], hex[v&0x0f])
	}
	return string(buf)
}

// IsValid 报告 s 是否为规范 36 字符 UUID 形态（8-4-4-4-12 十六进制，大小写均可）。
// 用于路径参数校验：形态非法属参数错误（400/10001，详设 §8.3），而非资源不存在。
func IsValid(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return false
			}
		default:
			isHex := (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
			if !isHex {
				return false
			}
		}
	}
	return true
}
