package recording

import (
	"fmt"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

// maxFilenameBytes original_filename 的存储长度上限（详设 §5.1）。
const maxFilenameBytes = 255

// supportedExtensions 上传扩展名白名单（详设 §5.1）。
var supportedExtensions = map[string]bool{
	"wav": true,
	"mp3": true,
	"m4a": true,
	"aac": true,
}

// ParseExtension 取文件名最后一个点后的后缀并转小写，仅接受白名单 wav/mp3/m4a/aac；
// 无后缀、仅点号（空前缀）与非白名单后缀一律拒绝，不猜测意图（详设 §5.1）。
func ParseExtension(filename string) (string, error) {
	dot := strings.LastIndex(filename, ".")
	if dot <= 0 { // 无点，或点在首位（如 ".wav"，空前缀）
		return "", ErrUnsupportedExtension
	}
	ext := strings.ToLower(filename[dot+1:])
	if !supportedExtensions[ext] {
		return "", fmt.Errorf("%w: %q", ErrUnsupportedExtension, ext)
	}
	return ext, nil
}

// SanitizeFilename 净化 original_filename（仅作展示元数据，路径永不使用，详设 §5.1）：
// 非法 UTF-8 字节替换为替换符 U+FFFD、去除控制字符、最长 255 字节（按 rune 边界截断）。
func SanitizeFilename(name string) string {
	valid := strings.ToValidUTF8(name, "�")
	var b strings.Builder
	for _, r := range valid {
		if unicode.IsControl(r) {
			continue
		}
		if b.Len()+utf8.RuneLen(r) > maxFilenameBytes {
			break
		}
		b.WriteRune(r)
	}
	return b.String()
}

// Recording 聚合根（详设 §2.1）：负责文件元数据与删除生命周期。
type Recording struct {
	ID               string
	OriginalFilename string
	StoragePath      string
	Extension        string
	SizeBytes        int64
	ContentHash      string
	DeletingAt       *time.Time
	CreatedAt        time.Time
	UpdatedAt        time.Time
}
