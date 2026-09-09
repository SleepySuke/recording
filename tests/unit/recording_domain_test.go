package unit

import (
	"errors"
	"strings"
	"testing"

	"recording-transcription/internal/domain/recording"
)

// TestUT06_ExtensionParsing —— 测试依据：测试设计 UT-06；设计依据：详设 §5.1。
// 最后一个点后缀转小写 + 白名单 wav/mp3/m4a/aac；无后缀、仅点号、非白名单一律拒绝。
func TestUT06_ExtensionParsing(t *testing.T) {
	valid := map[string]string{
		"A.WAV":     "wav", // 后缀转小写
		"a.wav.mp3": "mp3", // 取最后一个点后缀
		"rec.m4a":   "m4a",
		"会议.aac":    "aac",
		"a.Mp3":     "mp3",
	}
	for in, want := range valid {
		got, err := recording.ParseExtension(in)
		if err != nil {
			t.Errorf("ParseExtension(%q) 错误 = %v, want 通过", in, err)
			continue
		}
		if got != want {
			t.Errorf("ParseExtension(%q) = %q, want %q", in, got, want)
		}
	}

	invalid := []string{
		"a.mp3.exe", // 按最后后缀 exe 拒绝，不猜测意图
		"noextension",
		".wav", // 仅点号、空前缀
		"rec.txt",
		"a.", // 空后缀
	}
	for _, in := range invalid {
		got, err := recording.ParseExtension(in)
		if !errors.Is(err, recording.ErrUnsupportedExtension) {
			t.Errorf("ParseExtension(%q) 错误 = %v, want ErrUnsupportedExtension", in, err)
		}
		if got != "" {
			t.Errorf("ParseExtension(%q) = %q, want 空串", in, got)
		}
	}
}

// TestUT07_FilenameSanitize —— 测试依据：测试设计 UT-07；设计依据：详设 §5.1。
// 超 255 字节按 rune 边界截断、控制字符去除、非法 UTF-8 替换为替换符。
func TestUT07_FilenameSanitize(t *testing.T) {
	if got := recording.SanitizeFilename(strings.Repeat("a", 300)); len(got) != 255 {
		t.Errorf("300 字节输入净化后长度 = %d, want 255", len(got))
	}
	if got := recording.SanitizeFilename(strings.Repeat("a", 255)); got != strings.Repeat("a", 255) {
		t.Errorf("恰好 255 字节的输入不应被改动, got %q", got)
	}

	// 多字节字符按 rune 边界截断：252 字节 ASCII + “世界”（各 3 字节）→ 252+3=255，“界”被截断
	got := recording.SanitizeFilename(strings.Repeat("a", 252) + "世界")
	if len(got) != 255 || !strings.HasSuffix(got, "世") {
		t.Errorf("rune 边界截断结果 = %q (len=%d), want 252×a+世 (len=255)", got, len(got))
	}

	// 控制字符去除（含 DEL 0x7f）
	if got := recording.SanitizeFilename("re\x00co\x07rd\x1fing.wav"); got != "recording.wav" {
		t.Errorf("控制字符未去除: %q", got)
	}
	if got := recording.SanitizeFilename("a\x7fb"); got != "ab" {
		t.Errorf("DEL 0x7f 未去除: %q", got)
	}

	// 非法 UTF-8 字节替换为替换符 U+FFFD
	if got := recording.SanitizeFilename("bad\xffname.wav"); got != "bad�name.wav" {
		t.Errorf("非法 UTF-8 未替换: %q", got)
	}
}
