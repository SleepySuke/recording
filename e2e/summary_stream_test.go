package e2e

import (
	"bufio"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestE2E10_SummaryStream 经真实 TCP 验证 SSE：先发状态、完成时发真实落库摘要并关闭。
func TestE2E10_SummaryStream(t *testing.T) {
	h := newE2E(t, e2eOpts{MockASRDelay: 100 * time.Millisecond})
	up, code, err := h.postUpload("E10-stream.wav", contentOf(2048))
	if err != nil || code != http.StatusAccepted {
		t.Fatalf("上传 = (%d, %v), want 202", code, err)
	}
	req, err := http.NewRequest(http.MethodGet, h.BaseURL+"/v1/recordings/"+up.RecordingID+"/summary/stream", nil)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Timeout: 8 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("SSE 请求失败: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		t.Fatalf("SSE 响应 = %d %q", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	var events []string
	var summary resultBody
	s := bufio.NewScanner(resp.Body)
	for s.Scan() {
		line := s.Text()
		if strings.HasPrefix(line, "event:") {
			events = append(events, strings.TrimSpace(strings.TrimPrefix(line, "event:")))
			continue
		}
		if strings.HasPrefix(line, "data:") && len(events) > 0 && events[len(events)-1] == "summary" {
			if err := json.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(line, "data:"))), &summary); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := s.Err(); err != nil {
		t.Fatalf("读取 SSE 失败: %v", err)
	}
	if len(events) < 2 || events[0] != "status" || events[len(events)-1] != "summary" {
		t.Fatalf("SSE events = %v, want status ... summary", events)
	}
	if summary.Summary == "" {
		t.Fatal("SSE summary 为空")
	}
}
