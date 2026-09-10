package unit

import (
	"errors"
	"strings"
	"testing"

	"recording-transcription/internal/domain/recording"
)

// TestUT04_ValidSummary —— 测试依据：测试设计 UT-04；设计依据：详设 §9。
// 非空 summary + 空 key_points/todos 通过；非空数组同样合法。
func TestUT04_ValidSummary(t *testing.T) {
	s, err := recording.ParseSummary([]byte(`{"summary":"会议讨论了上线计划","key_points":[],"todos":[]}`))
	if err != nil {
		t.Fatalf("合法 Summary 被拒绝: %v", err)
	}
	if s.Summary != "会议讨论了上线计划" {
		t.Errorf("Summary = %q", s.Summary)
	}
	if len(s.KeyPoints) != 0 || len(s.Todos) != 0 {
		t.Errorf("KeyPoints/Todos = %d/%d, want 0/0（空数组合法）", len(s.KeyPoints), len(s.Todos))
	}

	s2, err := recording.ParseSummary([]byte(`{"summary":"s","key_points":["要点一"],"todos":["待办一"]}`))
	if err != nil {
		t.Fatalf("非空数组 Summary 被拒绝: %v", err)
	}
	if len(s2.KeyPoints) != 1 || s2.KeyPoints[0] != "要点一" || len(s2.Todos) != 1 || s2.Todos[0] != "待办一" {
		t.Errorf("KeyPoints=%v Todos=%v", s2.KeyPoints, s2.Todos)
	}
}

// TestUT05_InvalidSummary —— 测试依据：测试设计 UT-05；设计依据：详设 §9。
// 缺字段、null、额外字段、元素空串、非字符串、Markdown 围栏、多个 JSON 值全部拒绝；
// 且「缺字段」与「合法空数组」可区分：前者报错并指明字段名，后者通过。
func TestUT05_InvalidSummary(t *testing.T) {
	cases := []struct {
		name      string
		payload   string
		wantField string // 非空时断言错误消息指明该字段（可区分性）
	}{
		{"顶层 null", `null`, ""},
		{"空载荷", ``, ""},
		{"缺字段 key_points", `{"summary":"s","todos":[]}`, "key_points"},
		{"字段为 null", `{"summary":"s","key_points":null,"todos":[]}`, "key_points"},
		{"额外字段", `{"summary":"s","key_points":[],"todos":[],"extra":true}`, ""},
		{"summary 为空串", `{"summary":"","key_points":[],"todos":[]}`, "summary"},
		{"元素为空串", `{"summary":"s","key_points":["a",""],"todos":[]}`, "key_points"},
		{"元素非字符串", `{"summary":"s","key_points":[1],"todos":[]}`, ""},
		{"字段类型错误", `{"summary":1,"key_points":[],"todos":[]}`, ""},
		{"Markdown 围栏", "```json\n{\"summary\":\"s\",\"key_points\":[],\"todos\":[]}\n```", ""},
		{"混入前置文本", `好的，以下是结果：{"summary":"s","key_points":[],"todos":[]}`, ""},
		{"多个 JSON 值", `{"summary":"s","key_points":[],"todos":[]} {"summary":"s","key_points":[],"todos":[]}`, ""},
	}
	for _, tc := range cases {
		_, err := recording.ParseSummary([]byte(tc.payload))
		if !errors.Is(err, recording.ErrSummaryInvalid) {
			t.Errorf("%s: 错误 = %v, want ErrSummaryInvalid", tc.name, err)
			continue
		}
		if tc.wantField != "" && !strings.Contains(err.Error(), tc.wantField) {
			t.Errorf("%s: 错误 %v 未指明字段 %s（缺字段须可与合法空数组区分）", tc.name, err, tc.wantField)
		}
	}

	// 可区分性的另一侧：同一字段写成合法空数组必须通过
	if _, err := recording.ParseSummary([]byte(`{"summary":"s","key_points":[],"todos":[]}`)); err != nil {
		t.Errorf("合法空数组被误拒: %v", err)
	}
}
