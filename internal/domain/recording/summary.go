package recording

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// Summary 值对象（详设 §2.1/§9）：summary 为非空字符串，key_points/todos 为字符串数组，
// 元素非空字符串，空数组允许。
type Summary struct {
	Summary   string
	KeyPoints []string
	Todos     []string
}

// ParseSummary 严格校验并解析 LLM 结构化输出（详设 §9）：拒绝顶层 null、缺字段、
// 字段为 null、类型错误、额外字段、元素空串、Markdown 围栏与混入文本、多个 JSON 值；允许空数组。
// 缺字段与合法空数组可区分：前者报错并指明字段名，后者解析成功。
func ParseSummary(data []byte) (Summary, error) {
	var probe any
	if err := json.Unmarshal(data, &probe); err != nil {
		return Summary{}, fmt.Errorf("%w: %s", ErrSummaryInvalid, err)
	}
	if probe == nil {
		return Summary{}, fmt.Errorf("%w: 顶层为 null", ErrSummaryInvalid)
	}

	// 指针字段区分「字段缺失/为 null」与「存在」：空数组也是存在，合法。
	var raw struct {
		Summary   *string   `json:"summary"`
		KeyPoints *[]string `json:"key_points"`
		Todos     *[]string `json:"todos"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&raw); err != nil {
		return Summary{}, fmt.Errorf("%w: %s", ErrSummaryInvalid, err)
	}
	switch {
	case raw.Summary == nil:
		return Summary{}, invalidSummaryField("summary")
	case raw.KeyPoints == nil:
		return Summary{}, invalidSummaryField("key_points")
	case raw.Todos == nil:
		return Summary{}, invalidSummaryField("todos")
	}
	if *raw.Summary == "" {
		return Summary{}, fmt.Errorf("%w: summary 不能为空字符串", ErrSummaryInvalid)
	}
	if err := checkNonEmptyElements("key_points", *raw.KeyPoints); err != nil {
		return Summary{}, err
	}
	if err := checkNonEmptyElements("todos", *raw.Todos); err != nil {
		return Summary{}, err
	}
	return Summary{Summary: *raw.Summary, KeyPoints: *raw.KeyPoints, Todos: *raw.Todos}, nil
}

func invalidSummaryField(field string) error {
	return fmt.Errorf("%w: 缺少字段 %s（或为 null）", ErrSummaryInvalid, field)
}

func checkNonEmptyElements(field string, items []string) error {
	for _, s := range items {
		if s == "" {
			return fmt.Errorf("%w: %s 含空字符串元素", ErrSummaryInvalid, field)
		}
	}
	return nil
}
