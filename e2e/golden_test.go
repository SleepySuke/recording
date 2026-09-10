// golden 比对机制（测试设计 §5.1/§5.3，T06E）：预期先行手写 e2e/expected/<case>.json，
// 运行采集同形状结果到 e2e/actual/<case>.json（忽略清单以占位符掩码归一化实现），
// 解析后深度相等才通过；不一致输出字段级 diff 到 e2e/diff/<case>.txt 并保留 actual 现场。
package e2e

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"
)

// reSeed 掩码 transcript 中的种子数值："(seed=12345)" → "(seed=<seed>)"。
var reSeed = regexp.MustCompile(`(seed=)\d+`)

// normalizeGolden 采集后归一化（§5.1 忽略清单）：字符串值按规则掩码——
// 真实 ID（ids 映射）→ <task_id>/<recording_id> 等占位符（含内嵌出现，如 transcript）；
// seed=数值 → seed=<seed>；RFC3339 时间戳 → <ts>。结构与非字符串值原样保留。
func normalizeGolden(v any, ids map[string]string) any {
	switch x := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, val := range x {
			out[k] = normalizeGolden(val, ids)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, val := range x {
			out[i] = normalizeGolden(val, ids)
		}
		return out
	case string:
		return maskString(x, ids)
	default:
		return v
	}
}

// maskString 单个字符串值的掩码规则（整值命中优先于子串替换）。
func maskString(s string, ids map[string]string) string {
	if repl, ok := ids[s]; ok {
		return repl
	}
	for id, repl := range ids {
		if id != "" && strings.Contains(s, id) {
			s = strings.ReplaceAll(s, id, repl)
		}
	}
	s = reSeed.ReplaceAllString(s, "${1}<seed>")
	if _, err := time.Parse(time.RFC3339, s); err == nil {
		return "<ts>"
	}
	return s
}

// runGolden 采集 → 归一化 → 写 actual → 与 expected 深度比对。
// actual 为采集结构体（真实值）；ids 为该用例「真实 ID → 占位符」映射（无则传 nil）。
func runGolden(t *testing.T, caseName string, actual any, ids map[string]string) {
	t.Helper()
	raw, err := json.Marshal(actual)
	if err != nil {
		t.Fatalf("采集结果序列化失败: %v", err)
	}
	var tree any
	if err := json.Unmarshal(raw, &tree); err != nil {
		t.Fatalf("采集结果反序列化失败: %v", err)
	}
	tree = normalizeGolden(tree, ids)

	actData, err := marshalIndentPlain(tree)
	if err != nil {
		t.Fatalf("actual 格式化失败: %v", err)
	}
	if err := os.MkdirAll("e2e/actual", 0o755); err != nil {
		t.Fatalf("创建 e2e/actual 失败: %v", err)
	}
	if err := os.WriteFile("e2e/actual/"+caseName+".json", append(actData, '\n'), 0o644); err != nil {
		t.Fatalf("写入 actual 失败: %v", err)
	}

	expData, err := os.ReadFile("e2e/expected/" + caseName + ".json")
	if err != nil {
		t.Fatalf("读取 golden（e2e/expected/%s.json）失败: %v", caseName, err)
	}
	var exp any
	if err := json.Unmarshal(expData, &exp); err != nil {
		t.Fatalf("golden 不是合法 JSON: %v", err)
	}

	var lines []string
	diffGolden("$", exp, tree, &lines)
	if len(lines) == 0 {
		_ = os.Remove("e2e/diff/" + caseName + ".txt") // 清理上次失败残留，保持 diff/ 空
		return
	}
	if err := os.MkdirAll("e2e/diff", 0o755); err != nil {
		t.Fatalf("创建 e2e/diff 失败: %v", err)
	}
	report := fmt.Sprintf("case=%s（expected=e2e/expected/%s.json actual=e2e/actual/%s.json）\n%s\n",
		caseName, caseName, caseName, strings.Join(lines, "\n"))
	if err := os.WriteFile("e2e/diff/"+caseName+".txt", []byte(report), 0o644); err != nil {
		t.Fatalf("写 diff 失败: %v", err)
	}
	t.Errorf("golden 比对不一致：%d 处差异，字段级 diff 见 e2e/diff/%s.txt（actual 现场已保留）",
		len(lines), caseName)
}

// diffGolden 递归比对两棵解码后的 JSON 树，差异行追加到 out（路径/值对照）。
func diffGolden(path string, exp, act any, out *[]string) {
	switch e := exp.(type) {
	case map[string]any:
		a, ok := act.(map[string]any)
		if !ok {
			*out = append(*out, fmt.Sprintf("%s: 类型不一致 expected=object actual=%s", path, kindOf(act)))
			return
		}
		keys := make([]string, 0, len(e)+len(a))
		for k := range e {
			keys = append(keys, k)
		}
		for k := range a {
			if _, dup := e[k]; !dup {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		for _, k := range keys {
			ev, inE := e[k]
			av, inA := a[k]
			switch {
			case !inE:
				*out = append(*out, fmt.Sprintf("%s.%s: 仅在 actual 中存在 actual=%s", path, k, jsonOf(av)))
			case !inA:
				*out = append(*out, fmt.Sprintf("%s.%s: 仅在 expected 中存在 expected=%s", path, k, jsonOf(ev)))
			default:
				diffGolden(path+"."+k, ev, av, out)
			}
		}
	case []any:
		a, ok := act.([]any)
		if !ok {
			*out = append(*out, fmt.Sprintf("%s: 类型不一致 expected=array actual=%s", path, kindOf(act)))
			return
		}
		if len(e) != len(a) {
			*out = append(*out, fmt.Sprintf("%s: 长度不一致 expected=%d actual=%d", path, len(e), len(a)))
		}
		n := min(len(e), len(a))
		for i := 0; i < n; i++ {
			diffGolden(fmt.Sprintf("%s[%d]", path, i), e[i], a[i], out)
		}
	default:
		if !reflect.DeepEqual(exp, act) {
			*out = append(*out, fmt.Sprintf("%s: expected=%s actual=%s", path, jsonOf(exp), jsonOf(act)))
		}
	}
}

func jsonOf(v any) string {
	b, err := marshalPlain(v)
	if err != nil {
		return fmt.Sprintf("%v", v)
	}
	return string(b)
}

// marshalPlain/marshalIndentPlain 关闭 HTML 转义——占位符 <task_id> 等在 actual
// 与 diff 中保持人类可读形态。
func marshalPlain(v any) ([]byte, error) {
	var sb strings.Builder
	enc := json.NewEncoder(&sb)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return []byte(strings.TrimSuffix(sb.String(), "\n")), nil
}

func marshalIndentPlain(v any) ([]byte, error) {
	var sb strings.Builder
	enc := json.NewEncoder(&sb)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return []byte(strings.TrimSuffix(sb.String(), "\n")), nil
}

func kindOf(v any) string {
	if v == nil {
		return "null"
	}
	switch v.(type) {
	case map[string]any:
		return "object"
	case []any:
		return "array"
	default:
		return fmt.Sprintf("%T", v)
	}
}
