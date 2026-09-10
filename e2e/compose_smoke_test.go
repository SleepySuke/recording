// E-COMPOSE：compose 全栈 golden 冒烟（测试设计 §5 两级 E2E 的第二级，T13）。
// 范围决策（T13 任务书收窄）：八个用例的深度验收由过程级套件（E01～E06 + 并发/
// 列表，随任务逐个落地，见 §5.0 映射表）承载；compose 层不重驱全量，只交付一条
// 完整用户旅程的单例 golden——make build 镜像 + compose 起栈（db healthy + app
// ready）→ 宿主经发布端口上传小 wav → 轮询至 done → 详情（result 三字段）→
// compose MySQL 事件链 → 命名卷内录音文件 → 宿主 ./logs/app.jsonl 事件镜像，
// 与 expected/E-COMPOSE.json 深度比对（§5.1 掩码机制复用 runGolden）。
//
// 替身注入（不改动 .env）：临时 compose override 文件仅覆写 app 两项 env——
// LLM_BASE_URL → http://host.docker.internal:<port>/v1（宿主 FakeLLM；Docker
// Desktop 将 host.docker.internal 转发到宿主回环，起跑前容器内 wget 预检实测）；
// MOCK_ASR_DELAY=100ms（确定性转写替身，测试设计 §2；生产 Mock 的 5～15s 随机
// 延迟与种子失败不适合冒烟）。收尾把栈复原为 .env 常规配置并保持运行（用户约定：
// 容器复用、不 compose down；app 容器因 env 变化被 compose 自动重建回常规配置）。
//
// 红线适配（测试设计 §2「测试库红线」）：冒烟直连的是 compose 应用库 recording
// （非 recording_test），库内是真实应用数据——绝不 TRUNCATE、不做全表 table_counts
// 断言，只断言本次上传自己 task_id 的行、事件链与该任务无重复 (task_id, event_seq)。
// 经 E2E_COMPOSE=1 显式开启，常规套件（make e2e / make test）不要求 Docker。
package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"recording-transcription/internal/infrastructure/llm"
	"recording-transcription/internal/infrastructure/persistence/mysql"

	"gorm.io/gorm"
)

// composeAppURL compose 发布的 app 端口固定 8080（compose.yaml ports）。
const composeAppURL = "http://127.0.0.1:8080"

// ecomposeActual E-COMPOSE.json 的采集形状（§5.1；与 E01 同构但去掉 table_counts：
// compose 库为真实应用数据不做全表断言，只比本任务自身链）。
type ecomposeActual struct {
	Upload struct {
		HTTPStatus  int    `json:"http_status"`
		RecordingID string `json:"recording_id"`
		TaskID      string `json:"task_id"`
		Status      string `json:"status"`
	} `json:"upload"`
	RecordingDetail struct {
		HTTPStatus       int    `json:"http_status"`
		ID               string `json:"id"`
		OriginalFilename string `json:"original_filename"`
		Extension        string `json:"extension"`
		SizeBytes        int64  `json:"size_bytes"`
		CreatedAt        string `json:"created_at"`
		Task             struct {
			ID      string `json:"id"`
			Status  string `json:"status"`
			Attempt int    `json:"attempt"`
		} `json:"task"`
		Transcript string      `json:"transcript"`
		Result     *resultBody `json:"result"`
	} `json:"recording_detail"`
	TaskFinal struct {
		Status      string      `json:"status"`
		Attempt     int         `json:"attempt"`
		EventSeq    int64       `json:"event_seq"`
		Transcript  string      `json:"transcript"`
		SummaryJSON *resultBody `json:"summary_json"`
	} `json:"task_final"`
	StatusSequence []string `json:"status_sequence"`
	Events         []evtRow `json:"events"`
	StorageFile    struct {
		Exists    bool  `json:"exists"`
		SizeBytes int64 `json:"size_bytes"`
	} `json:"storage_file"`
	MirrorEvents []string `json:"mirror_events"`
}

// TestE2ECompose：compose 全栈冒烟（E-COMPOSE）。总预算约 2～3 分钟（首起 db
// 初始化最慢）；全程可操作失败信息（docker 缺失 / 起栈失败附 app 日志尾部 /
// host.docker.internal 不可达各自单独提示）。
func TestE2ECompose(t *testing.T) {
	if os.Getenv("E2E_COMPOSE") != "1" {
		t.Skip("E2E_COMPOSE != 1，跳过 compose 全栈冒烟（E-COMPOSE；需 Docker 与 make build 镜像，测试设计 §5 第二级）")
	}

	// Docker / compose 可用性（E2E_COMPOSE=1 但无 Docker 时给出可操作信息，而非晦涩报错）。
	if out, err := tryCmd(30*time.Second, "docker", "compose", "version"); err != nil {
		t.Fatalf("docker compose 不可用（先启动 Docker Desktop）: %v\n%s", err, out)
	}

	// 宿主 FakeLLM 监听（127.0.0.1 即可：Docker Desktop 的 host.docker.internal
	// 转发到宿主回环；非 Docker Desktop 环境由下方容器内预检单独报错）。
	fake := llm.NewFake()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("FakeLLM 监听失败: %v", err)
	}
	fakeSrv := &http.Server{Handler: fake}
	go func() { _ = fakeSrv.Serve(ln) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = fakeSrv.Shutdown(ctx)
	})
	fakeHost := fmt.Sprintf("http://host.docker.internal:%d", ln.Addr().(*net.TCPAddr).Port)

	// 临时 compose override（t.TempDir 用后即删；不动 .env、不动 compose.yaml）。
	override := filepath.Join(t.TempDir(), "e-compose-override.yaml")
	ov := fmt.Sprintf(`# T13 E-COMPOSE 冒烟临时覆写（测试生成、用后即删；不改 .env）：
# LLM 指向宿主 FakeLLM；Mock ASR 切确定性替身（测试设计 §2，生产 Mock 不适合冒烟）。
services:
  app:
    environment:
      LLM_BASE_URL: %s
      MOCK_ASR_DELAY: 100ms
`, fakeHost+"/v1")
	if err := os.WriteFile(override, []byte(ov), 0o644); err != nil {
		t.Fatalf("写 override 失败: %v", err)
	}
	comp := func(args ...string) []string {
		return append([]string{"compose", "-f", "compose.yaml", "-f", override}, args...)
	}

	// 复原注册在最前（t.Cleanup LIFO → 最后执行、任何失败路径都会走到）：
	// 不带 override 重新 up——app 因 env 变化被重建回 .env 常规配置，栈保持运行。
	t.Cleanup(func() {
		out, err := tryCmd(3*time.Minute, "docker", "compose", "-f", "compose.yaml", "up", "-d", "--wait")
		if err != nil {
			t.Logf("复原 compose 常规配置未成功（栈未复原；可 make down && make start 重置）: %v\n%s", err, out)
			return
		}
		t.Log("compose 栈已复原为 .env 常规配置并保持运行（用户约定：容器复用，不 down）")
	})

	// 镜像缺失则构建（与 make start 同语义：已有镜像不自动重建；改码后请先 make build）。
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("获取工作目录失败: %v", err)
	}
	image := strings.ToLower(filepath.Base(wd)) + "-app:latest"
	if _, err := tryCmd(30*time.Second, "docker", "image", "inspect", image); err != nil {
		t.Logf("镜像 %s 缺失，先 compose build（约 1～2 分钟）", image)
		runCmd(t, 10*time.Minute, "docker", comp("build", "app")...)
	}

	// 起栈：db healthy + app ready（--wait 等两服务健康检查通过；首起 db 初始化
	// 最慢约 1 分钟）。失败附 app 日志尾部，避免黑盒超时。
	t.Log("compose 起栈（docker compose up -d --wait）…")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if out, err := exec.CommandContext(ctx, "docker", comp("up", "-d", "--wait")...).CombinedOutput(); err != nil {
		logOut, _ := tryCmd(15*time.Second, "docker", comp("logs", "app", "--tail", "40")...)
		t.Fatalf("compose up --wait 失败（db healthy + app ready 未达成；可 make logs 查看原因）: %v\n--- app 日志尾部 ---\n%s\n--- up 输出 ---\n%s", err, logOut, out)
	}

	// 宿主侧 readyz 探测（--wait 已保证容器内健康，此处验证发布端口 8080 对宿主可达）。
	deadline := time.Now().Add(30 * time.Second)
	for {
		resp, err := http.Get(composeAppURL + "/readyz")
		if err == nil {
			code := resp.StatusCode
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if code == http.StatusOK {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("宿主探测 %s/readyz 30s 内未就绪（发布端口绑定异常？make logs 查看原因）", composeAppURL)
		}
		time.Sleep(500 * time.Millisecond)
	}

	// 容器内预检 host.docker.internal → 宿主 FakeLLM 可达（FakeLLM 对任意路径 200 应答）。
	if out, err := tryCmd(15*time.Second, "docker", comp("exec", "-T", "app", "wget", "-q", "-T", "3", "-O", "/dev/null", fakeHost+"/preflight")...); err != nil {
		t.Fatalf("容器内无法访问宿主 FakeLLM（%s）: %v\n%s——host.docker.internal 需 Docker Desktop（Mac/Win）；Linux 需为 app 加 extra_hosts: \"host.docker.internal:host-gateway\"", fakeHost, err, out)
	}

	// compose 应用库直连（红线适配：应用库 recording 非 recording_test，不 TRUNCATE、
	// 只查本任务行；DSN 由 .env 的 MYSQL_* 推导，端口取 MYSQL_PORT——本机 3306 被共享
	// 实例占用，.env 已设 3307）。
	db := openComposeDB(t)
	h := &e2eHarness{
		t:       t,
		DB:      db,
		BaseURL: composeAppURL,
		LogDir:  "logs", // compose 将 /app/logs 绑定到仓库 ./logs
		Fake:    fake,
		client:  &http.Client{Timeout: 30 * time.Second},
	}

	// 用户旅程：上传小 wav → 轮询至 done。
	content := contentOf(8 * 1024)
	resp, upCode, err := h.postUpload("e-compose-meeting.wav", content)
	if err != nil {
		t.Fatalf("上传失败: %v", err)
	}
	if upCode != http.StatusAccepted || resp.TaskID == "" || resp.RecordingID == "" {
		t.Fatalf("上传响应异常: status=%d body=%+v", upCode, resp)
	}

	// 轮询至 done；failed 单独提示（最常见根因 = 容器外呼 LLM/转写替身不可达）。
	pollDeadline := time.Now().Add(90 * time.Second)
	var lastStatus string
	for {
		var tb taskBody
		code, body := h.doGet("/v1/tasks/" + resp.TaskID)
		if code != http.StatusOK {
			t.Fatalf("GET /v1/tasks/%s status = %d, want 200, body=%q", resp.TaskID, code, body)
		}
		if err := json.Unmarshal(body, &tb); err != nil {
			t.Fatalf("任务响应不是合法 JSON: %v, body=%q", err, body)
		}
		lastStatus = tb.Status
		if tb.Status == "done" {
			break
		}
		if tb.Status == "failed" {
			logOut, _ := tryCmd(15*time.Second, "docker", comp("logs", "app", "--tail", "30")...)
			t.Fatalf("任务进入 failed（当前 %s）：摘要/转写外呼失败——检查容器 → 宿主 FakeLLM 可达性与 make build 镜像新旧\n--- app 日志尾部 ---\n%s", fakeHost, logOut)
		}
		if time.Now().After(pollDeadline) {
			t.Fatalf("任务 %s 未在 90s 内到达 done，最后状态 = %s", resp.TaskID, lastStatus)
		}
		time.Sleep(200 * time.Millisecond)
	}

	// 采集（与 E01 同构）：详情 → DB 终态/事件链 → 命名卷文件 → 日志镜像。
	var a ecomposeActual
	a.Upload.HTTPStatus = upCode
	a.Upload.RecordingID = resp.RecordingID
	a.Upload.TaskID = resp.TaskID
	a.Upload.Status = resp.Status

	dCode, dRaw := h.doGet("/v1/recordings/" + resp.RecordingID)
	var d detailBody
	if dCode != http.StatusOK {
		t.Fatalf("GET 详情 status = %d, want 200, body=%q", dCode, dRaw)
	}
	if err := json.Unmarshal(dRaw, &d); err != nil {
		t.Fatalf("详情响应不是合法 JSON: %v, body=%q", err, dRaw)
	}
	a.RecordingDetail.HTTPStatus = dCode
	a.RecordingDetail.ID = d.ID
	a.RecordingDetail.OriginalFilename = d.OriginalFilename
	a.RecordingDetail.Extension = d.Extension
	a.RecordingDetail.SizeBytes = d.SizeBytes
	a.RecordingDetail.CreatedAt = d.CreatedAt
	a.RecordingDetail.Task.ID = d.Task.ID
	a.RecordingDetail.Task.Status = d.Task.Status
	a.RecordingDetail.Task.Attempt = d.Task.Attempt
	if d.Transcript != nil {
		a.RecordingDetail.Transcript = *d.Transcript
	}
	a.RecordingDetail.Result = d.Result

	var task struct {
		Status      string
		Attempt     int
		EventSeq    int64
		Transcript  string
		SummaryJSON *string
	}
	if err := db.Raw("SELECT status, attempt, event_seq, transcript, summary_json FROM tasks WHERE id = ?",
		resp.TaskID).Scan(&task).Error; err != nil {
		t.Fatalf("查询任务失败: %v", err)
	}
	a.TaskFinal.Status = task.Status
	a.TaskFinal.Attempt = task.Attempt
	a.TaskFinal.EventSeq = task.EventSeq
	a.TaskFinal.Transcript = task.Transcript
	if task.SummaryJSON != nil {
		var rb resultBody
		if err := json.Unmarshal([]byte(*task.SummaryJSON), &rb); err != nil {
			t.Fatalf("summary_json 不可解析: %v, raw=%q", err, *task.SummaryJSON)
		}
		a.TaskFinal.SummaryJSON = &rb
	}

	a.Events = h.taskEventChain(resp.TaskID)
	for _, e := range a.Events {
		a.StatusSequence = append(a.StatusSequence, e.ToStatus)
	}

	// 命名卷内录音文件（recordings-data 卷挂 /app/data/recordings，容器内 wc 实测；
	// 失败不 Fatal，交给 golden 呈现 exists=false 的字段级差异）。
	var storagePath string
	if err := db.Raw("SELECT storage_path FROM recordings WHERE id = ?",
		resp.RecordingID).Scan(&storagePath).Error; err != nil {
		t.Fatalf("查询录音失败: %v", err)
	}
	if out, err := tryCmd(30*time.Second, "docker", comp("exec", "-T", "app", "sh", "-c",
		fmt.Sprintf(`wc -c < "/app/data/recordings/%s"`, storagePath))...); err == nil {
		if size, perr := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64); perr == nil {
			a.StorageFile.Exists = true
			a.StorageFile.SizeBytes = size
		}
	} else {
		t.Logf("命名卷内文件检查失败: %v\n%s", err, out)
	}

	// 宿主 ./logs/app.jsonl 内该任务的事件镜像（按出现序，应与 DB 事件链同序）。
	a.MirrorEvents = h.mirroredEvents(resp.TaskID, 4)

	// 不变量巡检（测试设计 §6，范围限定本任务行——compose 库为真实应用数据，
	// 不做全库巡检）：无重复 (task_id, event_seq)、事件链 event_seq 从 1 连续。
	var dup int64
	if err := db.Raw(
		`SELECT COUNT(*) FROM (SELECT task_id, event_seq FROM task_events WHERE task_id = ? GROUP BY task_id, event_seq HAVING COUNT(*) > 1) d`,
		resp.TaskID).Scan(&dup).Error; err != nil {
		t.Fatalf("event_seq 重复巡检失败: %v", err)
	}
	if dup != 0 {
		t.Errorf("不变量违规：任务 %s 存在 %d 组 (task_id, event_seq) 重复", resp.TaskID, dup)
	}
	for i, e := range a.Events {
		if e.EventSeq != int64(i+1) {
			t.Errorf("不变量违规：事件链不连续（第 %d 条 event_seq=%d，应从 1 连续递增）", i+1, e.EventSeq)
		}
	}

	// golden 深度比对（§5.1，掩码机制复用 runGolden：真实 ID → 占位符、seed/时间戳归一）。
	runGolden(t, "E-COMPOSE", &a, map[string]string{
		resp.RecordingID: "<recording_id>",
		resp.TaskID:      "<task_id>",
	})
}

// openComposeDB 打开 compose 应用库（非测试库，红线适配见包注释）：DSN 由仓库
// .env 的 MYSQL_* 推导（端口取 MYSQL_PORT，本机 3307）；30s 连接重试覆盖 db
// 健康检查通过后的短暂就绪窗口。调用方只读/只查本任务行，绝不 TRUNCATE。
func openComposeDB(t *testing.T) *gorm.DB {
	t.Helper()
	env := readDotEnv(t, ".env")
	port := env["MYSQL_PORT"]
	if port == "" {
		port = "3306" // compose.yaml 宿主端口默认值
	}
	user := env["MYSQL_USER"]
	if user == "" {
		user = "recording"
	}
	pass := env["MYSQL_PASSWORD"]
	if pass == "" {
		t.Fatalf(".env 缺 MYSQL_PASSWORD（compose db 容器凭据，compose.yaml 初始化所需）")
	}
	dbname := env["MYSQL_DATABASE"]
	if dbname == "" {
		dbname = "recording"
	}
	dsn := fmt.Sprintf("%s:%s@tcp(127.0.0.1:%s)/%s?parseTime=true&loc=UTC", user, pass, port, dbname)

	deadline := time.Now().Add(30 * time.Second)
	for {
		db, err := mysql.Open(dsn)
		if err == nil {
			var one int
			if err := db.Raw("SELECT 1").Scan(&one).Error; err == nil {
				t.Cleanup(func() {
					if sqlDB, err := db.DB(); err == nil {
						_ = sqlDB.Close()
					}
				})
				return db
			}
			if sqlDB, err := db.DB(); err == nil {
				_ = sqlDB.Close()
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("30s 内连不上 compose 应用库（127.0.0.1:%s/%s）：%v", port, dbname, err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// readDotEnv 极简 .env 解析（仅本冒烟推导 compose db 连接用；不去动其他键）。
func readDotEnv(t *testing.T, path string) map[string]string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读取 %s 失败: %v", path, err)
	}
	out := make(map[string]string, 8)
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if k, v, ok := strings.Cut(line, "="); ok {
			out[strings.TrimSpace(k)] = strings.Trim(strings.TrimSpace(v), `"'`)
		}
	}
	return out
}

// runCmd 执行命令，失败即 Fatal（附合并输出）；冒烟的主路径命令用。
func runCmd(t *testing.T, timeout time.Duration, name string, args ...string) {
	t.Helper()
	out, err := tryCmd(timeout, name, args...)
	if err != nil {
		t.Fatalf("%s %s 失败: %v\n%s", name, strings.Join(args, " "), err, out)
	}
}

// tryCmd 执行命令返回 (合并输出, 错误)，不 Fatal：预检/采集类调用允许调用方
// 自行决定失败语义（可操作提示或交 golden 呈现差异）。
func tryCmd(timeout time.Duration, name string, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return exec.CommandContext(ctx, name, args...).CombinedOutput()
}
