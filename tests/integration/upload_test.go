package integration

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	apprec "recording-transcription/internal/application/recording"
	"recording-transcription/internal/infrastructure/filestore/local"
	"recording-transcription/internal/infrastructure/logging"
	"recording-transcription/internal/infrastructure/persistence/mysql"
	httpapi "recording-transcription/internal/interfaces/http"
	"recording-transcription/internal/interfaces/http/handler"
)

// 测试依据：T04 步骤 3；设计依据：详设 §5.1（上传判定树逐分支）、§4.6（成对创建）、
// §7.3（事件原子性）、§7.4（事件镜像）；架构 §3（事务①）。用例编号 IT-01 / IT-13 / IT-15。

const (
	itMaxFileBytes = 50 * 1024 * 1024 // 50MiB（详设 §5.1）
	itMaxBodyBytes = 53 * 1024 * 1024 // 请求体总上限（与生产默认 UPLOAD_MAX_BODY_MB=53 一致）
	itInstanceID   = "11111111-1111-1111-1111-111111111111"
)

type uploadResp struct {
	RecordingID string `json:"recording_id"`
	TaskID      string `json:"task_id"`
	Status      string `json:"status"`
}

type apiErrorBody struct {
	Error struct {
		Code      int    `json:"code"`
		Message   string `json:"message"`
		RequestID string `json:"request_id"`
	} `json:"error"`
}

// newUploadEnv 组装完整上传链：真实 MySQL + 本地文件存储 + 路由（httptest 走完整 HTTP 中间件链）。
// 数据目录与日志目录均为测试临时目录；RequireTestDB 保证三表从空开始。
func newUploadEnv(t *testing.T) (*gin.Engine, *gorm.DB, string, string) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	db := RequireTestDB(t)

	dataDir := t.TempDir()
	logDir := t.TempDir()
	logger, err := logging.New(logging.Options{Dir: logDir, Level: "INFO", MaxSizeMB: 20, MaxBackups: 5, MaxAgeDays: 7})
	if err != nil {
		t.Fatalf("初始化测试日志失败: %v", err)
	}
	store, err := local.New(dataDir, itMaxFileBytes, 512*1024*1024)
	if err != nil {
		t.Fatalf("初始化测试数据目录失败: %v", err)
	}
	svc := apprec.NewUploadService(store, mysql.NewRecordingTx(db, itInstanceID, logger), nil, logger, itInstanceID)
	h := handler.NewUploadHandler(svc, logger, handler.UploadLimits{
		MaxBodyBytes:    itMaxBodyBytes,
		ReadIdleTimeout: 10 * time.Second,
		TotalTimeout:    time.Minute,
	})
	// T05 起查询接口与上传共用同一引擎与测试库（只读，不影响上传断言）。
	queryHandler := handler.NewQueryHandler(apprec.NewQueryService(mysql.NewRecordingQuery(db), logger), logger)
	return httpapi.New(logger, h, queryHandler, nil, nil, nil), db, dataDir, logDir
}

type formPart struct {
	field, filename string
	content         []byte
}

func doUpload(t *testing.T, r *gin.Engine, parts ...formPart) *httptest.ResponseRecorder {
	t.Helper()
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	for _, p := range parts {
		fw, err := w.CreateFormFile(p.field, p.filename)
		if err != nil {
			t.Fatalf("构造 multipart 失败: %v", err)
		}
		if _, err := fw.Write(p.content); err != nil {
			t.Fatalf("写入 multipart 内容失败: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("关闭 multipart writer 失败: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/recordings", &buf)
	req.Header.Set("Content-Type", w.FormDataContentType())
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	return rec
}

func decodeUpload(t *testing.T, w *httptest.ResponseRecorder) uploadResp {
	t.Helper()
	var resp uploadResp
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("202 响应不是合法 JSON: %v, body=%q", err, w.Body.String())
	}
	return resp
}

func decodeAPIError(t *testing.T, w *httptest.ResponseRecorder) apiErrorBody {
	t.Helper()
	var body apiErrorBody
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("错误响应不是合法 JSON: %v, body=%q", err, w.Body.String())
	}
	return body
}

// tableCount 断言式取行数。
func tableCount(t *testing.T, db *gorm.DB, table string) int64 {
	t.Helper()
	var n int64
	if err := db.Raw("SELECT COUNT(*) FROM " + table).Scan(&n).Error; err != nil {
		t.Fatalf("统计 %s 失败: %v", table, err)
	}
	return n
}

// dataFiles 列出数据目录全部文件（断言无残留）。
func dataFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("读数据目录失败: %v", err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

func contentOf(size int) []byte {
	pattern := []byte("abcdefghijklmnopqrstuvwxyz012345")
	out := bytes.Repeat(pattern, size/len(pattern))
	return append(out, pattern[:size%len(pattern)]...)
}

// TestIT01_UploadCreatesTripleInOneTx：上传成功后 recordings / tasks(pending) /
// task_created 三行同事务可见（缺一不可），且事件镜像写入 logs/app.jsonl（§7.4）。
func TestIT01_UploadCreatesTripleInOneTx(t *testing.T) {
	r, db, dataDir, logDir := newUploadEnv(t)

	content := contentOf(4096)
	w := doUpload(t, r, formPart{field: "file", filename: "it01-meeting.wav", content: content})
	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202, body=%q", w.Code, w.Body.String())
	}
	resp := decodeUpload(t, w)
	if resp.RecordingID == "" || resp.TaskID == "" || resp.Status != "pending" {
		t.Fatalf("响应体 = %+v, want recording_id/task_id 非空且 status=pending", resp)
	}

	// recordings 行。
	var rec struct {
		OriginalFilename string
		StoragePath      string
		Extension        string
		SizeBytes        int64
		ContentHash      string
	}
	if err := db.Raw("SELECT original_filename, storage_path, extension, size_bytes, content_hash FROM recordings WHERE id = ?", resp.RecordingID).Scan(&rec).Error; err != nil {
		t.Fatalf("查询 recordings 失败: %v", err)
	}
	if rec.OriginalFilename != "it01-meeting.wav" || rec.Extension != "wav" || rec.SizeBytes != int64(len(content)) {
		t.Errorf("recordings 行不符: %+v", rec)
	}
	if _, err := os.Stat(filepath.Join(dataDir, rec.StoragePath)); err != nil {
		t.Errorf("落盘文件不存在（storage_path=%s）: %v", rec.StoragePath, err)
	}

	// tasks 行：pending、attempt=1、event_seq=1、逻辑关联一致。
	var task struct {
		Status      string
		Attempt     int
		EventSeq    int64
		RecordingID string
	}
	if err := db.Raw("SELECT status, attempt, event_seq, recording_id FROM tasks WHERE id = ?", resp.TaskID).Scan(&task).Error; err != nil {
		t.Fatalf("查询 tasks 失败: %v", err)
	}
	if task.Status != "pending" || task.Attempt != 1 || task.EventSeq != 1 || task.RecordingID != resp.RecordingID {
		t.Errorf("tasks 行不符: %+v", task)
	}

	// task_events 行：恰好一条 task_created，与状态同事务提交。
	var evt struct {
		Event      string
		EventSeq   int64
		Attempt    int
		FromStatus *string
		ToStatus   string
		InstanceID string
	}
	if err := db.Raw("SELECT event, event_seq, attempt, from_status, to_status, instance_id FROM task_events WHERE task_id = ?", resp.TaskID).Scan(&evt).Error; err != nil {
		t.Fatalf("查询 task_events 失败: %v", err)
	}
	if evt.Event != "task_created" || evt.EventSeq != 1 || evt.Attempt != 1 || evt.FromStatus != nil || evt.ToStatus != "pending" || evt.InstanceID != itInstanceID {
		t.Errorf("task_events 行不符: %+v", evt)
	}

	// 事件镜像（尽力而为）落到 app.jsonl，可按 task_id 还原（§7.4）。
	logData, err := os.ReadFile(filepath.Join(logDir, "app.jsonl"))
	if err != nil {
		t.Fatalf("读取 app.jsonl 失败: %v", err)
	}
	mirrored := false
	for _, line := range strings.Split(string(logData), "\n") {
		if strings.Contains(line, `"event":"task_created"`) && strings.Contains(line, `"task_id":"`+resp.TaskID+`"`) {
			mirrored = true
		}
	}
	if !mirrored {
		t.Errorf("app.jsonl 未找到 task_created 镜像（task_id=%s）", resp.TaskID)
	}
}

// TestIT13_ContentHashCorrect：已知字节流上传后 recordings.content_hash 等于预计算 SHA-256。
func TestIT13_ContentHashCorrect(t *testing.T) {
	r, db, _, _ := newUploadEnv(t)

	content := contentOf(3*64*1024 + 17) // 跨缓冲边界
	sum := sha256.Sum256(content)
	want := hex.EncodeToString(sum[:])

	w := doUpload(t, r, formPart{field: "file", filename: "it13-audio.m4a", content: content})
	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202, body=%q", w.Code, w.Body.String())
	}
	resp := decodeUpload(t, w)

	var got struct {
		ContentHash string
		SizeBytes   int64
	}
	if err := db.Raw("SELECT content_hash, size_bytes FROM recordings WHERE id = ?", resp.RecordingID).Scan(&got).Error; err != nil {
		t.Fatalf("查询 recordings 失败: %v", err)
	}
	if got.ContentHash != want {
		t.Errorf("content_hash = %q, want %q", got.ContentHash, want)
	}
	if got.SizeBytes != int64(len(content)) {
		t.Errorf("size_bytes = %d, want %d", got.SizeBytes, len(content))
	}
}

// TestIT15_UploadBoundaries：详设 §5.1 判定树边界——恰好 50MiB 通过、超 1 字节 413/20004、
// 空文件 400/20002、无后缀 400/20003、双后缀按最后后缀判定、多余 file 部分取第一个。
func TestIT15_UploadBoundaries(t *testing.T) {
	t.Run("恰好50MiB通过", func(t *testing.T) {
		r, db, _, _ := newUploadEnv(t)
		content := contentOf(itMaxFileBytes)
		w := doUpload(t, r, formPart{field: "file", filename: "exact.wav", content: content})
		if w.Code != http.StatusAccepted {
			t.Fatalf("status = %d, want 202, body=%q", w.Code, w.Body.String())
		}
		resp := decodeUpload(t, w)
		var size int64
		if err := db.Raw("SELECT size_bytes FROM recordings WHERE id = ?", resp.RecordingID).Scan(&size).Error; err != nil {
			t.Fatalf("查询失败: %v", err)
		}
		if size != int64(itMaxFileBytes) {
			t.Errorf("size_bytes = %d, want %d", size, itMaxFileBytes)
		}
	})

	t.Run("超1字节413且无残留", func(t *testing.T) {
		r, db, dataDir, _ := newUploadEnv(t)
		w := doUpload(t, r, formPart{field: "file", filename: "over.wav", content: contentOf(itMaxFileBytes + 1)})
		if w.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("status = %d, want 413, body=%q", w.Code, w.Body.String())
		}
		if body := decodeAPIError(t, w); body.Error.Code != 20004 {
			t.Errorf("code = %d, want 20004", body.Error.Code)
		}
		for _, table := range []string{"recordings", "tasks", "task_events"} {
			if n := tableCount(t, db, table); n != 0 {
				t.Errorf("%s 残留 %d 行", table, n)
			}
		}
		if names := dataFiles(t, dataDir); len(names) != 0 {
			t.Errorf("数据目录残留文件: %v", names)
		}
	})

	t.Run("空文件400_20002", func(t *testing.T) {
		r, db, dataDir, _ := newUploadEnv(t)
		w := doUpload(t, r, formPart{field: "file", filename: "empty.wav"})
		if w.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400, body=%q", w.Code, w.Body.String())
		}
		if body := decodeAPIError(t, w); body.Error.Code != 20002 {
			t.Errorf("code = %d, want 20002", body.Error.Code)
		}
		for _, table := range []string{"recordings", "tasks", "task_events"} {
			if n := tableCount(t, db, table); n != 0 {
				t.Errorf("%s 残留 %d 行", table, n)
			}
		}
		if names := dataFiles(t, dataDir); len(names) != 0 {
			t.Errorf("数据目录残留文件: %v", names)
		}
	})

	t.Run("无后缀400_20003", func(t *testing.T) {
		r, _, _, _ := newUploadEnv(t)
		w := doUpload(t, r, formPart{field: "file", filename: "noextension", content: []byte("x")})
		if w.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400, body=%q", w.Code, w.Body.String())
		}
		if body := decodeAPIError(t, w); body.Error.Code != 20003 {
			t.Errorf("code = %d, want 20003", body.Error.Code)
		}
	})

	t.Run("双后缀按最后后缀判定", func(t *testing.T) {
		r, db, _, _ := newUploadEnv(t)
		// a.wav.mp3 → 按 mp3 接受。
		w := doUpload(t, r, formPart{field: "file", filename: "a.wav.mp3", content: []byte("content-a")})
		if w.Code != http.StatusAccepted {
			t.Fatalf("a.wav.mp3 status = %d, want 202, body=%q", w.Code, w.Body.String())
		}
		resp := decodeUpload(t, w)
		var ext string
		if err := db.Raw("SELECT extension FROM recordings WHERE id = ?", resp.RecordingID).Scan(&ext).Error; err != nil {
			t.Fatalf("查询失败: %v", err)
		}
		if ext != "mp3" {
			t.Errorf("extension = %q, want mp3", ext)
		}

		// a.mp3.exe → 按 exe 拒绝。
		w2 := doUpload(t, r, formPart{field: "file", filename: "a.mp3.exe", content: []byte("content-b")})
		if w2.Code != http.StatusBadRequest {
			t.Fatalf("a.mp3.exe status = %d, want 400", w2.Code)
		}
		if body := decodeAPIError(t, w2); body.Error.Code != 20003 {
			t.Errorf("code = %d, want 20003", body.Error.Code)
		}
	})

	t.Run("缺少file字段400_20001", func(t *testing.T) {
		r, _, _, _ := newUploadEnv(t)
		// 名为 audio 的文件部分不构成 file 字段（与同名表单值同样不计数）。
		w := doUpload(t, r, formPart{field: "audio", filename: "a.wav", content: []byte("x")})
		if w.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400, body=%q", w.Code, w.Body.String())
		}
		if body := decodeAPIError(t, w); body.Error.Code != 20001 {
			t.Errorf("code = %d, want 20001", body.Error.Code)
		}
	})

	t.Run("多余file部分取第一个", func(t *testing.T) {
		r, db, dataDir, _ := newUploadEnv(t)
		first := []byte("FIRST-PART-CONTENT")
		second := []byte("SECOND")
		w := doUpload(t, r,
			formPart{field: "file", filename: "dup.wav", content: first},
			formPart{field: "file", filename: "dup2.mp3", content: second},
		)
		if w.Code != http.StatusAccepted {
			t.Fatalf("status = %d, want 202, body=%q", w.Code, w.Body.String())
		}
		resp := decodeUpload(t, w)
		sum := sha256.Sum256(first)
		var rec struct {
			ContentHash string
			SizeBytes   int64
			StoragePath string
		}
		if err := db.Raw("SELECT content_hash, size_bytes, storage_path FROM recordings WHERE id = ?", resp.RecordingID).Scan(&rec).Error; err != nil {
			t.Fatalf("查询失败: %v", err)
		}
		if rec.ContentHash != hex.EncodeToString(sum[:]) || rec.SizeBytes != int64(len(first)) {
			t.Errorf("应取第一个 file 部分: hash=%q size=%d", rec.ContentHash, rec.SizeBytes)
		}
		data, err := os.ReadFile(filepath.Join(dataDir, rec.StoragePath))
		if err != nil {
			t.Fatalf("读落盘文件失败: %v", err)
		}
		if !bytes.Equal(data, first) {
			t.Error("落盘内容应为第一个 file 部分")
		}
	})
}
