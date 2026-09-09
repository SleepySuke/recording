// Package handler 实现上传接口的 HTTP 适配（详设 §5.1：multipart 流式读取，
// 不调用 ParseMultipartForm，音频字节不进内存）。
package handler

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	appcode "recording-transcription/internal/application/errorcode"
	"recording-transcription/internal/application/ports"
	apprec "recording-transcription/internal/application/recording"
	"recording-transcription/internal/interfaces/http/dto"
	"recording-transcription/internal/pkg/uuid"
)

// UploadLimits 上传链路自身的限制与超时（详设 §5.1：读超时与总上传超时分开配置，
// 不把短 SQL 超时套在整个上传上）。
type UploadLimits struct {
	MaxBodyBytes    int64         // 请求体总上限（预留 multipart 开销）
	ReadIdleTimeout time.Duration // 单次读的空闲上限
	TotalTimeout    time.Duration // 整个上传的总超时
}

// UploadHandler POST /v1/recordings。
type UploadHandler struct {
	svc    *apprec.UploadService
	logger *slog.Logger
	lim    UploadLimits
}

// NewUploadHandler 构造上传处理器。
func NewUploadHandler(svc *apprec.UploadService, logger *slog.Logger, lim UploadLimits) *UploadHandler {
	return &UploadHandler{svc: svc, logger: logger, lim: lim}
}

// Handle 流式处理上传：MultipartReader 逐部分读取，取第一个名为 file 的文件部分；
// 找到后即停止读后续部分（多余部分忽略，详设 §5.1）。
func (h *UploadHandler) Handle(c *gin.Context) {
	// 请求体总上限 + 逐读空闲截止（详设 §5.1：不信任 Content-Length，流式计数）。
	rc := http.NewResponseController(c.Writer)
	c.Request.Body = io.NopCloser(&bodyLimitReader{
		r:      &idleReadReader{r: c.Request.Body, rc: rc, idle: h.lim.ReadIdleTimeout},
		remain: h.lim.MaxBodyBytes,
	})
	mr, err := c.Request.MultipartReader()
	if err != nil {
		c.Error(multipartError(err))
		return
	}

	// 总上传超时独立于读空闲超时（详设 §5.1）。
	ctx, cancel := context.WithTimeout(c.Request.Context(), h.lim.TotalTimeout)
	defer cancel()

	var filePart *multipart.Part
	for {
		p, err := mr.NextPart()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			c.Error(multipartError(err))
			return
		}
		if p.FormName() == "file" && p.FileName() != "" {
			filePart = p // 同名多余 file 部分取第一个（IT-15）
			break
		}
		// 多余字段/部分：忽略并 WARN，不因此拒绝请求（详设 §5.1）。
		h.logger.Warn("忽略多余 multipart 部分",
			"form_name", p.FormName(), "filename", p.FileName(),
			"request_id", requestID(c))
	}
	if filePart == nil {
		// 缺名为 file 的文件部分（与同名表单值区分）。
		c.Error(appcode.New(appcode.CodeFileRequired, nil))
		return
	}

	res, err := h.svc.Upload(ctx, apprec.UploadRequest{
		FilePart:  filePart,
		Filename:  filePart.FileName(),
		RequestID: requestID(c),
	})
	if err != nil {
		h.fail(c, err)
		return
	}
	c.JSON(http.StatusAccepted, dto.UploadResponse{
		RecordingID: res.RecordingID,
		TaskID:      res.TaskID,
		Status:      string(res.Status),
	})
}

// fail 用例错误 → HTTP：AppError 交统一渲染；ctx 取消/超时按客户端断开处理
// （详设 §5.1：连接已断无响应可发，仅记日志；499 为 nginx 语义的内部状态码）。
func (h *UploadHandler) fail(c *gin.Context, err error) {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		h.logger.Warn("上传中断（客户端断开或总超时）",
			"request_id", requestID(c), "err", err)
		c.AbortWithStatus(499)
		return
	}
	var appErr *appcode.AppError
	if !errors.As(err, &appErr) {
		appErr = appcode.New(appcode.CodeInternalError, err)
	}
	c.Error(appErr)
}

// multipartError multipart 读取失败：请求体超总上限 → 20004（413），其余按参数非法。
func multipartError(err error) *appcode.AppError {
	if errors.Is(err, ports.ErrFileTooLarge) {
		return appcode.New(appcode.CodeFileTooLarge, err)
	}
	return appcode.New(appcode.CodeInvalidArgument, err)
}

// requestID 安全读取中间件写入的 request_id；中间件缺失或类型异常时兜底生成
// （与 RequestID 中间件同源 uuid），不因 MustGet panic。
func requestID(c *gin.Context) string {
	if v, ok := c.Get("request_id"); ok {
		if s, ok := v.(string); ok && s != "" {
			return s
		}
	}
	return uuid.New()
}

// bodyLimitReader 请求体总上限（详设 §5.1：另设略大上限预留 multipart 开销）。
type bodyLimitReader struct {
	r      io.Reader
	remain int64
}

func (b *bodyLimitReader) Read(p []byte) (int, error) {
	if b.remain <= 0 {
		return 0, fmt.Errorf("%w: 请求体超过总上限", ports.ErrFileTooLarge)
	}
	if int64(len(p)) > b.remain {
		p = p[:b.remain]
	}
	n, err := b.r.Read(p)
	b.remain -= int64(n)
	return n, err
}

// idleReadReader 每次读前顺延读截止（详设 §5.1 读空闲超时）；底层连接不支持
// ResponseController（如 httptest.Recorder）时静默跳过。
type idleReadReader struct {
	r    io.Reader
	rc   *http.ResponseController
	idle time.Duration
}

func (i *idleReadReader) Read(p []byte) (int, error) {
	if i.idle > 0 && i.rc != nil {
		_ = i.rc.SetReadDeadline(time.Now().Add(i.idle))
	}
	return i.r.Read(p)
}
