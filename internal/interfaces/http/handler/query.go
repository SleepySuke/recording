// 只读查询接口的 HTTP 适配（详设 §8.1）：任务查询、录音列表、录音详情。
// 路径 UUID 与分页参数非法 → 400/10001（详设 §8.3）。
package handler

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	appcode "recording-transcription/internal/application/errorcode"
	apprec "recording-transcription/internal/application/recording"
	"recording-transcription/internal/interfaces/http/dto"
	"recording-transcription/internal/pkg/uuid"
)

// QueryHandler GET /v1/tasks/{id}、GET /v1/recordings、GET /v1/recordings/{id}。
type QueryHandler struct {
	svc    *apprec.QueryService
	logger *slog.Logger
}

// NewQueryHandler 构造查询处理器。
func NewQueryHandler(svc *apprec.QueryService, logger *slog.Logger) *QueryHandler {
	return &QueryHandler{svc: svc, logger: logger}
}

// HandleTask GET /v1/tasks/:id。
func (h *QueryHandler) HandleTask(c *gin.Context) {
	id := c.Param("id")
	if !uuid.IsValid(id) {
		h.badParam(c, "task_id", id)
		return
	}
	view, err := h.svc.GetTask(c.Request.Context(), id)
	if err != nil {
		h.fail(c, err)
		return
	}
	c.JSON(http.StatusOK, dto.NewTaskBody(view))
}

// HandleRecording GET /v1/recordings/:id。
func (h *QueryHandler) HandleRecording(c *gin.Context) {
	id := c.Param("id")
	if !uuid.IsValid(id) {
		h.badParam(c, "recording_id", id)
		return
	}
	detail, err := h.svc.GetRecording(c.Request.Context(), id)
	if err != nil {
		h.fail(c, err)
		return
	}
	c.JSON(http.StatusOK, dto.NewRecordingDetailBody(detail))
}

// HandleList GET /v1/recordings?page=&page_size=：缺省 page=1、page_size=20（详设 §8.1）；
// 提供但非法（非数字）→ 400/10001，取值范围（≤0、超上限）由用例层统一校验。
func (h *QueryHandler) HandleList(c *gin.Context) {
	page := apprec.DefaultPage
	if raw := c.Query("page"); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil {
			h.badParam(c, "page", raw)
			return
		}
		page = v
	}
	pageSize := apprec.DefaultPageSize
	if raw := c.Query("page_size"); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil {
			h.badParam(c, "page_size", raw)
			return
		}
		pageSize = v
	}
	list, err := h.svc.ListRecordings(c.Request.Context(), page, pageSize)
	if err != nil {
		h.fail(c, err)
		return
	}
	c.JSON(http.StatusOK, dto.NewRecordingListBody(list))
}

// fail 用例错误 → 统一渲染：AppError 原样，未知错误归 90001。
func (h *QueryHandler) fail(c *gin.Context, err error) {
	var appErr *appcode.AppError
	if !errors.As(err, &appErr) {
		appErr = appcode.New(appcode.CodeInternalError, err)
	}
	c.Error(appErr)
}

// badParam 参数非法 → 400/10001（详设 §8.3）。
func (h *QueryHandler) badParam(c *gin.Context, name, raw string) {
	c.Error(appcode.New(appcode.CodeInvalidArgument, fmt.Errorf("%s = %q 非法", name, raw)))
}
