// 查询用例（详设 §8.1）：分页参数校验 + 端口哨兵错误 → 数字业务码映射（§8.3）。
package recording

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"recording-transcription/internal/application/errorcode"
	"recording-transcription/internal/application/ports"
)

// 分页约定（详设 §8.1）：默认 page=1、page_size=20、上限 100；非法值 400/10001。
const (
	DefaultPage     = 1
	DefaultPageSize = 20
	MaxPageSize     = 100
)

// QueryService 只读查询用例：GetTask / GetRecording / ListRecordings。
type QueryService struct {
	q      ports.RecordingQuery
	logger *slog.Logger
}

// NewQueryService 构造查询用例。
func NewQueryService(q ports.RecordingQuery, logger *slog.Logger) *QueryService {
	return &QueryService{q: q, logger: logger}
}

// GetTask 任务状态查询：不存在/删除中 → 404/30001。
func (s *QueryService) GetTask(ctx context.Context, taskID string) (ports.TaskView, error) {
	view, err := s.q.GetTask(ctx, taskID)
	if err != nil {
		return ports.TaskView{}, s.mapErr(err)
	}
	return view, nil
}

// GetRecording 录音详情查询：不存在/删除中 → 404/20005；缺任务 → 500/90004。
func (s *QueryService) GetRecording(ctx context.Context, id string) (ports.RecordingDetail, error) {
	detail, err := s.q.GetRecording(ctx, id)
	if err != nil {
		return ports.RecordingDetail{}, s.mapErr(err)
	}
	return detail, nil
}

// ListRecordings 分页列表：page ≤ 0、page_size ≤ 0 或超上限 → 400/10001（详设 §8.1）。
func (s *QueryService) ListRecordings(ctx context.Context, page, pageSize int) (ports.RecordingList, error) {
	if page <= 0 || pageSize <= 0 || pageSize > MaxPageSize {
		return ports.RecordingList{}, errorcode.New(errorcode.CodeInvalidArgument,
			fmt.Errorf("非法分页参数 page=%d page_size=%d", page, pageSize))
	}
	list, err := s.q.ListRecordings(ctx, page, pageSize)
	if err != nil {
		return ports.RecordingList{}, s.mapErr(err)
	}
	return list, nil
}

// mapErr 端口哨兵错误 → AppError；90004 与数据库故障记日志（详设 §8.3：90004 需记录告警）。
func (s *QueryService) mapErr(err error) error {
	switch {
	case errors.Is(err, ports.ErrTaskNotFound):
		return errorcode.New(errorcode.CodeTaskNotFound, err)
	case errors.Is(err, ports.ErrRecordingNotFound):
		return errorcode.New(errorcode.CodeRecordingNotFound, err)
	case errors.Is(err, ports.ErrDataInconsistent):
		s.logger.Error("查询发现逻辑关联异常", slog.Any("err", err))
		return errorcode.New(errorcode.CodeDataInconsistent, err)
	default:
		s.logger.Error("查询数据库失败", slog.Any("err", err))
		return errorcode.New(errorcode.CodeDatabaseUnavailable, err)
	}
}
