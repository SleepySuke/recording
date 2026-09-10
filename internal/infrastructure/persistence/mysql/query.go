// 只读查询适配器（详设 §8.1；架构 §3 查询只走数据库）：全部 SELECT、不加锁；
// 所有查询过滤 deleting_at IS NULL；正常录音缺任务不静默隐藏，返回 ErrDataInconsistent（§4.6）。
package mysql

import (
	"context"
	"errors"
	"fmt"

	"gorm.io/gorm"

	"recording-transcription/internal/application/ports"
	domain "recording-transcription/internal/domain/recording"
)

// RecordingQueryGORM RecordingQuery 端口的 MySQL 实现。
type RecordingQueryGORM struct {
	db *gorm.DB
}

// NewRecordingQuery 构造查询端口实现。
func NewRecordingQuery(db *gorm.DB) *RecordingQueryGORM {
	return &RecordingQueryGORM{db: db}
}

// GetTask 按 ID 查任务；其录音缺失或删除中同样按任务不可见处理（详设 §8.3：30001）。
func (q *RecordingQueryGORM) GetTask(ctx context.Context, taskID string) (ports.TaskView, error) {
	var po TaskPO
	err := q.db.WithContext(ctx).Where("id = ?", taskID).First(&po).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return ports.TaskView{}, ports.ErrTaskNotFound
	}
	if err != nil {
		return ports.TaskView{}, fmt.Errorf("查询 tasks 失败: %w", err)
	}
	var rec RecordingPO
	err = q.db.WithContext(ctx).Select("deleting_at").Where("id = ?", po.RecordingID).First(&rec).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return ports.TaskView{}, ports.ErrTaskNotFound
	}
	if err != nil {
		return ports.TaskView{}, fmt.Errorf("查询 recordings 失败: %w", err)
	}
	if rec.DeletingAt != nil {
		return ports.TaskView{}, ports.ErrTaskNotFound
	}
	return taskView(po), nil
}

// GetRecording 查录音详情（删除中不可见）；正常录音缺任务 → ErrDataInconsistent（§4.6）。
func (q *RecordingQueryGORM) GetRecording(ctx context.Context, id string) (ports.RecordingDetail, error) {
	var rec RecordingPO
	err := q.db.WithContext(ctx).Where("id = ? AND deleting_at IS NULL", id).First(&rec).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return ports.RecordingDetail{}, ports.ErrRecordingNotFound
	}
	if err != nil {
		return ports.RecordingDetail{}, fmt.Errorf("查询 recordings 失败: %w", err)
	}

	var tk TaskPO
	err = q.db.WithContext(ctx).Where("recording_id = ?", rec.ID).First(&tk).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return ports.RecordingDetail{}, fmt.Errorf("%w: 录音 %s 缺任务", ports.ErrDataInconsistent, rec.ID)
	}
	if err != nil {
		return ports.RecordingDetail{}, fmt.Errorf("查询 tasks 失败: %w", err)
	}

	detail := ports.RecordingDetail{
		Task:             taskView(tk),
		OriginalFilename: rec.OriginalFilename,
		Extension:        rec.Extension,
		SizeBytes:        rec.SizeBytes,
		CreatedAt:        rec.CreatedAt,
	}
	if tk.Transcript != "" {
		t := tk.Transcript
		detail.Transcript = &t
	}
	if domain.TaskStatus(tk.Status) == domain.StatusDone {
		result, err := summaryFromPO(&tk)
		if err != nil {
			return ports.RecordingDetail{}, fmt.Errorf("%w: 录音 %s 任务 %s: %v",
				ports.ErrDataInconsistent, rec.ID, tk.ID, err)
		}
		detail.Result = result
	}
	return detail, nil
}

// ListRecordings 分页列表：created_at DESC, id DESC；逐项要求任务存在，缺任务整体报
// ErrDataInconsistent（500/90004），不用 JOIN 静默隐藏（§4.6）。tasks.recording_id UNIQUE，
// 单行即最新状态。
func (q *RecordingQueryGORM) ListRecordings(ctx context.Context, page, pageSize int) (ports.RecordingList, error) {
	var total int64
	if err := q.db.WithContext(ctx).Model(&RecordingPO{}).
		Where("deleting_at IS NULL").Count(&total).Error; err != nil {
		return ports.RecordingList{}, fmt.Errorf("统计 recordings 失败: %w", err)
	}
	list := ports.RecordingList{
		Items:    []ports.RecordingListItem{}, // 空页输出空数组而非 null
		Page:     page,
		PageSize: pageSize,
		Total:    int(total),
	}

	var recs []RecordingPO
	err := q.db.WithContext(ctx).Where("deleting_at IS NULL").
		Order("created_at DESC, id DESC").
		Limit(pageSize).Offset((page - 1) * pageSize).
		Find(&recs).Error
	if err != nil {
		return ports.RecordingList{}, fmt.Errorf("分页查询 recordings 失败: %w", err)
	}
	if len(recs) == 0 {
		return list, nil
	}

	ids := make([]string, 0, len(recs))
	for _, r := range recs {
		ids = append(ids, r.ID)
	}
	var tasks []TaskPO
	err = q.db.WithContext(ctx).Where("recording_id IN ?", ids).Find(&tasks).Error
	if err != nil {
		return ports.RecordingList{}, fmt.Errorf("批量查询 tasks 失败: %w", err)
	}
	byRecording := make(map[string]TaskPO, len(tasks))
	for _, tk := range tasks {
		byRecording[tk.RecordingID] = tk
	}
	for _, r := range recs {
		tk, ok := byRecording[r.ID]
		if !ok {
			return ports.RecordingList{}, fmt.Errorf("%w: 录音 %s 缺任务", ports.ErrDataInconsistent, r.ID)
		}
		list.Items = append(list.Items, ports.RecordingListItem{
			RecordingID:      r.ID,
			OriginalFilename: r.OriginalFilename,
			SizeBytes:        r.SizeBytes,
			CreatedAt:        r.CreatedAt,
			TaskID:           tk.ID,
			Status:           domain.TaskStatus(tk.Status),
		})
	}
	return list, nil
}

// GetDeleting 取单个删除中录音的清理视图（详设 §5.3）；未标记/已清理 → found=false。
// 删除中缺任务允许继续幂等清理（§4.6），此时 TaskID 为空串。
func (q *RecordingQueryGORM) GetDeleting(ctx context.Context, recordingID string) (ports.DeletingCleanup, bool, error) {
	var rec RecordingPO
	err := q.db.WithContext(ctx).Where("id = ? AND deleting_at IS NOT NULL", recordingID).First(&rec).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return ports.DeletingCleanup{}, false, nil
	}
	if err != nil {
		return ports.DeletingCleanup{}, false, fmt.Errorf("查询删除中录音失败: %w", err)
	}
	return q.deletingOf(rec), true, nil
}

// ListDeleting 全量删除中录音（deleting_at 非空），供低频清理循环扫描（详设 §5.3/§10）。
// 删除中行数量受清理循环收敛约束，逐行补查任务（无需 JOIN）。
func (q *RecordingQueryGORM) ListDeleting(ctx context.Context) ([]ports.DeletingCleanup, error) {
	var recs []RecordingPO
	err := q.db.WithContext(ctx).Where("deleting_at IS NOT NULL").
		Order("deleting_at, id").Find(&recs).Error
	if err != nil {
		return nil, fmt.Errorf("扫描删除中录音失败: %w", err)
	}
	out := make([]ports.DeletingCleanup, 0, len(recs))
	for _, rec := range recs {
		out = append(out, q.deletingOf(rec))
	}
	return out, nil
}

// deletingOf 组装清理视图：任务行缺省字段为零值（TaskID 空串 = 幂等清理，§4.6）。
func (q *RecordingQueryGORM) deletingOf(rec RecordingPO) ports.DeletingCleanup {
	out := ports.DeletingCleanup{
		RecordingID: rec.ID,
		StoragePath: rec.StoragePath,
	}
	var tk TaskPO
	err := q.db.Where("recording_id = ?", rec.ID).First(&tk).Error
	if err != nil {
		return out // 含 NotFound：任务已清理/缺任务，继续幂等清理
	}
	out.TaskID = tk.ID
	out.Status = tk.Status
	out.Attempt = tk.Attempt
	out.EventSeq = tk.EventSeq
	return out
}

// taskView PO → 任务视图；error_code 非空即异步执行错误（详设 §8.4）。
func taskView(po TaskPO) ports.TaskView {
	view := ports.TaskView{
		ID:          po.ID,
		RecordingID: po.RecordingID,
		Status:      domain.TaskStatus(po.Status),
		Attempt:     po.Attempt,
		CreatedAt:   po.CreatedAt,
		StartedAt:   po.StartedAt,
		FinishedAt:  po.FinishedAt,
	}
	if po.ErrorCode != nil {
		view.Error = &ports.TaskError{Code: *po.ErrorCode, Message: po.ErrorMessage}
	}
	return view
}

// summaryFromPO 解析 summary_json：复用领域严格解析（详设 §9），落库内容损坏视为关联异常。
func summaryFromPO(po *TaskPO) (*domain.Summary, error) {
	if po.SummaryJSON == nil {
		return nil, fmt.Errorf("done 任务缺 summary_json")
	}
	s, err := domain.ParseSummary([]byte(*po.SummaryJSON))
	if err != nil {
		return nil, err
	}
	return &s, nil
}
