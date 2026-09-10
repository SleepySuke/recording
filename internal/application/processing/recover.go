// recover.go 启动恢复用例（详设 §6.1 崩溃窗口、§6.2 七步顺序、§6.3 12h 降级变体、
// §4.6 关联巡检、§5.2 孤儿文件核对）：数据库状态是唯一事实，恢复只依据已提交状态；
// 在单实例独占窗口（无 worker、无上传）执行，§6.2 步骤 1/2（健康检查/迁移）与
// 步骤 7（启动 worker 与 HTTP、置 ready）由 bootstrap 装配编排。
package processing

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"recording-transcription/internal/application/errorcode"
	"recording-transcription/internal/application/ports"
)

// DeletionResumer 删除恢复的最小接口（详设 §6.2 步骤 4 = §5.3 未完成删除续做）：
// 与低频清理循环同语义（扫描 deleting_at → 删文件（不存在视为成功）→ 三表清理；
// 失败记日志保留标记），由 *recording.DeleteService 实现、启动时直接复用。
type DeletionResumer interface {
	CleanupPending(ctx context.Context)
}

// Recoverer 启动恢复用例对外接口（T11 交付接口，按 §6.2 顺序调用）。
type Recoverer interface {
	// Inspect 关联巡检：LEFT JOIN 孤立任务 / 非删除中缺任务录音 → 90004，
	// 不静默修复；调用方据此阻止 ready。
	Inspect(ctx context.Context) error
	// ResumeDeletions deleting_at 非空 → 删文件 → 三表清理；失败记日志不阻塞。
	ResumeDeletions(ctx context.Context) error
	// ResetInFlight 单事务批量：transcribing/summarizing → pending、attempt+1、
	// 清空产物/错误/时间 + task_recovered（reset 模式，详设 §6.2 步骤 5），或 →
	// failed/30003 + task_interrupted（interrupt 模式，详设 §6.3）。失败整体回滚。
	ResetInFlight(ctx context.Context) (int, error)
	// RemoveOrphanFiles tmp- 直接删；数据目录内未被 storage_path 引用的删除；
	// 引用集查询失败不清理（详设 §5.2）。
	RemoveOrphanFiles(ctx context.Context) (int, error)
}

// RecoverService 启动恢复用例；interrupt 由 RECOVERY_MODE=interrupt 决定（详设 §6.3，
// 两种模式互斥、启动时定型）。
type RecoverService struct {
	tx        ports.RecoveryTx
	deletions DeletionResumer
	store     ports.FileStore
	logger    *slog.Logger
	interrupt bool
}

// 编译期保证 RecoverService 实现 Recoverer。
var _ Recoverer = (*RecoverService)(nil)

// NewRecoverService 构造恢复用例；deletions 复用删除用例的清理链（§5.3 语义）。
func NewRecoverService(tx ports.RecoveryTx, deletions DeletionResumer, store ports.FileStore,
	logger *slog.Logger, interrupt bool) *RecoverService {
	return &RecoverService{tx: tx, deletions: deletions, store: store, logger: logger, interrupt: interrupt}
}

// Inspect 关联巡检（详设 §6.2 步骤 3、§4.6）：异常记录数字码 90004（slog.Int）并返回
// 错误——调用方阻止 ready、不启动 worker，等运维介入；本用例不修复数据。
func (s *RecoverService) Inspect(ctx context.Context) error {
	if err := s.tx.InspectIntegrity(ctx); err != nil {
		s.logger.Error("关联巡检未通过，阻止就绪（不静默修复）",
			slog.Int("code", int(errorcode.CodeDataInconsistent)),
			slog.Any("err", err))
		return err
	}
	return nil
}

// ResumeDeletions 恢复未完成删除（详设 §6.2 步骤 4）：与低频清理循环完全同语义，
// 直接复用删除用例；文件/清理失败已在其中记日志并保留标记（§5.3），不阻塞任务恢复
// ——留给低频清理或重复 DELETE 续做，故恒返回 nil。
func (s *RecoverService) ResumeDeletions(ctx context.Context) error {
	s.deletions.CleanupPending(ctx)
	return nil
}

// ResetInFlight 批量重置在途任务（详设 §6.2 步骤 5 / §6.3）。事务失败整体回滚，
// 调用方（bootstrap）据此阻止启动。
func (s *RecoverService) ResetInFlight(ctx context.Context) (int, error) {
	n, err := s.tx.ResetInFlight(ctx, s.interrupt)
	if err != nil {
		return 0, err
	}
	if n > 0 {
		mode := "reset（pending + attempt+1 + task_recovered）"
		if s.interrupt {
			mode = "interrupt（failed/30003 + task_interrupted）"
		}
		s.logger.Info("在途任务已重置", slog.Int("count", n), slog.String("mode", mode))
	}
	return n, nil
}

// RemoveOrphanFiles 孤儿文件核对（详设 §6.2 步骤 6、§5.2）：tmp- 前缀直接删（不可能
// 被引用，不参与核对）；其余与 storage_path 引用集比对，删除确定未引用者。引用集或
// 目录列举失败 → 返回错误且不执行任何清理；单个删除失败记日志继续（最坏残留文件，
// 不阻塞启动）。范围天然限定服务数据目录（FileStore 只看自己的目录）。
func (s *RecoverService) RemoveOrphanFiles(ctx context.Context) (int, error) {
	stored, err := s.store.ListStored(ctx)
	if err != nil {
		return 0, fmt.Errorf("列举数据目录失败，不执行孤儿清理: %w", err)
	}
	refs, err := s.tx.ListStoragePaths(ctx)
	if err != nil {
		return 0, fmt.Errorf("查询引用集失败，不执行孤儿清理（详设 §5.2）: %w", err)
	}
	refSet := make(map[string]bool, len(refs))
	for _, p := range refs {
		refSet[p] = true
	}

	removed := 0
	for _, name := range stored {
		if !strings.HasPrefix(name, "tmp-") && refSet[name] {
			continue
		}
		if err := s.store.Delete(ctx, name); err != nil {
			s.logger.Warn("孤儿文件删除失败，保留待下次核对",
				slog.String("file", name), slog.Any("err", err))
			continue
		}
		removed++
	}
	if removed > 0 {
		s.logger.Info("孤儿文件核对完成", slog.Int("removed", removed),
			slog.Int("stored", len(stored)), slog.Int("referenced", len(refSet)))
	}
	return removed, nil
}
