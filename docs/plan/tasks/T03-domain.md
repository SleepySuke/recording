# T03：领域层——状态机与值对象

> 依赖：T01（仅需 errorcode 类型，不依赖 DB） · 预算：1h · 状态：**未开始**

**设计依据**：详设 §2.1（聚合与统一语言）、§4.1（状态转换矩阵）、§4.4（attempt 隔离）、§9（Summary 校验规则）、§5.1（扩展名与文件名净化）；架构 §5（状态机）
**测试依据**：UT-01～UT-07、UT-10

## 目标

纯函数领域层：任务状态机（11 条出边）、Summary 值对象、扩展名解析与文件名净化、领域错误。无任何外部依赖（不 import gin/gorm/slog），全部行为先测后写。

## 涉及文件

- Create：`internal/domain/recording/task.go`（状态、轮次、转换、event_seq 分配）
- Create：`internal/domain/recording/recording.go`（聚合根 + 扩展名解析 + 文件名净化）
- Create：`internal/domain/recording/summary.go`（值对象 + 解析校验）
- Create：`internal/domain/recording/event.go`（事件枚举与值描述）
- Create：`internal/domain/recording/errors.go`（领域错误）
- Test：同目录 `task_test.go`、`recording_test.go`、`summary_test.go`（用例编号写进测试名）

## 交付接口（后续任务依赖）

```go
type TaskStatus string // pending / transcribing / summarizing / done / failed
const ( StatusPending, StatusTranscribing, StatusSummarizing, StatusDone, StatusFailed TaskStatus = … )

// task.go
type ProcessingTask struct{ /* id, recording_id, status, attempt, event_seq, transcript, summary, error_code, error_message, 时间字段 */ }
func (t *ProcessingTask) CanTransition(to TaskStatus) bool        // §4.1 矩阵 11 条出边 + 拒绝其余
func (t *ProcessingTask) Transition(attempt int, to TaskStatus) error // 校验 attempt 匹配 + 合法出边；失败返回领域错误
func (t *ProcessingTask) AllocateEventSeq() int64                 // 行锁内调用，自增并返回；UT-10
type ExecutionKey struct{ TaskID string; Attempt int }

// recording.go
func ParseExtension(filename string) (string, error)  // 最后一个点后缀转小写，白名单 wav/mp3/m4a/aac
func SanitizeFilename(name string) string             // 限长 255 字节、去控制字符、非法 UTF-8 替换
type Recording struct{ /* id, original_filename, storage_path, extension, size_bytes, content_hash, deleting_at, created_at, updated_at */ }

// summary.go
type Summary struct{ Summary string; KeyPoints []string; Todos []string }
func ParseSummary(data []byte) (Summary, error) // §9：拒绝 null/缺字段/类型错误/空串元素/围栏/多 JSON 值；允许空数组

// errors.go
var ( ErrInvalidTransition, ErrAttemptMismatch, ErrSummaryInvalid, ErrUnsupportedExtension, … )

// event.go
type TaskEvent struct{ /* 详设 §7.2 字段的领域值描述 */ }
type EventKind string // task_created / task_claimed / transcription_completed / task_completed /
                      // task_failed / task_retry_accepted / task_recovered / task_interrupted /
                      // task_delete_requested / task_deleted
```

转换矩阵（CanTransition 的唯一依据，详设 §4.1）：pending→{transcribing, failed}；transcribing→{summarizing, failed, pending}（pending 仅启动恢复基线）；summarizing→{done, failed, pending}（同上）；failed→{pending}（手动重试）；其余全部拒绝。降级恢复 transcribing/summarizing→failed 由 RECOVERY_MODE 分支单独表达（`TransitionForRecovery(to, interrupt bool)` 或等价形式）。

## 步骤

- [ ] 1. 写失败测试（矩阵）：`TestUT01_LegalTransitions`（11 条出边逐条执行，from/to/attempt 正确）；`TestUT02_IllegalTransitions`（done→pending、pending→done、done→failed、failed→summarizing 等返回 ErrInvalidTransition 且状态不变）。
- [ ] 2. 写失败测试（attempt/序号）：`TestUT03_AttemptIsolation`（旧 attempt N 对新 attempt N+1 的转换请求被拒）；`TestUT10_EventSeqAllocation`（模拟行锁（sync.Mutex）下并发 100 次分配，无重复无跳号）。
- [ ] 3. 写失败测试（Summary）：`TestUT04_ValidSummary`（非空 summary + 空 key_points/todos 通过）；`TestUT05_InvalidSummary`（缺字段、null、元素空串、非字符串、Markdown 围栏、多个 JSON 值全部拒绝，且"缺字段"与"合法空数组"错误可区分）。
- [ ] 4. 写失败测试（上传规则）：`TestUT06_ExtensionParsing`（`A.WAV`→wav、`a.mp3.exe`→拒、`a.wav.mp3`→mp3、无后缀→拒、`.wav` 仅点号→拒）；`TestUT07_FilenameSanitize`（超 255 字节截断、控制字符去除、非法 UTF-8 替换）。
- [ ] 5. 运行确认失败：`go test ./internal/domain/... -v`。
- [ ] 6. 实现五个文件（无外部依赖；错误消息不含内部细节）。
- [ ] 7. 运行确认通过：`go test ./internal/domain/... -race -v` 全绿。
- [ ] 8. 提交：`feat: implement domain model and state machine`。

## 完成标准

- [ ] UT-01～UT-07、UT-10 全绿；`-race` 通过。
- [ ] `internal/domain` 不 import 任何 gin/gorm/slog（`go list -deps` 或直接检查 import）。
