# T04：上传接口与本地文件存储

> 依赖：T01、T02、T03 · 预算：1.5h · 状态：**未开始**

**设计依据**：详设 §5.1（上传判定树与细则）、§5.2（提交结果未知）、§2.4/§2.5（事务端口与上传链）、§4.6（成对创建）、§7.3（事件原子性）、§7.4（事件镜像）；架构 §3（事务①）
**测试依据**：IT-01、IT-13、IT-15；验收清单「上传与查询」第 2～4 条

## 目标

完整上传链路：multipart 流式读取（SHA-256 + 字节计数 + tmp- 临时文件 + rename）、事务①成对创建三表记录、202 返回。每个失败分支有清理动作，不留残留文件或半条记录。

## 涉及文件

- Create：`internal/application/ports/filestore.go`、`recordingtx.go`
- Create：`internal/application/recording/upload.go`（Upload 用例）
- Create：`internal/infrastructure/filestore/local/store.go`
- Create：`internal/infrastructure/persistence/mysql/recording_tx.go`（事务①实现）
- Create：`internal/interfaces/http/dto/upload.go`、`handler/upload.go`
- Create：`internal/infrastructure/logging/mirror.go`（事件镜像，详设 §7.4）
- Modify：`internal/interfaces/http/router.go`（挂 POST /v1/recordings）
- Test：`internal/infrastructure/filestore/local/store_test.go`；`tests/integration/upload_test.go`

## 交付接口（后续任务依赖）

```go
// ports/filestore.go
type StoredFile struct{ StoragePath string; SizeBytes int64; ContentHash string }
type FileStore interface {
    Save(ctx context.Context, src io.Reader, ext string) (StoredFile, error) // 磁盘预检→tmp- 写入（流式 SHA-256 + 计数）→rename；失败自清理
    Delete(ctx context.Context, storagePath string) error                    // 不存在视为成功
}

// ports/recordingtx.go
type CreateInput struct{ Recording domain.Recording; Task domain.ProcessingTask; Event domain.TaskEvent }
type RecordingTx interface {
    CreateWithTask(ctx context.Context, in CreateInput) error // 事务①：INSERT recordings + tasks(pending) + task_created，任一失败整体回滚
}

// application/recording
type UploadService struct{ … } // 依赖 FileStore、RecordingTx、*slog.Logger
func (s *UploadService) Upload(ctx context.Context, r UploadRequest) (UploadResult, error)
// UploadRequest{ FilePart io.Reader; Filename string }
// UploadResult{ RecordingID, TaskID string; Status domain.TaskStatus }
// 错误映射：缺 file→20001、空文件→20002、扩展名→20003、超限→20004、落盘→90003、DB 结果未知→90002（均包成 *errorcode.AppError）
```

## 步骤

- [ ] 1. 通读设计：详设 §5.1 判定树逐分支 + §5.2 + §7.3；架构 §3 事务①。
- [ ] 2. 写失败测试（单元，filestore）：`TestFileStore_SaveHashAndSize`（已知字节流 → SHA-256 与 size 正确，即 IT-13 的核心断言）；`TestFileStore_SaveCleansTmpOnFailure`（写入中途读错误 → 无 tmp- 残留）。
- [ ] 3. 写失败测试（集成，`tests/integration/upload_test.go`，经 httptest 走完整 HTTP 链）：
  - `TestIT01_UploadCreatesTripleInOneTx`：上传成功后同事务可见 recordings/tasks/task_created 三行。
  - `TestIT13_ContentHashCorrect`：已知内容的 content_hash 等于预计算 SHA-256。
  - `TestIT15_UploadBoundaries`（子测试）：恰好 50MiB 通过；超 1 字节 413/20004 且无文件与记录残留；空文件 400/20002；无后缀 400/20003；双后缀按最后后缀判定；同名多余 file 部分取第一个、多余部分忽略。
- [ ] 4. 运行确认失败：`TEST_MYSQL_DSN=… go test ./tests/integration/ -run 'TestIT01|TestIT13|TestIT15' -v` 及 `go test ./internal/infrastructure/filestore/... -v`。
- [ ] 5. 实现要点（对照 §5.1 判定树逐分支，不遗漏清理）：
  - multipart 用 `MultipartReader()` 流式处理，不 `ParseMultipartForm` 进内存；找到第一个名为 `file` 的文件部分；额外部分忽略并 WARN。
  - 读取上限 = 50MiB + 1 字节（流式计数，不信任 Content-Length）；请求体总上限另设并预留 multipart 开销；读超时与总超时分开配置。
  - 写入前磁盘空间预检，不足 503/90003；写中途失败清理 tmp 并 503/90003。
  - rename 后进入事务①；明确回滚→删文件再返回错误；提交结果未知→保留文件返回 503/90002（§5.2，保守不删）。
  - event_id/recording_id/task_id 应用预生成（UUID）；事件与状态同事务；提交后 `logging.MirrorEvent` 镜像（尽力而为）。
- [ ] 6. 运行确认通过：上述命令全绿。
- [ ] 7. 提交：`feat: implement recording upload with local file store`。

## 完成标准

- [ ] IT-01/13/15 全绿；失败路径（413/400/503）后数据目录与三表无残留。
- [ ] 手动 curl 上传一个小 wav：202 + `{recording_id, task_id, status:"pending"}`，`logs/app.jsonl` 出现 task_created 镜像。
