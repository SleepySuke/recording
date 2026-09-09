# T05：查询接口——任务 / 列表 / 详情

> 依赖：T04 · 预算：0.75h · 状态：**未开始**

**设计依据**：详设 §8.1（端点总表与分页约定）、§8.3（404 码）、§4.6（查询不用 INNER JOIN 静默隐藏损坏关联）；架构 §3 查询段
**测试依据**：IT-16；验收清单「上传与查询」第 5～8 条

## 目标

三个只读接口：任务状态查询（体现当前阶段与错误）、录音分页列表（含每条最新任务状态）、录音详情（done 后含 transcript 与 result）。查询全部走数据库只读路径，隐藏删除中资源。

## 涉及文件

- Create：`internal/application/ports/query.go`（查询端口）
- Create：`internal/application/recording/query.go`（Get/GetTask/List 用例）
- Create：`internal/infrastructure/persistence/mysql/query.go`（查询实现）
- Create：`internal/interfaces/http/dto/query.go`、`handler/query.go`
- Modify：`internal/interfaces/http/router.go`
- Test：`tests/integration/query_test.go`

## 交付接口（后续任务依赖）

```go
// ports/query.go
type QueryService interface { // 或拆为三个方法，命名保持一致即可
    GetTask(ctx context.Context, taskID string) (TaskView, error)          // 不存在/删除中 → 404/30001
    GetRecording(ctx context.Context, id string) (RecordingDetail, error)  // 不存在/删除中 → 404/20005
    ListRecordings(ctx context.Context, page, pageSize int) (RecordingList, error)
}
// TaskView：id、recording_id、status、attempt、error{code,message}（成功/处理中为 null）、created/started/finished_at
// RecordingDetail：TaskView 全部 + original_filename、extension、size_bytes、transcript（可用后非 null）、result（仅 done 非 null）
// RecordingList：items[]（含 task_id 与最新 status）、page、page_size、total
```

分页规则（详设 §8.1）：默认 page=1、page_size=20、上限 100；非法值（≤0、非数字、超上限）→ 400/10001；排序 `created_at DESC, id DESC`；超出总页数返回空数组；所有查询带 `deleting_at IS NULL` 过滤；正常录音缺任务 → 500/90004（不用 JOIN 静默隐藏，§4.6）。

## 步骤

- [ ] 1. 写失败测试（`tests/integration/query_test.go`，预置数据后走 HTTP）：
  - `TestIT16_Pagination`（子测试）：默认 20 / 上限 100 / 非法值 400 / 超总页数空列表 / 同 created_at 并列时按 id 倒序稳定。
  - `TestQuery_NotFound`：不存在任务 404/30001；不存在录音 404/20005；（预置 deleting_at）删除中资源同样 404。
  - `TestQuery_DetailFields`：pending 详情 transcript/result 为 null；手工置为 done 后 transcript 与 result（三个摘要字段）非 null；failed 任务 GET 仍 200，error.code 为落库值。
  - `TestQuery_MissingTaskIsolation`：手工删除 tasks 行制造正常录音缺任务 → 列表/详情返回 500/90004。
- [ ] 2. 运行确认失败：`TEST_MYSQL_DSN=… go test ./tests/integration/ -run 'TestIT16|TestQuery' -v`。
- [ ] 3. 实现：查询端口与 MySQL 实现（只读，不加锁）、三个 handler 与 DTO（时间输出 RFC3339）、路由挂载。
- [ ] 4. 运行确认通过：全绿。
- [ ] 5. 提交：`feat: add task and recording query apis`。

## 完成标准

- [ ] IT-16 与三个补充用例全绿。
- [ ] `GET /v1/tasks/{id}` 在 transcribing / summarizing 时返回对应 status（处理中体现阶段）。
