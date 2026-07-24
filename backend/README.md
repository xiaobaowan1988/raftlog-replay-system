# GitOps Dashboard — Golang Backend

只读配置控制台的后端服务。职责：**同步 Git → Deep Merge（服务 Values + 模板 Values）→ 渲染 Manifests → 以 JSON 提供给前端**，并通过 GitLab Webhook 触发重新同步。

> The backend never writes to the repo. All mutation happens in GitLab; this
> service is a read-through cache with a rendering pipeline.

## 交互链路 (对应 Tech Spec)

```
GitLab push ──webhook──▶ POST /api/webhook/gitlab
                              │
                              ▼
                       store.Rebuild()
              ┌───────────────┼────────────────┐
              ▼               ▼                 ▼
        gitrepo.Sync    scan services/    load templates/
        (clone/pull)    parse YAML        values.yaml (defaults)
              │               │                 │
              └──────────► merge.DeepMerge ◀─────┘
                              │
                              ▼
                       render.Render (text/template) ─▶ K8s manifests
                              │
                              ▼
                    atomic swap in-memory Snapshot
                              │
       React app ◀── GET /api/tree · GET /api/services/{id}
```

## 目录结构

```
backend/
├── cmd/server/main.go          # 入口：配置、初始构建、定时刷新、优雅退出
├── internal/
│   ├── model/                  # 与前端对齐的 JSON 结构体
│   ├── gitrepo/                # git clone/pull + 单文件 commit 查询 (shell out)
│   ├── merge/                  # Deep Merge (含单元测试)
│   ├── render/                 # text/template 渲染 K8s manifests
│   ├── store/                  # 内存快照：扫描/合并/渲染/原子替换
│   └── api/                    # HTTP 路由 + GitLab webhook + CORS
└── testdata/repo/              # 示例 GitOps 仓库 (本地开发用)
    ├── services/*.yaml
    └── templates/<name>/{values.yaml, templates/*.yaml}
```

## HTTP API

| Method | Path | 说明 |
| --- | --- | --- |
| `GET`  | `/api/healthz` | 存活探针 |
| `GET`  | `/api/tree` | 侧边栏：`{ services[], templates[] }` |
| `GET`  | `/api/services/{id}` | 单个服务：元数据 + `{ source, merged, manifest }` |
| `POST` | `/api/webhook/gitlab` | 触发重新同步（校验 `X-Gitlab-Token`） |

`GET /api/services/{id}` 返回结构（与前端 `Content` 完全对齐）：

```json
{
  "id": "service-a",
  "name": "service-a.yaml",
  "template": "standard-web-chart",
  "commit": "8f3a2b1",
  "time": "10 minutes ago",
  "content": {
    "source":   "name: service-a\n...",
    "merged":   "# Auto-merged ...\nname: service-a\n...",
    "manifest": "apiVersion: apps/v1\nkind: Deployment\n..."
  }
}
```

## 配置 (环境变量)

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `GITOPS_REPO_URL` | *(空)* | 远端仓库地址；留空则进入**本地模式**，直接使用 `REPO_DIR` |
| `GITOPS_REPO_BRANCH` | `main` | 跟踪的分支 |
| `GITOPS_REPO_DIR` | `./data/repo` | 磁盘上的工作目录 |
| `GITOPS_WEBHOOK_SECRET` | *(空)* | GitLab Webhook 的 `Secret Token`；留空则不校验 |
| `GITOPS_LISTEN` | `:8080` | 监听地址 |
| `GITOPS_REFRESH_INTERVAL` | `5m` | 兜底定时同步；`0` 关闭 |
| `GITOPS_CORS_ORIGIN` | `http://localhost:5173` | 允许的前端来源 |

## 本地运行

无需真实仓库，直接跑内置示例数据（本地模式）：

```bash
cd backend
GITOPS_REPO_DIR=./testdata/repo GITOPS_REFRESH_INTERVAL=0 go run ./cmd/server

# 另开终端
curl -s localhost:8080/api/tree | jq
curl -s localhost:8080/api/services/service-a | jq -r .content.merged
```

对接真实 GitLab 仓库：

```bash
GITOPS_REPO_URL=https://gitlab.example.com/infra/gitops-config.git \
GITOPS_REPO_BRANCH=main \
GITOPS_REPO_DIR=/var/lib/gitops/repo \
GITOPS_WEBHOOK_SECRET=$(cat /run/secrets/webhook) \
go run ./cmd/server
```

> 私有仓库鉴权：使用带 token 的 URL
> (`https://oauth2:<token>@gitlab.example.com/...`) 或在运行环境中配置 SSH /
> git credential helper —— 后端直接调用系统 `git`，沿用其凭证机制。

## GitLab Webhook 配置

在 GitLab 项目 **Settings → Webhooks**：
- **URL**：`https://<backend-host>/api/webhook/gitlab`
- **Secret Token**：与 `GITOPS_WEBHOOK_SECRET` 一致
- **Trigger**：勾选 *Push events*（可限定分支）

后端收到后立即返回 `202 Accepted`，在后台执行 `git fetch + reset + 重建快照`。

## 测试 / 构建

```bash
go test ./...        # 含 Deep Merge 单元测试
go vet ./...
go build -o bin/server ./cmd/server
```

## 前端对接

前端当前使用 `src/mockData.js` 占位。对接本后端时，把数据来源换成两次请求即可，
结构无需改动：

```js
// src/api.js
const BASE = import.meta.env.VITE_API_BASE ?? 'http://localhost:8080';

export const fetchTree = () =>
  fetch(`${BASE}/api/tree`).then((r) => r.json());

export const fetchService = (id) =>
  fetch(`${BASE}/api/services/${id}`).then((r) => r.json());
// -> { ...meta, content: { source, merged, manifest } }
```

App 中：进入时 `fetchTree()` 渲染侧边栏；切换服务时 `fetchService(id)`，
把 `detail.content[activeTab]` 交给 Monaco。这样每个服务/每个 tab 都会展示各自的真实内容
（修复了 mock 阶段“内容不随服务变化”的限制）。
