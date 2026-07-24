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
│   ├── render/                 # text/template 渲染 CR (含 toYaml/indent 助手)
│   ├── store/                  # 内存快照：扫描/合并/渲染/原子替换
│   └── api/                    # HTTP 路由 + GitLab webhook + CORS
└── testdata/repo/              # 示例 GitOps 仓库 (本地开发用)
    ├── services/*.yaml
    └── templates/<name>/{values.yaml, templates/*.yaml}
```

## 领域模型 (Flink Kubernetes Operator)

本示例围绕 **Flink Kubernetes Operator** 建模：

- **template = FlinkDeployment CR 模板**：`templates/<name>/` 下的 `values.yaml`
  是该模板的默认 `spec`，`templates/flinkdeployment.yaml` 是 CR 骨架
  (`apiVersion: flink.apache.org/v1beta1`, `kind: FlinkDeployment`)。
- **service = 具体的 FlinkDeployment**：`services/<name>.yaml` 引用某个模板，
  只写需要覆盖的 `spec` 字段（镜像、并行度、TM 资源、flinkConfiguration 等）。

三个视图对应：**Source** = 提交到 Git 的服务覆盖文件；**Merged** = 与模板默认值
Deep Merge 后生效的完整 spec；**Manifest** = 最终下发给 Operator 的 FlinkDeployment CR。

渲染骨架用 Helm 风格的 `toYaml` / `indent` 助手把合并后的 spec 原样嵌入 CR，
从而天然支持 `flinkConfiguration` 这类自由格式的 map。

## HTTP API

| Method | Path | 说明 |
| --- | --- | --- |
| `GET`  | `/api/healthz` | 存活探针 |
| `GET`  | `/api/tree` | 侧边栏：`{ services[], templates[] }` |
| `GET`  | `/api/services/{id}` | 单个服务：元数据 + `{ source, merged, manifest }` |
| `POST` | `/api/services/{id}/deploy` | **写路径**：把渲染出的 FlinkDeployment CR 用 SSA 下发到集群（`?dryRun=true` 只校验/准入不落库） |
| `POST` | `/api/webhook/gitlab` | 触发重新同步（校验 `X-Gitlab-Token`） |

`GET /api/services/{id}` 返回结构（与前端 `Content` 完全对齐）：

```json
{
  "id": "fraud-detection",
  "name": "fraud-detection.yaml",
  "template": "flink-stateful-ha",
  "commit": "8f3a2b1",
  "time": "10 minutes ago",
  "content": {
    "source":   "name: fraud-detection\n...",
    "merged":   "# Auto-merged ...\nname: fraud-detection\n...",
    "manifest": "apiVersion: flink.apache.org/v1beta1\nkind: FlinkDeployment\n..."
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
| `GITOPS_KUBECONFIG` | *(空)* | 部署用 kubeconfig 路径；留空则先试 in-cluster、再回退默认 kubeconfig |
| `GITOPS_DEPLOY_NAMESPACE` | `default` | FlinkDeployment 下发到的命名空间 |

## 本地运行

无需真实仓库，直接跑内置示例数据（本地模式）：

```bash
cd backend
GITOPS_REPO_DIR=./testdata/repo GITOPS_REFRESH_INTERVAL=0 go run ./cmd/server

# 另开终端
curl -s localhost:8080/api/tree | jq
curl -s localhost:8080/api/services/fraud-detection | jq -r .content.manifest
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

## 部署 (Run) — 唯一的写路径

前端点 **Run / 部署** → `POST /api/services/{id}/deploy`。后端取该服务当前渲染出的
FlinkDeployment CR，用 **client-go 动态客户端 + Server-Side Apply** 下发到集群：

- field manager 固定为 `gitops-dashboard`，重复 apply 幂等、冲突可归因；
- 命名空间取 CR 自身 `metadata.namespace`，缺省则用 `GITOPS_DEPLOY_NAMESPACE`；
- 下发前先跑 `deploy.Validate`（结构 + FlinkDeployment 必填字段校验）；
- `?dryRun=true` 让 apiserver 只校验/准入、不落库。

集群凭证按 client-go 惯例解析：先 in-cluster，再 `GITOPS_KUBECONFIG` / `$KUBECONFIG`
/ `~/.kube/config`。**若都不可达，后端照常提供只读 API，deploy 端点返回 `503` 并说明未配置**
（读服务不受影响）。

```bash
# 指向你的集群后启用部署
GITOPS_REPO_DIR=./testdata/repo \
GITOPS_KUBECONFIG=$HOME/.kube/config \
GITOPS_DEPLOY_NAMESPACE=flink \
go run ./cmd/server

curl -s -X POST localhost:8080/api/services/fraud-detection/deploy | jq         # 真实 apply
curl -s -X POST "localhost:8080/api/services/fraud-detection/deploy?dryRun=true" # 仅校验/准入
```

## CR 校验 (无需真集群)

两层校验保证 “Run 会下发的东西” 是合法可部署的 FlinkDeployment：

1. **内置 Go 校验器**（`internal/deploy`）：渲染每个服务并断言 GVK、`metadata.name`、
   `spec` 及 Flink 必填字段（image / flinkVersion / jobManager|taskManager.resource /
   application 模式的 job.jarURI）。随 `go test` 运行，不依赖集群。
2. **对照真实 CRD schema**（`hack/validate-crs.sh`）：拉取 Apache Flink Kubernetes
   Operator 的 FlinkDeployment CRD，转成 JSON schema，用 `kubeconform` 校验渲染结果。

```bash
go test ./internal/deploy/           # 层 1：内置校验器
go install github.com/yannh/kubeconform/cmd/kubeconform@latest
bash hack/validate-crs.sh            # 层 2：对照真实 Flink CRD schema
```

## 测试 / 构建

```bash
go test ./...        # Deep Merge 单测 + FlinkDeployment CR 校验
go vet ./...
go build -o bin/server ./cmd/server
```

## 前端对接

前端（仓库根目录）已接入本后端 —— 见根 `README.md` 的“对接后端”。核心 3 个调用在
`src/api.js`：`fetchTree()`、`fetchService(id)`、`deployService(id, { dryRun })`。
开发模式下 Vite 代理把 `/api` 转发到 `localhost:8080`，浏览器发同源请求、无需 CORS。

App 中：进入时 `fetchTree()` 渲染侧边栏；切换服务时 `fetchService(id)`，
把 `detail.content[activeTab]` 交给 Monaco。这样每个服务/每个 tab 都会展示各自的真实内容
（修复了 mock 阶段“内容不随服务变化”的限制）。
