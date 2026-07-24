# GitOps Read-Only Configuration Dashboard

一个基于 **"Git-as-a-Database"** 架构的配置可视化控制台。系统的修改入口已完全移交至
GitLab（人工或 Code Agent 提交），因此本前端页面的核心目标是：**实时状态监控、配置合并链路追踪、以及修改审计**。

> A read-only visualization console for a Git-as-a-Database configuration system.
> All writes happen in GitLab; this UI never mutates state.

## 核心设计原则 (Design Principles)

- **配置只读 (Read-Only Config)：** 配置的唯一修改入口是 GitLab；前端不提供任何编辑/保存表单，Monaco Editor 强制 `readOnly: true`。
- **真相统一 (Single Source of Truth)：** 页面展示的数据 100% 反映远端 Git 仓库及后端内存的当前状态。
- **开发体验 (Developer Experience)：** 采用类似 VS Code 的沉浸式代码阅读体验 (`vs-dark`)，降低 YAML 阅读疲劳。

> **关于 “Run / 部署”：** 头部的 Run 按钮是一个显式的**下发动作**，把当前服务渲染出的
> FlinkDeployment CR 通过后端 Server-Side Apply 到 K8s 集群。它**不修改 Git 配置**
> （配置仍只在 GitLab 改），只把已合并的期望状态下发到集群 —— 属于“只读配置 + 显式部署”，
> 而非早期纯浏览版的绝对 Zero-Write。详见 `backend/README.md`。

## 系统交互链路 (Architecture Flow)

1. **修改发生：** Code Agent 或工程师向 GitLab 提交 `service-a.yaml` 的修改。
2. **状态同步：** GitLab 触发 Webhook，Golang 后端重新拉取最新文件，在内存中执行 Deep Merge（服务 Values + 模板 Values）。
3. **前端拉取：** 前端从后端获取最新的结构化配置数据。
4. **视图渲染：** 前端 Monaco Editor 实时刷新 YAML 视图。

## UI 布局 (Layout)

经典 IDE 布局 `左侧导航树 + 右侧工作区`，深色主题。

- **左侧资源导航树：** 纯前端模糊搜索，映射 GitLab 的 `services/`（可选）与 `templates/`（只读参考）目录。
- **右侧元数据状态栏：** 服务名称、模板继承关系、Git 追踪信息（Commit Hash / 时间）。
- **右侧核心阅读器 (三视角)：**
  1. **原始配置 (Source)** — 审计 Code Agent 究竟向 Git 写入了什么。
  2. **完整参数 (Merged Values)** — 该服务当前生效的所有参数（含模板继承）。
  3. **底层清单 (Manifests Preview)** — 最终下发到 K8s 集群的 YAML。

## 技术选型 (Tech Stack)

| 层面 | 选型 |
| --- | --- |
| UI 框架 | React 18 (Hooks) |
| 样式 | Tailwind CSS 3 |
| 编辑器 | `@monaco-editor/react` (`readOnly: true`) |
| 图标 | `lucide-react` |
| 构建工具 | Vite 5 |

## 快速开始 (Getting Started)

```bash
# 安装依赖
npm install

# 本地开发 (http://localhost:5173)
npm run dev

# 生产构建
npm run build

# 预览构建产物
npm run preview
```

## 项目结构 (Structure)

```
.
├── index.html            # Vite 入口
├── vite.config.js        # Vite 配置
├── tailwind.config.js    # Tailwind 配置
├── postcss.config.js     # PostCSS (tailwind + autoprefixer)
└── src/
    ├── main.jsx          # React 挂载入口
    ├── App.jsx           # 主界面 (导航树 + 元数据栏 + 多标签阅读器)
    ├── api.js            # 后端 API 客户端 (fetchTree / fetchService)
    ├── mockData.js       # 离线回退数据 (后端不可达时使用)
    ├── monacoSetup.js    # 本地自托管 Monaco (无需 CDN)
    └── index.css         # Tailwind 指令 + 全局样式
```

## 对接后端 (Wiring to the Backend)

前端已接入 Golang 后端（见 `backend/`）：

- 启动时 `GET /api/tree` 渲染侧边栏；
- 切换服务时 `GET /api/services/{id}`，把 `content[activeTab]`
  (`source` / `merged` / `manifest`) 交给 Monaco。

**开发模式**：`vite.config.js` 已配置代理，把 `/api` 转发到
`http://localhost:8080`（可用 `VITE_PROXY_TARGET` 覆盖），因此浏览器发的是
同源请求、无需处理 CORS。两个服务分别启动：

```bash
# 终端 1 — 后端 (示例数据)
cd backend && GITOPS_REPO_DIR=./testdata/repo GITOPS_REFRESH_INTERVAL=0 go run ./cmd/server

# 终端 2 — 前端
npm run dev        # http://localhost:5173
```

**生产/其它地址**：用 `VITE_API_BASE` 指定后端绝对地址（此时依赖后端的 CORS 配置）：

```bash
VITE_API_BASE=https://gitops-api.internal npm run build
```

**离线回退**：当后端不可达时，前端自动回退到 `src/mockData.js` 的内置示例数据，
并在顶部显示提示条 —— 保证 `npm run dev` 单独也能跑起来看 UI。
