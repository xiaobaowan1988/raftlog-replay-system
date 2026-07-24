// ==========================================
// 模拟数据 (真实场景由 Golang 后端 API 传入)
// ==========================================
// In production this payload is served by the Golang backend, which pulls the
// latest files from GitLab and performs the in-memory Deep Merge
// (service Values + template Values) before exposing the structured config.
export const MOCK_DATA = {
  services: [
    { id: 'service-a', name: 'service-a.yaml', template: 'standard-web-chart', commit: '8f3a2b1', time: '10 mins ago' },
    { id: 'service-b', name: 'service-b.yaml', template: 'python-worker', commit: '2c9e4d5', time: '1 hour ago' },
    { id: 'payment-gateway', name: 'payment-gateway.yaml', template: 'standard-web-chart', commit: '1a2b3c4', time: '2 days ago' },
  ],
  templates: [
    { id: 'standard-web-chart', name: 'standard-web-chart' },
    { id: 'python-worker', name: 'python-worker' },
  ],
  yamlContent: {
    source: `name: service-a\nenv: production\ntemplate_ref:\n  path: "templates/standard-web-chart"\nvalues:\n  replicaCount: 3\n  image:\n    repository: "my-registry/service-a"\n    tag: "v2.0.1"`,

    merged: `# 系统自动合并了 [standard-web-chart] 的默认参数\nname: service-a\nenv: production\ntemplate_ref:\n  path: "templates/standard-web-chart"\nvalues:\n  replicaCount: 3\n  image:\n    repository: "my-registry/service-a"\n    tag: "v2.0.1"\n  port: 8080            # 继承自模板\n  resources:            # 继承自模板\n    requests:\n      cpu: 100m\n      memory: 128Mi\n    limits:\n      cpu: 500m\n      memory: 512Mi`,

    manifest: `---\n# 渲染结果预览 (Source: standard-web-chart/templates/deployment.yaml)\napiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: service-a\n  labels:\n    app: service-a\n    environment: production\nspec:\n  replicas: 3\n  selector:\n    matchLabels:\n      app: service-a\n  template:\n    metadata:\n      labels:\n        app: service-a\n    spec:\n      containers:\n        - name: service-a\n          image: "my-registry/service-a:v2.0.1"\n          ports:\n            - containerPort: 8080\n          resources:\n            requests:\n              cpu: 100m\n              memory: 128Mi\n            limits:\n              cpu: 500m\n              memory: 512Mi`,
  },
};

// ==========================================
// 离线回退适配器 (Offline fallback adapters)
// ==========================================
// These mirror the backend's response shape so the UI degrades gracefully to
// bundled sample data when the API is unreachable (e.g. `npm run dev` with no
// backend running). In this mode every service shares the same yamlContent.

export function mockTree() {
  return { services: MOCK_DATA.services, templates: MOCK_DATA.templates };
}

export function mockServiceDetail(id) {
  const meta = MOCK_DATA.services.find((s) => s.id === id) ?? MOCK_DATA.services[0];
  return { ...meta, content: MOCK_DATA.yamlContent };
}
