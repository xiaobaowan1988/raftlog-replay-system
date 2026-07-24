// ==========================================
// 离线回退数据 (Offline fallback data)
// ==========================================
// Sample FlinkDeployment data mirroring the backend's response shape, used
// when the API is unreachable (e.g. `npm run dev` with no backend running).
// Generated from the backend's own output so the offline view is faithful.
//   templates/ = FlinkDeployment CR skeletons (default spec)
//   services/  = concrete FlinkDeployments that inherit a template and override its spec

export const MOCK_DATA = {
  services: [
    {"id": "clickstream-etl", "name": "clickstream-etl.yaml", "template": "flink-streaming-standard", "commit": "a1b2c3d", "time": "8 minutes ago"},
    {"id": "fraud-detection", "name": "fraud-detection.yaml", "template": "flink-stateful-ha", "commit": "e4f5a6b", "time": "2 hours ago"},
    {"id": "session-metrics", "name": "session-metrics.yaml", "template": "flink-streaming-standard", "commit": "c7d8e9f", "time": "1 day ago"},
  ],
  templates: [
    {"id": "flink-stateful-ha", "name": "flink-stateful-ha"},
    {"id": "flink-streaming-standard", "name": "flink-streaming-standard"},
  ],
  // Per-service content keyed by id: { source, merged, manifest }.
  content: {
    "clickstream-etl": {
      source: "name: clickstream-etl\nenv: production\ntemplate_ref:\n  path: \"templates/flink-streaming-standard\"\nvalues:\n  image: \"my-registry/flink-jobs/clickstream-etl:1.4.2\"\n  job:\n    jarURI: \"local:///opt/flink/usrlib/clickstream-etl.jar\"\n    entryClass: \"com.example.ClickstreamJob\"\n    parallelism: 4\n    args:\n      - \"--source.topic\"\n      - \"clickstream\"\n      - \"--sink\"\n      - \"iceberg\"\n  taskManager:\n    resource:\n      memory: \"4096m\"\n",
      merged: "# Auto-merged with defaults from template [flink-streaming-standard]\nname: clickstream-etl\nenv: production\ntemplate_ref:\n  path: templates/flink-streaming-standard\nvalues:\n  flinkConfiguration:\n    execution.checkpointing.interval: \"60000\"\n    execution.checkpointing.mode: EXACTLY_ONCE\n    state.backend: hashmap\n    taskmanager.numberOfTaskSlots: \"2\"\n  flinkVersion: v1_18\n  image: my-registry/flink-jobs/clickstream-etl:1.4.2\n  job:\n    args:\n      - --source.topic\n      - clickstream\n      - --sink\n      - iceberg\n    entryClass: com.example.ClickstreamJob\n    jarURI: local:///opt/flink/usrlib/clickstream-etl.jar\n    parallelism: 4\n    state: running\n    upgradeMode: stateless\n  jobManager:\n    replicas: 1\n    resource:\n      cpu: 1\n      memory: 2048m\n  serviceAccount: flink\n  taskManager:\n    resource:\n      cpu: 1\n      memory: 4096m\n",
      manifest: "# 渲染结果预览 (FlinkDeployment CR — applied to the Flink Kubernetes Operator)\napiVersion: flink.apache.org/v1beta1\nkind: FlinkDeployment\nmetadata:\n  name: clickstream-etl\n  labels:\n    app.kubernetes.io/name: clickstream-etl\n    app.kubernetes.io/managed-by: gitops\n    environment: production\nspec:\n  flinkConfiguration:\n    execution.checkpointing.interval: \"60000\"\n    execution.checkpointing.mode: EXACTLY_ONCE\n    state.backend: hashmap\n    taskmanager.numberOfTaskSlots: \"2\"\n  flinkVersion: v1_18\n  image: my-registry/flink-jobs/clickstream-etl:1.4.2\n  job:\n    args:\n      - --source.topic\n      - clickstream\n      - --sink\n      - iceberg\n    entryClass: com.example.ClickstreamJob\n    jarURI: local:///opt/flink/usrlib/clickstream-etl.jar\n    parallelism: 4\n    state: running\n    upgradeMode: stateless\n  jobManager:\n    replicas: 1\n    resource:\n      cpu: 1\n      memory: 2048m\n  serviceAccount: flink\n  taskManager:\n    resource:\n      cpu: 1\n      memory: 4096m\n",
    },
    "fraud-detection": {
      source: "name: fraud-detection\nenv: production\ntemplate_ref:\n  path: \"templates/flink-stateful-ha\"\nvalues:\n  image: \"my-registry/flink-jobs/fraud-detection:2.3.0\"\n  flinkConfiguration:\n    taskmanager.numberOfTaskSlots: \"6\"\n    execution.checkpointing.interval: \"15000\"\n  job:\n    jarURI: \"local:///opt/flink/usrlib/fraud-detection.jar\"\n    entryClass: \"com.example.FraudDetectionJob\"\n    parallelism: 12\n  taskManager:\n    resource:\n      cpu: 4\n      memory: \"16384m\"\n",
      merged: "# Auto-merged with defaults from template [flink-stateful-ha]\nname: fraud-detection\nenv: production\ntemplate_ref:\n  path: templates/flink-stateful-ha\nvalues:\n  flinkConfiguration:\n    execution.checkpointing.interval: \"15000\"\n    execution.checkpointing.mode: EXACTLY_ONCE\n    high-availability.storageDir: s3://flink-ha/\n    high-availability.type: kubernetes\n    state.backend: rocksdb\n    state.backend.incremental: \"true\"\n    state.checkpoints.dir: s3://flink-checkpoints/\n    state.savepoints.dir: s3://flink-savepoints/\n    taskmanager.numberOfTaskSlots: \"6\"\n  flinkVersion: v1_18\n  image: my-registry/flink-jobs/fraud-detection:2.3.0\n  job:\n    entryClass: com.example.FraudDetectionJob\n    jarURI: local:///opt/flink/usrlib/fraud-detection.jar\n    parallelism: 12\n    state: running\n    upgradeMode: savepoint\n  jobManager:\n    replicas: 2\n    resource:\n      cpu: 2\n      memory: 4096m\n  serviceAccount: flink\n  taskManager:\n    resource:\n      cpu: 4\n      memory: 16384m\n",
      manifest: "# 渲染结果预览 (FlinkDeployment CR — applied to the Flink Kubernetes Operator)\napiVersion: flink.apache.org/v1beta1\nkind: FlinkDeployment\nmetadata:\n  name: fraud-detection\n  labels:\n    app.kubernetes.io/name: fraud-detection\n    app.kubernetes.io/managed-by: gitops\n    environment: production\n  annotations:\n    flink.apache.org/profile: stateful-ha\nspec:\n  flinkConfiguration:\n    execution.checkpointing.interval: \"15000\"\n    execution.checkpointing.mode: EXACTLY_ONCE\n    high-availability.storageDir: s3://flink-ha/\n    high-availability.type: kubernetes\n    state.backend: rocksdb\n    state.backend.incremental: \"true\"\n    state.checkpoints.dir: s3://flink-checkpoints/\n    state.savepoints.dir: s3://flink-savepoints/\n    taskmanager.numberOfTaskSlots: \"6\"\n  flinkVersion: v1_18\n  image: my-registry/flink-jobs/fraud-detection:2.3.0\n  job:\n    entryClass: com.example.FraudDetectionJob\n    jarURI: local:///opt/flink/usrlib/fraud-detection.jar\n    parallelism: 12\n    state: running\n    upgradeMode: savepoint\n  jobManager:\n    replicas: 2\n    resource:\n      cpu: 2\n      memory: 4096m\n  serviceAccount: flink\n  taskManager:\n    resource:\n      cpu: 4\n      memory: 16384m\n",
    },
    "session-metrics": {
      source: "name: session-metrics\nenv: staging\ntemplate_ref:\n  path: \"templates/flink-streaming-standard\"\nvalues:\n  image: \"my-registry/flink-jobs/session-metrics:0.9.1\"\n  job:\n    jarURI: \"local:///opt/flink/usrlib/session-metrics.jar\"\n    entryClass: \"com.example.SessionMetricsJob\"\n    parallelism: 3\n    upgradeMode: last-state\n",
      merged: "# Auto-merged with defaults from template [flink-streaming-standard]\nname: session-metrics\nenv: staging\ntemplate_ref:\n  path: templates/flink-streaming-standard\nvalues:\n  flinkConfiguration:\n    execution.checkpointing.interval: \"60000\"\n    execution.checkpointing.mode: EXACTLY_ONCE\n    state.backend: hashmap\n    taskmanager.numberOfTaskSlots: \"2\"\n  flinkVersion: v1_18\n  image: my-registry/flink-jobs/session-metrics:0.9.1\n  job:\n    entryClass: com.example.SessionMetricsJob\n    jarURI: local:///opt/flink/usrlib/session-metrics.jar\n    parallelism: 3\n    state: running\n    upgradeMode: last-state\n  jobManager:\n    replicas: 1\n    resource:\n      cpu: 1\n      memory: 2048m\n  serviceAccount: flink\n  taskManager:\n    resource:\n      cpu: 1\n      memory: 2048m\n",
      manifest: "# 渲染结果预览 (FlinkDeployment CR — applied to the Flink Kubernetes Operator)\napiVersion: flink.apache.org/v1beta1\nkind: FlinkDeployment\nmetadata:\n  name: session-metrics\n  labels:\n    app.kubernetes.io/name: session-metrics\n    app.kubernetes.io/managed-by: gitops\n    environment: staging\nspec:\n  flinkConfiguration:\n    execution.checkpointing.interval: \"60000\"\n    execution.checkpointing.mode: EXACTLY_ONCE\n    state.backend: hashmap\n    taskmanager.numberOfTaskSlots: \"2\"\n  flinkVersion: v1_18\n  image: my-registry/flink-jobs/session-metrics:0.9.1\n  job:\n    entryClass: com.example.SessionMetricsJob\n    jarURI: local:///opt/flink/usrlib/session-metrics.jar\n    parallelism: 3\n    state: running\n    upgradeMode: last-state\n  jobManager:\n    replicas: 1\n    resource:\n      cpu: 1\n      memory: 2048m\n  serviceAccount: flink\n  taskManager:\n    resource:\n      cpu: 1\n      memory: 2048m\n",
    },
  },
};

export function mockTree() {
  return { services: MOCK_DATA.services, templates: MOCK_DATA.templates };
}

export function mockServiceDetail(id) {
  const meta = MOCK_DATA.services.find((s) => s.id === id) ?? MOCK_DATA.services[0];
  const content = MOCK_DATA.content[meta.id] ?? MOCK_DATA.content[MOCK_DATA.services[0].id];
  return { ...meta, content };
}
