import React, { useState, useEffect, useMemo } from 'react';
import Editor from '@monaco-editor/react';
import {
  Search, FileText, Folder, CheckCircle2, GitCommit, Loader2, WifiOff,
  Play, XCircle,
} from 'lucide-react';
import { fetchTree, fetchService, deployService } from './api';

const TABS = [
  { id: 'source', label: '原始配置 (Source)' },
  { id: 'merged', label: '完整参数 (Merged Values)' },
  { id: 'manifest', label: '底层清单 (Manifests Preview)' },
];

export default function App() {
  const [tree, setTree] = useState({ services: [], templates: [] });
  const [activeId, setActiveId] = useState(null);
  const [detail, setDetail] = useState(null);
  const [activeTab, setActiveTab] = useState('source');
  const [searchQuery, setSearchQuery] = useState('');
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailError, setDetailError] = useState(null);
  // offline === true means the backend was unreachable and we're rendering the
  // bundled sample data instead.
  const [offline, setOffline] = useState(false);
  // deploy: { status: 'idle'|'running'|'ok'|'error', msg }
  const [deploy, setDeploy] = useState({ status: 'idle', msg: '' });

  // Load the sidebar tree once on mount; fall back to mock data on failure.
  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const t = await fetchTree();
        if (cancelled) return;
        setTree(t);
        setActiveId(t.services[0]?.id ?? null);
      } catch {
        const { mockTree } = await import('./mockData');
        if (cancelled) return;
        const t = mockTree();
        setTree(t);
        setActiveId(t.services[0]?.id ?? null);
        setOffline(true);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  // Load the selected service's content whenever the selection (or mode) changes.
  useEffect(() => {
    if (!activeId) return;
    let cancelled = false;
    setDetailLoading(true);
    setDetailError(null);
    setDeploy({ status: 'idle', msg: '' });
    (async () => {
      try {
        const d = offline
          ? (await import('./mockData')).mockServiceDetail(activeId)
          : await fetchService(activeId);
        if (!cancelled) setDetail(d);
      } catch (e) {
        if (!cancelled) {
          setDetail(null);
          setDetailError(e.message || '加载失败');
        }
      } finally {
        if (!cancelled) setDetailLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [activeId, offline]);

  const filteredServices = useMemo(
    () =>
      tree.services.filter((s) => s.name.toLowerCase().includes(searchQuery.toLowerCase())),
    [tree.services, searchQuery]
  );

  // Header shows the tree's metadata immediately on click; content streams in after.
  const activeMeta = useMemo(
    () => tree.services.find((s) => s.id === activeId) ?? null,
    [tree.services, activeId]
  );

  const editorValue = detail?.content?.[activeTab] ?? '';

  // Deploy: Server-Side Apply the rendered FlinkDeployment CR to the cluster.
  // Fires immediately on click (no confirmation, per the chosen behavior).
  async function handleDeploy() {
    if (!activeId || deploy.status === 'running') return;
    setDeploy({ status: 'running', msg: '' });
    try {
      const res = await deployService(activeId);
      const applied = res.applied?.[0];
      const where = applied ? `${applied.kind}/${applied.name} → ns:${applied.namespace}` : '已应用';
      setDeploy({ status: 'ok', msg: where });
    } catch (e) {
      setDeploy({ status: 'error', msg: e.message || '部署失败' });
    }
  }

  const canDeploy = !offline && !!activeId;

  return (
    <div className="flex flex-col h-screen w-full bg-[#1e1e1e] text-gray-300 font-sans selection:bg-blue-900 overflow-hidden">

      {offline && (
        <div className="flex items-center justify-center gap-2 text-xs py-1.5 bg-[#3a2e1e] text-amber-300 border-b border-amber-900/60 shrink-0">
          <WifiOff className="w-3.5 h-3.5" />
          后端未连接，正在展示内置示例数据 (offline demo)
        </div>
      )}

      <div className="flex flex-1 min-h-0">

        {/* ---------------- 左侧导航栏 ---------------- */}
        <div className="w-64 border-r border-[#333333] flex flex-col bg-[#252526] shrink-0 h-full">

          <div className="p-3 border-b border-[#333333]">
            <div className="relative">
              <Search className="w-4 h-4 absolute left-2.5 top-2.5 text-gray-500" />
              <input
                type="text"
                placeholder="搜索服务名称..."
                className="w-full bg-[#3c3c3c] text-sm text-gray-200 rounded px-8 py-1.5 border border-transparent focus:outline-none focus:border-blue-500 transition-colors"
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
              />
            </div>
          </div>

          <div className="flex-1 overflow-y-auto py-3">
            <div className="px-4 mb-2 flex items-center text-[11px] font-bold text-gray-400 uppercase tracking-wider">
              <Folder className="w-3.5 h-3.5 mr-1.5" /> services /
            </div>
            <ul className="mb-6 space-y-0.5">
              {filteredServices.map((service) => (
                <li key={service.id}>
                  <button
                    onClick={() => setActiveId(service.id)}
                    className={`w-full text-left px-5 py-1.5 text-sm flex items-center transition-colors ${
                      activeId === service.id
                        ? 'bg-[#37373d] text-white border-l-2 border-blue-500'
                        : 'text-gray-400 hover:bg-[#2a2d2e] hover:text-gray-200 border-l-2 border-transparent'
                    }`}
                  >
                    <FileText className="w-4 h-4 mr-2 opacity-70 shrink-0" />
                    <span className="truncate">{service.name}</span>
                  </button>
                </li>
              ))}
              {filteredServices.length === 0 && (
                <div className="px-5 py-2 text-xs text-gray-600">未找到匹配的服务</div>
              )}
            </ul>

            <div className="px-4 mb-2 flex items-center text-[11px] font-bold text-gray-500 uppercase tracking-wider">
              <Folder className="w-3.5 h-3.5 mr-1.5" /> templates /
            </div>
            <ul className="space-y-0.5">
              {tree.templates.map((template) => (
                <li
                  key={template.id}
                  className="w-full text-left px-5 py-1.5 text-sm flex items-center text-gray-600 cursor-not-allowed"
                >
                  <FileText className="w-4 h-4 mr-2 opacity-40 shrink-0" />
                  <span className="truncate">{template.name}</span>
                </li>
              ))}
            </ul>
          </div>
        </div>

        {/* ---------------- 右侧主工作区 ---------------- */}
        <div className="flex-1 flex flex-col min-w-0 bg-[#1e1e1e] h-full">

          <div className="h-20 border-b border-[#333333] px-6 flex flex-col justify-center gap-2 shrink-0">
            <div className="flex items-center justify-between">
              <div className="flex items-center space-x-4">
                <h1 className="text-lg font-medium text-white flex items-center">
                  <FileText className="w-5 h-5 mr-2 text-blue-400" />
                  {activeMeta?.name ?? '—'}
                </h1>
                <div className="flex items-center text-xs px-2.5 py-1 bg-[#1e2e1e] text-green-400 rounded-full border border-green-900">
                  <CheckCircle2 className="w-3.5 h-3.5 mr-1.5" />
                  已同步 (Synced)
                </div>
                {detailLoading && (
                  <Loader2 className="w-4 h-4 text-gray-500 animate-spin" />
                )}
              </div>

              {/* Deploy (Run) — the one write action: apply the FlinkDeployment CR. */}
              <div className="flex items-center gap-3">
                {deploy.status === 'ok' && (
                  <span className="flex items-center text-xs text-green-400 max-w-md truncate">
                    <CheckCircle2 className="w-3.5 h-3.5 mr-1.5 shrink-0" />
                    已部署: <span className="font-mono ml-1 truncate">{deploy.msg}</span>
                  </span>
                )}
                {deploy.status === 'error' && (
                  <span className="flex items-center text-xs text-red-400 max-w-md truncate" title={deploy.msg}>
                    <XCircle className="w-3.5 h-3.5 mr-1.5 shrink-0" />
                    <span className="truncate">{deploy.msg}</span>
                  </span>
                )}
                <button
                  onClick={handleDeploy}
                  disabled={!canDeploy || deploy.status === 'running'}
                  title={offline ? '后端未连接，无法部署' : '部署 FlinkDeployment 到集群'}
                  className={`flex items-center gap-1.5 text-sm px-3.5 py-1.5 rounded-md font-medium transition-colors ${
                    canDeploy && deploy.status !== 'running'
                      ? 'bg-blue-600 hover:bg-blue-500 text-white'
                      : 'bg-[#2d2d2d] text-gray-500 cursor-not-allowed'
                  }`}
                >
                  {deploy.status === 'running' ? (
                    <><Loader2 className="w-4 h-4 animate-spin" /> 部署中...</>
                  ) : (
                    <><Play className="w-4 h-4" fill="currentColor" /> Run / 部署</>
                  )}
                </button>
              </div>
            </div>

            {activeMeta && (
              <div className="flex items-center text-xs text-gray-400 space-x-6">
                <div className="flex items-center">
                  <span className="mr-2 shrink-0">引用模板:</span>
                  <span className="text-blue-300 font-mono bg-[#2d2d2d] px-1.5 py-0.5 rounded border border-[#444] truncate max-w-md">
                    templates/{activeMeta.template}
                  </span>
                </div>
                <div className="flex items-center shrink-0">
                  <GitCommit className="w-3.5 h-3.5 mr-1" />
                  <span>Commit: <span className="font-mono text-gray-300">{activeMeta.commit}</span> ({activeMeta.time})</span>
                </div>
              </div>
            )}
          </div>

          <div className="flex bg-[#252526] text-sm overflow-x-auto no-scrollbar border-b border-[#333333] px-2 pt-2 shrink-0">
            {TABS.map((tab) => (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id)}
                className={`px-5 py-2 border-t-2 rounded-t-sm transition-colors whitespace-nowrap ${
                  activeTab === tab.id
                    ? 'border-blue-500 bg-[#1e1e1e] text-white'
                    : 'border-transparent text-gray-400 hover:bg-[#2d2d2d] hover:text-gray-200'
                }`}
              >
                {tab.label}
              </button>
            ))}
          </div>

          <div className="flex-1 relative w-full h-full">
            {detailError ? (
              <div className="absolute inset-0 flex items-center justify-center text-sm text-red-400">
                加载服务详情失败：{detailError}
              </div>
            ) : (
              <Editor
                width="100%"
                height="100%"
                language="yaml"
                theme="vs-dark"
                path={`${activeId ?? 'none'}:${activeTab}`}
                value={editorValue}
                options={{
                  readOnly: true,
                  minimap: { enabled: false },
                  scrollBeyondLastLine: false,
                  fontSize: 14,
                  fontFamily: "'JetBrains Mono', 'Fira Code', 'Courier New', monospace",
                  wordWrap: 'on',
                  padding: { top: 20, bottom: 20 },
                  lineHeight: 24,
                  renderLineHighlight: 'all',
                  matchBrackets: 'always',
                  automaticLayout: true,
                }}
              />
            )}
          </div>

        </div>
      </div>
    </div>
  );
}
