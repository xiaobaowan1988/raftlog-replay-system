import React, { useState, useMemo } from 'react';
import Editor from '@monaco-editor/react';
import { Search, FileText, Folder, CheckCircle2, GitCommit } from 'lucide-react';
import { MOCK_DATA } from './mockData';

// ==========================================
// 主组件代码
// ==========================================
export default function App() {
  const [activeService, setActiveService] = useState(MOCK_DATA.services[0]);
  const [activeTab, setActiveTab] = useState('source');
  const [searchQuery, setSearchQuery] = useState('');

  const filteredServices = useMemo(() => {
    return MOCK_DATA.services.filter((s) =>
      s.name.toLowerCase().includes(searchQuery.toLowerCase())
    );
  }, [searchQuery]);

  return (
    <div className="flex h-screen w-full bg-[#1e1e1e] text-gray-300 font-sans selection:bg-blue-900 overflow-hidden">

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
                  onClick={() => setActiveService(service)}
                  className={`w-full text-left px-5 py-1.5 text-sm flex items-center transition-colors ${
                    activeService.id === service.id
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
            {MOCK_DATA.templates.map((template) => (
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
                {activeService.name}
              </h1>
              <div className="flex items-center text-xs px-2.5 py-1 bg-[#1e2e1e] text-green-400 rounded-full border border-green-900">
                <CheckCircle2 className="w-3.5 h-3.5 mr-1.5" />
                已同步 (Synced)
              </div>
            </div>
          </div>

          <div className="flex items-center text-xs text-gray-400 space-x-6">
            <div className="flex items-center">
              <span className="mr-2 shrink-0">引用模板:</span>
              <span className="text-blue-300 font-mono bg-[#2d2d2d] px-1.5 py-0.5 rounded border border-[#444] truncate max-w-md">
                templates/{activeService.template}
              </span>
            </div>
            <div className="flex items-center shrink-0">
              <GitCommit className="w-3.5 h-3.5 mr-1" />
              <span>Commit: <span className="font-mono text-gray-300">{activeService.commit}</span> ({activeService.time})</span>
            </div>
          </div>
        </div>

        <div className="flex bg-[#252526] text-sm overflow-x-auto no-scrollbar border-b border-[#333333] px-2 pt-2 shrink-0">
          {[
            { id: 'source', label: '原始配置 (Source)' },
            { id: 'merged', label: '完整参数 (Merged Values)' },
            { id: 'manifest', label: '底层清单 (Manifests Preview)' },
          ].map((tab) => (
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
          <Editor
            width="100%"
            height="100%"
            language="yaml"
            theme="vs-dark"
            path={activeTab}
            value={MOCK_DATA.yamlContent[activeTab]}
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
        </div>

      </div>
    </div>
  );
}
