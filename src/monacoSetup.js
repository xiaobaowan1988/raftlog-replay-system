// Self-host Monaco instead of loading it from a CDN.
//
// @monaco-editor/react defaults to fetching the editor from jsDelivr at
// runtime. That breaks in air-gapped / restricted internal networks — exactly
// where a GitOps console tends to live — so we bundle Monaco locally and point
// the loader at it.
import { loader } from '@monaco-editor/react';
// Import only the editor core + the YAML basic-language, not the full
// monaco-editor bundle (which ships every language and would ~10x the payload).
import * as monaco from 'monaco-editor/esm/vs/editor/editor.api';
import 'monaco-editor/esm/vs/basic-languages/yaml/yaml.contribution';
import editorWorker from 'monaco-editor/esm/vs/editor/editor.worker?worker';

// The YAML basic-language (tokenization / colorization) runs on the main
// thread, so the generic editor worker is all we need.
self.MonacoEnvironment = {
  getWorker() {
    return new editorWorker();
  },
};

loader.config({ monaco });
