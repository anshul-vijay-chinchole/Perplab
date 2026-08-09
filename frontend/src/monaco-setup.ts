import * as monaco from 'monaco-editor'
import { loader } from '@monaco-editor/react'
import editorWorker from 'monaco-editor/esm/vs/editor/editor.worker?worker'

/**
 * Bundle Monaco locally instead of fetching it from a CDN.
 *
 * `@monaco-editor/react` defaults to loading the editor from jsDelivr at runtime. That is a
 * sensible default for a public web app and the wrong one here for three reasons, any of
 * which alone would settle it:
 *
 * 1. PerpLab binds to `127.0.0.1` (spec 11). A tool that only works when the machine has
 *    internet is not a local tool, and the moment it is needed most — something has gone
 *    wrong, the network included — is the moment it would fail to open.
 * 2. It makes the editor a third-party dependency at *runtime*. The page holding the code
 *    that trades your money would be executing a script fetched from a host outside this
 *    project's control.
 * 3. It is slow and unversioned relative to `package.json`.
 *
 * Only the base editor worker is registered. Monaco ships dedicated language services for
 * TypeScript, JSON, CSS and HTML; Python has none — its support is Monarch tokenisation
 * that runs on the main thread — so wiring the others in would bundle three workers that
 * this app can never use.
 *
 * Imported for its side effect, before the app renders. `loader.config` has to run before
 * the first `<Editor>` mounts, or the component will already have started the CDN fetch.
 */

self.MonacoEnvironment = {
  getWorker() {
    return new editorWorker()
  },
}

loader.config({ monaco })

export {}
