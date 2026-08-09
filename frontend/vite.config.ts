import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

// Monaco is large and ships several web workers. Splitting it into its own chunk keeps the
// app shell small enough to paint before the editor has downloaded, which matters because
// the Strategies list is useful without the editor and the editor is not useful without it.
export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    port: 5173,
    // The API binds to loopback (spec 11). Proxying in dev means the browser talks to one
    // origin, so nothing depends on CORS being right in development and wrong in
    // production — the two are the same request.
    proxy: {
      '/api': {
        target: 'http://127.0.0.1:8756',
        changeOrigin: false,
      },
    },
  },
  build: {
    outDir: 'dist',
    sourcemap: true,
    chunkSizeWarningLimit: 4000,
    rollupOptions: {
      output: {
        manualChunks(id) {
          if (id.includes('monaco-editor')) return 'monaco'
          if (id.includes('node_modules')) return 'vendor'
          return undefined
        },
      },
    },
  },
})
