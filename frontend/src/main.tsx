import React from 'react'
import ReactDOM from 'react-dom/client'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import './monaco-setup'
import { App } from './App'
import { ErrorBoundary } from './components/ErrorBoundary'
import './styles.css'

const client = new QueryClient({
  defaultOptions: {
    queries: {
      // This is a single-user tool talking to localhost. Refetching on every window focus
      // would re-run the strategy list each time the author alt-tabs back from the docs,
      // and there is no second user whose writes we could be missing.
      refetchOnWindowFocus: false,
      retry: 1,
      staleTime: 5_000,
    },
  },
})

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <QueryClientProvider client={client}>
      {/* The backstop. Anything that escapes the per-panel boundaries lands here rather
          than unmounting the root and leaving a white window -- which is what a render
          fault did before this existed, with no indication that the sessions behind it
          were still running. The routine catcher is the per-tab boundary inside App,
          which keeps the chrome (and the kill switch) mounted; this one only fires when
          the chrome itself faults. `onReset` evicts the query cache so Try Again retries
          against fresh data rather than the cached payload that threw. */}
      <ErrorBoundary what="PerpLab" onReset={() => client.clear()}>
        <App />
      </ErrorBoundary>
    </QueryClientProvider>
  </React.StrictMode>,
)
