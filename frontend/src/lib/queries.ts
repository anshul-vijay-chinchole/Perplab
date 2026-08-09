/**
 * The single source of truth for the three safety-relevant polled queries.
 *
 * `['sessions']`, `['kill']` and `['exchange-status']` were each registered in two or
 * three components with *different* `refetchInterval` and `retry` options. TanStack Query
 * keys one cache entry per key, so which options actually governed depended on mount
 * order — the chrome's 2 s kill poll could silently become the dashboard's 3 s one, and
 * "how stale can the kill badge be" was decided by which tab happened to mount first.
 * Declaring the options once beside the key ends the argument.
 *
 * All three set `staleTime: 0`: these answer "is real money at risk right now", and the
 * global 5 s default exists for strategy lists, not safety indicators. All three set
 * `retry: 0`: they poll on a short interval anyway, so a retry only multiplies the
 * requests a backend that is down has to refuse — and the *callers* are required to treat
 * a failed query as unknown, never as empty (see `Chrome.tsx`'s kill button).
 */

import { queryOptions } from '@tanstack/react-query'
import { api } from '../api'

export const sessionsQuery = queryOptions({
  queryKey: ['sessions'],
  queryFn: api.sessions,
  refetchInterval: 2000,
  staleTime: 0,
  retry: 0,
})

export const killQuery = queryOptions({
  queryKey: ['kill'],
  queryFn: api.killState,
  refetchInterval: 2000,
  staleTime: 0,
  retry: 0,
})

export const exchangeStatusQuery = queryOptions({
  queryKey: ['exchange-status'],
  queryFn: api.exchangeStatus,
  refetchInterval: 5000,
  staleTime: 0,
  retry: 0,
})
