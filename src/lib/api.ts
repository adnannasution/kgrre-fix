import type {
  DatasetStats,
  DatasetSummary,
  Equipment360,
  FolderScan,
  GraphEdgeDetail,
  GraphNode,
  GraphSlice,
  ImportJob,
  LoadSummaryRow,
  NodeDegree,
  QueryMetadata,
  ReadinessContext,
  ReviewIssue,
  ReliabilityInsight,
  RuSummary,
  SchemaBundle,
} from '../types'

const API = '/api'

async function request<T>(path: string, options?: RequestInit): Promise<T> {
  const response = await fetch(`${API}${path}`, {
    ...options,
    headers: options?.body instanceof FormData ? options.headers : { 'Content-Type': 'application/json', ...options?.headers },
  })
  if (!response.ok) {
    const detail = await response.json().catch(() => ({ detail: response.statusText }))
    throw new Error(detail.detail || 'Permintaan gagal.')
  }
  return response.json()
}

export const api = {
  scanFolder: (validate = false) => request<FolderScan>(`/folder?validate=${validate}`),
  updateFolder: (upload_folder: string) =>
    request<FolderScan>('/folder', { method: 'PUT', body: JSON.stringify({ upload_folder }) }),
  startImport: (name: string, allow_partial = true) =>
    request<ImportJob>('/imports', { method: 'POST', body: JSON.stringify({ name, allow_partial }) }),
  startZipImport: (file: File, name: string, allow_partial = true) => {
    const form = new FormData()
    form.append('file', file)
    form.append('name', name)
    form.append('allow_partial', String(allow_partial))
    return request<ImportJob>('/imports/zip', { method: 'POST', body: form })
  },
  importStatus: (id: string) => request<ImportJob>(`/imports/${id}`),
  cancelImport: (id: string) => request(`/imports/${id}`, { method: 'DELETE' }),
  initChunkedUpload: (name: string, files: { name: string; total_chunks: number }[]) =>
    request<{ upload_id: string }>('/imports/chunked/init', { method: 'POST', body: JSON.stringify({ name, files }) }),
  uploadChunk: (uploadId: string, fileName: string, chunkIndex: number, data: Blob) => {
    const form = new FormData()
    form.append('file_name', fileName)
    form.append('chunk_index', String(chunkIndex))
    form.append('data', data, fileName)
    return request<{ file_name: string; chunk_index: number; received: number; total: number }>(
      `/imports/chunked/${uploadId}/chunk`, { method: 'POST', body: form }
    )
  },
  commitChunkedUpload: (uploadId: string) =>
    request<ImportJob>(`/imports/chunked/${uploadId}/commit`, { method: 'POST' }),
  etlUpload: (files: File[], name: string) => {
    const form = new FormData()
    files.forEach(f => form.append('files', f, f.name))
    form.append('name', name)
    return request<ImportJob>('/etl/upload', { method: 'POST', body: form })
  },
  resetAll: () => request<{ ok: boolean }>('/reset', { method: 'POST' }),
  datasets: () => request<DatasetSummary[]>('/datasets'),
  dataset: (id: string) => request<DatasetSummary>(`/datasets/${id}`),
  loadSummary: (id: string) => request<LoadSummaryRow[]>(`/datasets/${id}/load-summary`),
  renameDataset: (id: string, name: string) =>
    request<DatasetSummary>(`/datasets/${id}`, { method: 'PATCH', body: JSON.stringify({ name }) }),
  deleteDataset: (id: string) => request(`/datasets/${id}`, { method: 'DELETE' }),
  stats: (id: string) => request<DatasetStats>(`/datasets/${id}/stats`),
  queryMetadata: (id: string) => request<QueryMetadata>(`/datasets/${id}/query-metadata`),
  readinessContext: (id: string, nodeId: string) =>
    request<ReadinessContext>(`/datasets/${id}/readiness-context/${encodeURIComponent(nodeId)}`),
  search: (id: string, query = '', nodeType = '', domain = '', limit = 50, refineryUnit = '', equipmentCode = '') =>
    request<GraphNode[]>(`/datasets/${id}/search?${new URLSearchParams({ q: query, node_type: nodeType, domain, limit: String(limit), refinery_unit: refineryUnit, equipment_code: equipmentCode })}`),
  neighbors: (id: string, nodeId: string, options: { depth: number; includeCandidates: boolean; minConfidence: number; relationshipType?: string; nodeType?: string; refineryUnit?: string; equipmentCode?: string; limit?: number }) =>
    request<GraphSlice>(`/datasets/${id}/neighbors/${encodeURIComponent(nodeId)}?${new URLSearchParams({
      depth: String(options.depth),
      include_candidates: String(options.includeCandidates),
      min_confidence: String(options.minConfidence),
      relationship_type: options.relationshipType ?? '',
      node_type: options.nodeType ?? '',
      refinery_unit: options.refineryUnit ?? '',
      equipment_code: options.equipmentCode ?? '',
      limit: String(options.limit ?? 300),
    })}`),
  degree: (id: string, nodeId: string, includeCandidates = false) =>
    request<NodeDegree>(`/datasets/${id}/nodes/${encodeURIComponent(nodeId)}/degree?include_candidates=${includeCandidates}`),
  directedDescendants: (id: string, nodeId: string, options: { minDepth?: number; maxDepth?: number; limit?: number; relationshipType?: string; includeCandidates?: boolean }) =>
    request<GraphSlice>(`/datasets/${id}/directed-descendants/${encodeURIComponent(nodeId)}?${new URLSearchParams({
      min_depth: String(options.minDepth ?? 3),
      max_depth: String(options.maxDepth ?? 5),
      limit: String(options.limit ?? 300),
      relationship_type: options.relationshipType ?? '',
      include_candidates: String(options.includeCandidates ?? false),
    })}`),
  node: (id: string, nodeId: string) =>
    request<GraphNode & { domain_record?: Record<string, unknown> }>(`/datasets/${id}/nodes/${encodeURIComponent(nodeId)}`),
  relationship: (id: string, relationshipId: string) =>
    request<GraphEdgeDetail>(`/datasets/${id}/relationships/${encodeURIComponent(relationshipId)}`),
  propertyQuery: (id: string, query: string, limit = 200) =>
    request<GraphSlice>(`/datasets/${id}/property-query`, { method: 'POST', body: JSON.stringify({ query, limit }) }),
  equipment360: (id: string, nodeId: string) =>
    request<Equipment360>(`/datasets/${id}/equipment/${encodeURIComponent(nodeId)}/360`),
  issues: (id: string, issueType = '') =>
    request<{ total: number; items: ReviewIssue[] }>(`/datasets/${id}/issues?${new URLSearchParams({ issue_type: issueType, limit: '500' })}`),
  audit: (id: string, issueType: string) =>
    request<{ total: number; items: unknown[] }>(`/datasets/${id}/audit/${issueType}?limit=500`),
  ruSummary: (id: string) => request<RuSummary>(`/datasets/${id}/ru-summary`),
  schema: (id: string) => request<SchemaBundle>(`/datasets/${id}/schema`),
  reliabilityInsight: (id: string) => request<ReliabilityInsight>(`/datasets/${id}/insights/reliability`),
  analysis: (id: string, name: string) => request<Record<string, unknown>[]>(`/datasets/${id}/analysis/${name}`),
  exportUrl: (id: string, kind: string) => `${API}/datasets/${id}/export/${kind}`,
}

export async function streamDiagnosis(
  prompt: string,
  role: string,
  onChunk: (text: string) => void,
  signal?: AbortSignal,
): Promise<void> {
  const res = await fetch(`${API}/diagnosis/generate`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ prompt, role }),
    signal,
  })
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }))
    throw new Error((err as { detail?: string }).detail || 'Gagal menghubungi server.')
  }
  const reader = res.body!.getReader()
  const dec = new TextDecoder()
  let buf = ''
  while (true) {
    const { done, value } = await reader.read()
    if (done) break
    buf += dec.decode(value, { stream: true })
    const lines = buf.split('\n')
    buf = lines.pop() ?? ''
    for (const line of lines) {
      if (!line.startsWith('data: ')) continue
      const payload = line.slice(6)
      if (payload === '[DONE]') return
      try {
        const parsed = JSON.parse(payload) as { text?: string }
        if (parsed.text) onChunk(parsed.text)
      } catch { /* skip malformed */ }
    }
  }
}
