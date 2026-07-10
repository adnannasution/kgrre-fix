import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import type { JSX, ReactNode } from 'react'
import { GraphView } from './components/GraphView'
import {
  AlertIcon,
  ChainIcon,
  SparkleIcon,
  CheckIcon,
  ChevronIcon,
  DatabaseIcon,
  DownloadIcon,
  EquipmentIcon,
  GraphIcon,
  GridIcon,
  SearchIcon,
  TrashIcon,
  UploadIcon,
} from './components/Icons'
import { api, streamDiagnosis, streamAnalysis } from './lib/api'
import type {
  DatasetStats,
  DatasetSummary,
  EquipmentCoverageDomain,
  EquipmentRelated,
  FolderScan,
  UnmatchedEquipment,
  GraphEdge,
  GraphEdgeDetail,
  GraphNode,
  GraphSlice,
  ImportJob,
  LoadSummaryRow,
  QueryMetadata,
  ReadinessContext,
  ReliabilityInsight,
  ReviewIssue,
  RuSummary,
} from './types'

type Page = 'overview' | 'import' | 'executive' | 'insight' | 'equipment' | 'graph' | 'depth' | 'review' | 'datasets' | 'chains' | 'coverage' | 'analisis'
const emptyGraph: GraphSlice = { nodes: [], edges: [], truncated: false }

// Ambil data dashboard berat (executive/reliability) yang dihitung di latar oleh backend.
// Backend membalas { computing: true } selagi cache dingin; kita polling tiap 3 detik
// sampai hasil final tiba. Mengembalikan { data, computing }.
function useWarmingData<T extends { computing?: boolean }>(
  key: string | undefined,
  fetcher: () => Promise<T>,
): { data?: T; computing: boolean } {
  const [data, setData] = useState<T>()
  const [computing, setComputing] = useState(false)
  useEffect(() => {
    if (!key) return
    let cancelled = false
    let timer: ReturnType<typeof setTimeout>
    const load = () => {
      void fetcher().then((result) => {
        if (cancelled) return
        setData(result)
        if (result.computing) {
          setComputing(true)
          timer = setTimeout(load, 3000)
        } else {
          setComputing(false)
        }
      })
    }
    load()
    return () => { cancelled = true; clearTimeout(timer) }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key])
  return { data, computing }
}
type GraphSource = 'empty' | 'neighborhood' | 'directed' | 'property'
type QueryEntity = 'NODE' | 'EDGE'
type QueryOperator = '=' | '!=' | '>' | '<' | '>=' | '<=' | 'CONTAINS' | 'LIKE' | 'NOT LIKE' | 'EXISTS'
type AnalysisEvidence = Record<string, Record<string, unknown>[]>
interface DiagnosisEvidencePack {
  focusRu: string
  primaryDiagnosis: string[]
  businessRisk: string[]
  processReliabilitySignal: string[]
  equipmentReliabilitySignal: string[]
  workManagementSignal: string[]
  leadershipDecision: string[]
  confidenceAndCaveats: string[]
  reasoning: string[]
  missing: string[]
}
interface RelatedNodeEvidence {
  lines: string[]
  truncated: boolean
  totalRelated: number
}
interface QueryCondition {
  id: number
  field: string
  operator: QueryOperator
  value: string
}
interface QueryBuilderState {
  entity: QueryEntity
  type: string
  conditions: QueryCondition[]
}
const queryOperators: QueryOperator[] = ['=', '!=', '>', '<', '>=', '<=', 'CONTAINS', 'LIKE', 'NOT LIKE', 'EXISTS']
const defaultQueryBuilder: QueryBuilderState = {
  entity: 'NODE',
  type: 'equipment',
  conditions: [{ id: 1, field: 'criticallity', operator: '=', value: 'Z' }],
}
const queryPresets: Array<{ label: string; builder: QueryBuilderState }> = [
  { label: 'Risk equipment', builder: { entity: 'NODE', type: 'equipment', conditions: [{ id: 1, field: 'derived_risk_score', operator: '>', value: '0' }] } },
  { label: 'RKAP RU audit', builder: { entity: 'EDGE', type: 'EQUIPMENT_HAS_RKAP_PROGRAM', conditions: [{ id: 1, field: 'ru_consistency', operator: '!=', value: 'match' }] } },
  { label: 'Readiness RU audit', builder: { entity: 'EDGE', type: 'EQUIPMENT_HAS_READINESS_RECORD', conditions: [{ id: 1, field: 'ru_consistency', operator: '!=', value: 'match' }] } },
  { label: 'Inspection matches', builder: { entity: 'EDGE', type: 'EQUIPMENT_HAS_INSPECTION', conditions: [{ id: 1, field: 'match_quality_bucket', operator: '=', value: 'verified' }] } },
  { label: 'High value RKAP', builder: { entity: 'NODE', type: 'rkap_program', conditions: [{ id: 1, field: 'derived_is_high_value', operator: '=', value: 'true' }] } },
  { label: 'Abnormal reliability', builder: { entity: 'NODE', type: 'reliability_observation', conditions: [{ id: 1, field: 'derived_is_abnormal_status', operator: '=', value: 'true' }] } },
]
const diagnosisEvidenceTables = [
  'analysis_ready_ru_cross_domain_assessment',
  'analysis_ready_data_confidence_by_ru_domain',
  'analysis_ready_rkap_cost_alignment_signal',
  'analysis_ready_reliability_performance_signal',
  'analysis_ready_work_management_health',
  'analysis_ready_program_effectiveness_signal',
  'analysis_ready_readiness_operation_signal',
  'analysis_ready_defect_elimination_signal',
  'analysis_ready_reasoning_evidence_index',
]
const cleanSession = new URLSearchParams(window.location.search).get('fresh') === '1'

function requiresValue(operator: QueryOperator) {
  return operator !== 'EXISTS'
}

function quoteQueryValue(value: string) {
  return value.includes('"') && !value.includes("'") ? `'${value}'` : `"${value.replaceAll('"', "'")}"`
}

function buildPropertyQuery(builder: QueryBuilderState) {
  const base = `${builder.entity} ${builder.type}`
  const conditions = builder.conditions
    .filter((condition) => condition.field.trim())
    .map((condition) => {
      const operator = condition.operator
      return requiresValue(operator)
        ? `${condition.field.trim()} ${operator} ${quoteQueryValue(condition.value.trim())}`
        : `${condition.field.trim()} EXISTS`
    })
  return conditions.length ? `${base} WHERE ${conditions.join(' AND ')}` : base
}

function validateQueryBuilder(builder: QueryBuilderState) {
  if (!builder.type.trim()) return 'Pilih node atau relationship type.'
  for (const condition of builder.conditions) {
    if (!condition.field.trim()) return 'Pilih field untuk setiap kondisi.'
    if (requiresValue(condition.operator) && !condition.value.trim()) return 'Isi value untuk setiap kondisi yang membutuhkan value.'
  }
  return ''
}

export default function App() {
  const [page, setPage] = useState<Page>('import')
  const [scan, setScan] = useState<FolderScan>()
  const [datasets, setDatasets] = useState<DatasetSummary[]>([])
  const [activeId, setActiveId] = useState(cleanSession ? '' : localStorage.getItem('kg-active-dataset') ?? '')
  const [stats, setStats] = useState<DatasetStats>()
  const [statsLoading, setStatsLoading] = useState(false)
  const [job, setJob] = useState<ImportJob>()
  const [error, setError] = useState('')
  const [busy, setBusy] = useState(false)
  const [density, setDensity] = useState(() => Number(localStorage.getItem('kg-density')) || 1)

  const active = datasets.find((item) => item.id === activeId)
  const refreshDatasets = useCallback(async () => {
    const items = await api.datasets()
    setDatasets(items)
    if (!activeId && items[0]) setActiveId(items[0].id)
    if (activeId && !items.some((item) => item.id === activeId)) setActiveId(items[0]?.id ?? '')
  }, [activeId])
  const refreshFolder = useCallback(async (validate = false) => {
    try {
      setScan(await api.scanFolder(validate))
      setError('')
    } catch (reason) {
      setError(message(reason))
    }
  }, [])

  useEffect(() => {
    void refreshFolder()
    void refreshDatasets()
    const timer = window.setInterval(() => void refreshFolder(), 10_000)
    return () => window.clearInterval(timer)
  }, [refreshFolder, refreshDatasets])

  useEffect(() => {
    localStorage.setItem('kg-density', String(density))
    document.documentElement.style.setProperty('--density', String(density))
  }, [density])

  useEffect(() => {
    if (!activeId) {
      setStats(undefined)
      return
    }
    if (cleanSession) localStorage.removeItem('kg-active-dataset')
    else localStorage.setItem('kg-active-dataset', activeId)
    setStatsLoading(true)
    setStats(undefined)
    api.stats(activeId).then(setStats).catch((reason) => setError(message(reason))).finally(() => setStatsLoading(false))
  }, [activeId])

  useEffect(() => {
    if (!job || !['queued', 'running'].includes(job.status)) return
    const timer = window.setInterval(async () => {
      const next = await api.importStatus(job.id)
      setJob(next)
      if (next.status === 'completed') {
        await refreshDatasets()
        // Hanya auto-switch ke dataset baru jika belum ada dataset yang aktif.
        // Kalau sudah ada dataset aktif (mis. upload domain tambahan), jangan
        // ganti activeId agar tampilan existing dataset tidak terganggu.
        if (!activeId && next.dataset_id) setActiveId(next.dataset_id)
        setPage('overview')
        await refreshFolder()
      }
    }, 1000)
    return () => window.clearInterval(timer)
  }, [job, refreshDatasets, refreshFolder])

  const startImport = async (name: string) => {
    setBusy(true)
    setError('')
    try {
      setScan(await api.scanFolder(true))
      const next = await api.startImport(name, true)
      setJob(next)
    } catch (reason) {
      setError(message(reason))
    } finally {
      setBusy(false)
    }
  }

  const startZipImport = async (file: File, name: string) => {
    setBusy(true)
    setError('')
    try {
      setJob(await api.startZipImport(file, name, true))
    } catch (reason) {
      setError(message(reason))
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand">
          <div className="brand-mark"><GraphIcon /></div>
          <div><strong>Kilang Graph</strong><span>Equipment intelligence</span></div>
        </div>
        <nav>
          <Nav icon={<GridIcon />} label="Overview" active={page === 'overview'} onClick={() => setPage('overview')} />
          <Nav icon={<DatabaseIcon />} label="Executive RU" active={page === 'executive'} onClick={() => setPage('executive')} />
          <Nav icon={<GridIcon />} label="Reliability Insight" active={page === 'insight'} onClick={() => setPage('insight')} />
          <Nav icon={<EquipmentIcon />} label="Equipment 360" active={page === 'equipment'} onClick={() => setPage('equipment')} />
          <Nav icon={<GraphIcon />} label="Graph Explorer" active={page === 'graph'} onClick={() => setPage('graph')} />
          <Nav icon={<SparkleIcon />} label="Analisis AI" active={page === 'analisis'} onClick={() => setPage('analisis')} />
          <Nav icon={<CheckIcon />} label="Coverage Equipment" active={page === 'coverage'} onClick={() => setPage('coverage')} />
          {false && <Nav icon={<ChainIcon />} label="Rantai Relasi" active={page === 'chains'} onClick={() => setPage('chains')} />}
          <Nav icon={<ChevronIcon />} label="Depth Explorer" active={page === 'depth'} onClick={() => setPage('depth')} />
          <Nav icon={<AlertIcon />} label="Data Review" active={page === 'review'} onClick={() => setPage('review')} badge={stats?.issues} />
          <Nav icon={<DatabaseIcon />} label="Daftar Dataset" active={page === 'datasets'} onClick={() => setPage('datasets')} />
          <Nav icon={<UploadIcon />} label="Import Center" active={page === 'import'} onClick={() => setPage('import')} />
        </nav>
        <div className="sidebar-foot">
          <span className="eyebrow">Active dataset</span>
          <select value={activeId} onChange={(event) => setActiveId(event.target.value)}>
            <option value="">Belum ada dataset</option>
            {datasets.map((item) => <option key={item.id} value={item.id}>{item.name}</option>)}
          </select>
          <div className="local-chip"><span /> Railway · PostgreSQL</div>
        </div>
      </aside>

      <main className="main">
        <header className="topbar">
          <div>
            <span className="eyebrow">{active ? active.mode.replaceAll('_', ' ') : 'knowledge graph'}</span>
            <h1>{titleFor(page)}</h1>
          </div>
          <div className="top-actions">
            <div className="privacy"><span>●</span> Tersimpan di PostgreSQL (Railway)</div>
            <button className="icon-button" onClick={() => void refreshFolder()} title="Scan folder"><UploadIcon /></button>
          </div>
        </header>
        {error && <div className="error-banner"><AlertIcon />{error}<button onClick={() => setError('')}>×</button></div>}

        {page === 'overview' && <Overview active={active} stats={stats} onNavigate={setPage} />}
        {page === 'import' && (
          <ImportCenter
            scan={scan}
            job={job}
            busy={busy}
            onScan={() => void refreshFolder(true)}
            onStart={startImport}
            onZip={startZipImport}
            onCancel={() => job && void api.cancelImport(job.id)}
            onFolder={async (path) => setScan(await api.updateFolder(path))}
            onNavigate={setPage}
            datasets={datasets}
            onRefreshDatasets={refreshDatasets}
          />
        )}
        {page === 'executive' && <ExecutiveDashboard dataset={active} />}
        {page === 'insight' && <ReliabilityInsightPage dataset={active} onNavigate={setPage} />}
        {page === 'equipment' && <Equipment360 dataset={active} />}
        {page === 'graph' && <GraphExplorer dataset={active} stats={stats} />}
        {page === 'depth' && <DepthExplorer dataset={active} />}
        {page === 'review' && <DataReview dataset={active} />}
        {page === 'chains' && <ChainExplorer dataset={active} />}
        {page === 'coverage' && <EquipmentCoveragePage dataset={active} />}
        {page === 'analisis' && <AnalisisPage dataset={active} />}
        {page === 'datasets' && (
          <DatasetManager
            datasets={datasets}
            activeId={activeId}
            onActivate={setActiveId}
            onRefresh={refreshDatasets}
            onResetAll={() => { setActiveId(''); setPage('import') }}
          />
        )}
      </main>
    </div>
  )
}

function Nav({ icon, label, active, onClick, badge }: { icon: ReactNode; label: string; active: boolean; onClick: () => void; badge?: number }) {
  return <button className={`nav-item ${active ? 'active' : ''}`} onClick={onClick}>{icon}<span>{label}</span>{badge ? <b>{compact(badge)}</b> : null}</button>
}

function Overview({ active, stats, onNavigate }: { active?: DatasetSummary; stats?: DatasetStats; onNavigate: (page: Page) => void }) {
  if (!active) return <EmptyState icon={<DatabaseIcon />} title="Belum ada dataset aktif" text="Upload ZIP output ETL melalui Import Center untuk memuat knowledge graph ke database." action="Buka Import Center" onAction={() => onNavigate('import')} />
  return (
    <section className="stack">
      <div className="hero-panel">
        <div>
          <span className="eyebrow">Knowledge graph ready</span>
          <h2>{active.name}</h2>
          <p>{active.workbooks.length} file CSV/JSON ETL terhubung melalui graph lokal yang dapat ditelusuri hingga lima hop.</p>
        </div>
        <button className="primary" onClick={() => onNavigate('graph')}>Explore graph <ChevronIcon /></button>
      </div>
      <div className="metrics">
        <Metric label="Nodes" value={stats?.nodes} accent="mint" />
        <Metric label="Verified edges" value={stats?.verified_edges} accent="blue" />
        <Metric label="Candidate edges" value={stats?.candidate_edges} accent="violet" />
        <Metric label="Data issues" value={stats?.issues} accent="amber" />
      </div>
      <div className="two-column">
        <section className="panel">
          <PanelTitle title="Node landscape" subtitle="Tipe node aktual dari dataset" />
          <div className="type-list">
            {stats?.node_types.map((item, index) => (
              <div key={item.node_type}><span className={`type-dot c${index % 6}`} /><span>{human(item.node_type)}</span><b>{format(item.count)}</b></div>
            ))}
          </div>
        </section>
        <section className="panel">
          <PanelTitle title="Dataset provenance" subtitle="Output ETL yang terakhir di-ingest" />
          <div className="workbook-list">
            {active.workbooks.map((file) => <div key={file}><CheckIcon /><span>{file}</span></div>)}
          </div>
        </section>
      </div>
    </section>
  )
}

// ---------------------------------------------------------------------------
// Halaman ETL — upload file Excel mentah → knowledge graph otomatis
// ---------------------------------------------------------------------------

const ETL_PHASES: Record<string, number> = {
  'Memuat file Excel': 5, 'Membaca sheet Excel': 20, 'Membangun node RU & Plant': 25,
  'Membangun node Equipment': 35, 'Membangun node Maintenance Order': 45,
  'Membangun node RKAP Program': 55, 'Membangun node Reliability': 62,
  'Membangun node Inspection': 68, 'Membangun node ICU Issue': 74,
  'Membangun node Readiness': 76, 'Membangun node OA Data': 79,
  'Membangun node PLO': 82, 'Menulis output CSV': 88, 'Import ke database': 90,
}

const ETL_CHUNK_SIZE = 4 * 1024 * 1024 // 4 MB per chunk

function EtlUploadPanel({ name: datasetName, onNavigate, datasets, onRefreshDatasets }: { name: string; onNavigate: (p: Page) => void; datasets: DatasetSummary[]; onRefreshDatasets: () => Promise<void> }) {
  const [files, setFiles] = useState<File[]>([])
  const [job, setJob] = useState<ImportJob>()
  const [uploading, setUploading] = useState(false)
  const [uploadProgress, setUploadProgress] = useState<{ done: number; total: number } | null>(null)
  const [error, setError] = useState<string>()
  // Tujuan upload: default ke dataset terakhir jika ada, baru jika belum ada dataset
  const [target, setTarget] = useState<string>('__new__')
  useEffect(() => {
    if (datasets.length > 0) {
      const latest = [...datasets].sort((a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime())[0]
      setTarget(latest.id)
    }
  }, [datasets.length])

  useEffect(() => {
    if (!job || !['queued', 'running'].includes(job.status)) return
    const id = setInterval(async () => {
      const next = await api.importStatus(job.id).catch(() => null)
      if (next) setJob(next)
    }, 2000)
    return () => clearInterval(id)
  }, [job?.id, job?.status])

  const handleFiles = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files) setFiles(Array.from(e.target.files))
  }

  const isAppend = target !== '__new__'
  // Peringatkan kalau membuat dataset "baru" dengan nama yang sudah dipakai — penyebab
  // munculnya dua dataset kembar. Cocokkan case-insensitive.
  const nameCollision = !isAppend && datasets.some(
    d => d.name.trim().toLowerCase() === datasetName.trim().toLowerCase(),
  )

  const startEtl = async () => {
    if (!files.length) return
    setError(undefined)
    setUploading(true)
    setUploadProgress(null)
    try {
      const entries = files.map(f => ({
        file: f,
        name: f.name,
        total_chunks: Math.max(1, Math.ceil(f.size / ETL_CHUNK_SIZE)),
      }))
      const totalChunks = entries.reduce((s, e) => s + e.total_chunks, 0)
      let doneChunks = 0
      const targetDataset = isAppend ? datasets.find(d => d.id === target) : undefined
      const { upload_id } = await api.initChunkedUpload(
        isAppend ? (targetDataset?.name ?? datasetName) : datasetName,
        entries.map(e => ({ name: e.name, total_chunks: e.total_chunks })),
        isAppend ? 'etl_append' : 'etl',
        isAppend ? target : undefined,
      )
      for (const entry of entries) {
        for (let i = 0; i < entry.total_chunks; i++) {
          const chunk = entry.file.slice(i * ETL_CHUNK_SIZE, (i + 1) * ETL_CHUNK_SIZE)
          await api.uploadChunk(upload_id, entry.name, i, chunk)
          doneChunks++
          setUploadProgress({ done: doneChunks, total: totalChunks })
        }
      }
      const j = await api.commitChunkedUpload(upload_id)
      setJob(j)
      setFiles([])
      void onRefreshDatasets()
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Upload gagal.')
    } finally {
      setUploading(false)
      setUploadProgress(null)
    }
  }

  const isRunning = job?.status === 'queued' || job?.status === 'running'
  const isDone = job?.status === 'completed'
  const isFailed = job?.status === 'failed'

  return (
    <>

      {/* Form upload */}
      {!isRunning && !isDone && (
        <section className="panel import-action" style={{ flexDirection: 'column', alignItems: 'stretch', gap: '12px' }}>
          <div>
            <h2 style={{ margin: '0 0 4px' }}>Upload Data Mentah (Excel)</h2>
            <p style={{ margin: 0, color: 'var(--muted)', fontSize: 'var(--fs-xs)' }}>Upload file Excel SAP/maintenance langsung — ETL otomatis menghasilkan knowledge graph siap pakai.</p>
          </div>
          <div style={{ display: 'grid', gap: '6px' }}>
            <label htmlFor="etl-target">Tujuan upload</label>
            <select id="etl-target" value={target} onChange={e => setTarget(e.target.value)} disabled={uploading}>
              <option value="__new__">➕ Buat dataset baru — {datasetName}</option>
              {datasets.length > 0 && <optgroup label="Tambahkan ke dataset yang sudah ada (gabung)">
                {datasets.map(d => (
                  <option key={d.id} value={d.id}>{d.name} — {d.node_count.toLocaleString('id-ID')} node</option>
                ))}
              </optgroup>}
            </select>
            <p style={{ margin: 0, color: 'var(--muted)', fontSize: '12px' }}>
              {isAppend
                ? 'Mode gabung: file domain ini ditambahkan tanpa menghapus data lama. Setelah semua file diupload, buka Kelola Dataset → 🔗 Rebuild Relasi.'
                : 'Membuat dataset baru dari file yang dipilih. Untuk menambah domain lain nanti, upload lagi lalu pilih dataset ini di "Tujuan upload".'}
            </p>
            {nameCollision && (
              <p style={{ margin: 0, color: 'var(--warning, #b45309)', fontSize: '12px', fontWeight: 600 }}>
                ⚠️ Sudah ada dataset bernama sama. Kalau ini lanjutan data yang sama, sebaiknya pilih
                dataset itu di "Tambahkan ke dataset yang sudah ada" — bukan buat baru — supaya tidak
                muncul dua dataset kembar.
              </p>
            )}
          </div>
          <div style={{ display: 'grid', gap: '8px' }}>
            <label htmlFor="etl-files">Pilih file Excel (bisa multi-file sekaligus)</label>
            <input id="etl-files" type="file" accept=".xlsx,.xls" multiple onChange={handleFiles} />
            {files.length > 0 && (
              <div style={{ display: 'grid', gap: '4px', marginTop: '4px' }}>
                {files.map(f => (
                  <div key={f.name} style={{ fontSize: '13px', fontFamily: 'monospace', color: 'var(--muted)' }}>
                    {f.name} — {(f.size / 1024 / 1024).toFixed(1)} MB
                  </div>
                ))}
              </div>
            )}
          </div>
          {error && <p className="job-error">{error}</p>}
          {uploadProgress && (
            <div style={{ fontSize: '13px', color: 'var(--muted)' }}>
              Mengunggah… chunk {uploadProgress.done}/{uploadProgress.total}
              <div style={{ marginTop: '4px', height: '4px', background: 'var(--border)', borderRadius: '2px' }}>
                <div style={{ height: '100%', borderRadius: '2px', background: 'var(--accent)', width: `${Math.round(uploadProgress.done / uploadProgress.total * 100)}%`, transition: 'width .2s' }} />
              </div>
            </div>
          )}
          <button className="primary large" disabled={!files.length || uploading} onClick={() => void startEtl()}>
            {uploading ? 'Mengunggah…' : isAppend ? 'Proses ETL & Tambahkan ke Dataset' : 'Proses ETL & Buat Knowledge Graph'} <ChevronIcon />
          </button>
        </section>
      )}

      {/* Panduan nama file — collapsed by default */}
      {!isRunning && !isDone && (
        <details className="settings panel">
          <summary>Panduan deteksi otomatis nama file</summary>
          <p style={{ fontSize: '13px', color: 'var(--muted)', margin: '8px 0 12px' }}>
            ETL mendeteksi domain dari nama file. File dengan nama di luar pola ini tetap bisa diupload — hanya tidak akan terdeteksi domainnya dan dilewati.
          </p>
          <div className="file-grid">
            {[
              { name: 'all_ru_equipment_*.xlsx', desc: 'Master equipment (sheet: Sheet4)', required: true },
              { name: 'pt02_*.xlsx / pt03_*.xlsx', desc: 'Maintenance order & notification', required: false },
              { name: 'vw_reportirkapplanactual*.xlsx', desc: 'RKAP / cost program', required: false },
              { name: 'running_hours_*.xlsx / n_0_*.xlsx', desc: 'Reliability & running hours', required: false },
              { name: 'inspection_plan*.xlsx', desc: 'Inspection plan', required: false },
              { name: 'icu_database*.xlsx / icu*.xlsx', desc: 'ICU issue database', required: false },
              { name: 'apr_*.xlsx / readiness_atg*.xlsx', desc: 'Readiness & operasi', required: false },
              { name: 'rcps_db_*.xlsx', desc: 'RCPS (sheet: rcps, rekomendasi)', required: false },
              { name: 'issue_list*.xlsx / paf_issue*.xlsx', desc: 'Organization issue list', required: false },
              { name: 'oa_data*.xlsx', desc: 'Operational Availability, Allowance Unplanned & Issue List (3 sheet)', required: false },
              { name: 'plo_*.xlsx / plo*.xlsx', desc: 'Perizinan Layak Operasi (PLO) per instalasi', required: false },
            ].map(f => (
              <div key={f.name} className={`file-row status-${f.required ? 'ready' : 'optional'}`}>
                <div className="file-number">{f.required ? 'R' : 'O'}</div>
                <div className="file-info">
                  <strong style={{ fontFamily: 'monospace', fontSize: '13px' }}>{f.name}</strong>
                  <span>{f.desc}</span>
                </div>
              </div>
            ))}
          </div>
          <p style={{ marginTop: '10px', fontSize: '12px', color: 'var(--muted)' }}>R = Wajib · O = Opsional</p>
        </details>
      )}

      {/* Progress ETL */}
      {(isRunning || isDone || isFailed) && job && (
        <section className="panel" style={{ display: 'flex', flexDirection: 'column', gap: '14px' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
            <div>
              <span className="eyebrow">{job.phase}</span>
              <h3 style={{ margin: '4px 0 0' }}>{job.name}</h3>
            </div>
            <b style={{ fontSize: '20px' }}>{job.progress}%</b>
          </div>

          {/* Progress bar keseluruhan */}
          <div style={{ height: '8px', background: 'var(--border)', borderRadius: '4px', overflow: 'hidden' }}>
            <div style={{
              height: '100%', width: `${job.progress}%`,
              background: isDone ? 'var(--green, #22c55e)' : isFailed ? 'var(--red, #ef4444)' : 'var(--blue, #3b82f6)',
              transition: 'width 0.3s'
            }} />
          </div>

          {/* Tahapan ETL */}
          <div style={{ display: 'grid', gap: '6px' }}>
            {Object.entries(ETL_PHASES).map(([phase, pct]) => {
              const done = job.progress > pct
              const active = job.phase === phase
              return (
                <div key={phase} style={{ display: 'flex', alignItems: 'center', gap: '8px', fontSize: '13px' }}>
                  <span style={{
                    width: '16px', height: '16px', borderRadius: '50%', flexShrink: 0,
                    background: done ? 'var(--green, #22c55e)' : active ? 'var(--blue, #3b82f6)' : 'var(--border)',
                    display: 'flex', alignItems: 'center', justifyContent: 'center',
                    fontSize: '10px', color: done || active ? 'white' : 'transparent'
                  }}>
                    {done ? '✓' : active ? '…' : ''}
                  </span>
                  <span style={{ color: done ? 'inherit' : active ? 'var(--blue, #3b82f6)' : 'var(--muted)' }}>
                    {phase}
                  </span>
                </div>
              )
            })}
          </div>

          {job.message && <p style={{ fontSize: '13px', color: 'var(--muted)', margin: 0 }}>{job.message}</p>}
          {job.error && <p className="job-error">{job.error}</p>}

          {/* Notifikasi aman tutup */}
          {isRunning && job.progress >= 88 && (
            <div style={{ padding: '10px 14px', background: 'var(--green-subtle, #dcfce7)', borderRadius: '8px', border: '1px solid var(--green, #22c55e)', fontSize: '13px' }}>
              <b style={{ color: 'var(--green, #16a34a)' }}>Import berjalan di server</b>
              <p style={{ margin: '4px 0 0' }}>Browser sudah aman untuk ditutup. Proses tetap berjalan di background Railway.</p>
            </div>
          )}

          {/* Selesai */}
          {isDone && (
            <div style={{ display: 'flex', flexDirection: 'column', gap: '10px' }}>
              <div style={{ padding: '12px 16px', background: 'var(--green-subtle, #dcfce7)', borderRadius: '8px', border: '1px solid var(--green, #22c55e)' }}>
                <b style={{ color: 'var(--green, #16a34a)' }}>✓ Knowledge Graph berhasil dibuat</b>
                <p style={{ margin: '4px 0 0', fontSize: '13px' }}>{job.message}</p>
              </div>
              {job.warnings && job.warnings.length > 0 && (
                <div style={{ padding: '12px 16px', background: 'var(--warning-subtle, #fefce8)', borderRadius: '8px', border: '1px solid var(--warning, #ca8a04)' }}>
                  <b style={{ color: 'var(--warning, #ca8a04)', fontSize: '13px' }}>⚠ Peringatan builder ({job.warnings.length})</b>
                  <ul style={{ margin: '6px 0 0', paddingLeft: '18px', fontSize: '12px', display: 'flex', flexDirection: 'column', gap: '2px' }}>
                    {job.warnings.map((w, i) => <li key={i}>{w}</li>)}
                  </ul>
                </div>
              )}
              <div style={{ display: 'flex', gap: '8px' }}>
                <button className="primary" onClick={() => onNavigate('graph')}>Buka Graph Explorer <ChevronIcon /></button>
                <button className="secondary" onClick={() => { setJob(undefined); setError(undefined) }}>Upload lagi</button>
              </div>
            </div>
          )}

          {isFailed && (
            <button className="secondary" onClick={() => { setJob(undefined); setError(undefined) }}>Coba lagi</button>
          )}
        </section>
      )}
    </>
  )
}

const CHUNK_SIZE = 5 * 1024 * 1024 // 5 MB

type ChunkFileState = { file: File; uploaded: number; total: number; done: boolean }

type UploadPhase = 'idle' | 'uploading' | 'committed' | 'done' | 'error'

function ChunkedUploadPanel({ name, onJobStart, disabled }: { name: string; onJobStart: (job: ImportJob) => void; disabled: boolean }) {
  const [files, setFiles] = useState<Record<string, ChunkFileState>>({})
  const [phase, setPhase] = useState<UploadPhase>('idle')
  const [activeFile, setActiveFile] = useState<string>()
  const [error, setError] = useState<string>()
  const inputRef = useRef<HTMLInputElement>(null)

  const addFiles = (incoming: FileList | null) => {
    if (!incoming) return
    setFiles(prev => {
      const next = { ...prev }
      Array.from(incoming).forEach(f => {
        next[f.name] = { file: f, uploaded: 0, total: Math.ceil(f.size / CHUNK_SIZE), done: false }
      })
      return next
    })
  }

  const removeFile = (fn: string) => setFiles(prev => { const n = { ...prev }; delete n[fn]; return n })

  const totalChunks = Object.values(files).reduce((s, f) => s + f.total, 0)
  const uploadedChunks = Object.values(files).reduce((s, f) => s + f.uploaded, 0)
  const uploadPct = totalChunks > 0 ? Math.round((uploadedChunks / totalChunks) * 100) : 0

  const startUpload = async () => {
    setError(undefined)
    const entries = Object.entries(files).filter(([, s]) => s.file)
    if (!entries.some(([n]) => n === 'nodes.csv') || !entries.some(([n]) => n === 'relationships.csv')) {
      setError('nodes.csv dan relationships.csv wajib disertakan.')
      return
    }
    setPhase('uploading')
    try {
      const { upload_id } = await api.initChunkedUpload(name, entries.map(([n, s]) => ({ name: n, total_chunks: s.total })))
      for (const [fileName, state] of entries) {
        setActiveFile(fileName)
        for (let i = 0; i < state.total; i++) {
          const start = i * CHUNK_SIZE
          const chunk = state.file.slice(start, start + CHUNK_SIZE)
          await api.uploadChunk(upload_id, fileName, i, chunk)
          setFiles(prev => ({ ...prev, [fileName]: { ...prev[fileName], uploaded: i + 1 } }))
        }
        setFiles(prev => ({ ...prev, [fileName]: { ...prev[fileName], done: true } }))
      }
      setActiveFile(undefined)
      const job = await api.commitChunkedUpload(upload_id)
      onJobStart(job)
      setPhase('committed')
      setFiles({})
    } catch (e) {
      setPhase('error')
      setError(e instanceof Error ? e.message : 'Upload gagal.')
    }
  }

  const hasRequired = files['nodes.csv'] && files['relationships.csv']
  const fileList = Object.entries(files)
  const filePct = (f: ChunkFileState) => f.total > 0 ? Math.round((f.uploaded / f.total) * 100) : 0
  const fileMb = (f: ChunkFileState) => (f.file.size / 1024 / 1024).toFixed(1)

  return (
    <section className="panel chunked-panel">
      <div className="chunked-panel-header">
        <div>
          <label className="chunked-panel-title">Upload CSV langsung — chunked</label>
          <p className="chunked-panel-sub">Pilih satu atau beberapa file CSV sekaligus. File dipotong otomatis {CHUNK_SIZE / 1024 / 1024} MB/chunk · <b>nodes.csv</b> dan <b>relationships.csv</b> wajib ada.</p>
        </div>
      </div>

      {/* Drop zone / file picker */}
      <div
        className={`chunked-dropzone${phase === 'uploading' ? ' disabled' : ''}`}
        onClick={() => phase !== 'uploading' && inputRef.current?.click()}
        onDragOver={e => { e.preventDefault() }}
        onDrop={e => { e.preventDefault(); if (phase !== 'uploading') addFiles(e.dataTransfer.files) }}
      >
        <UploadIcon />
        <span>Klik atau seret file CSV ke sini</span>
        <small>Bisa pilih banyak file sekaligus</small>
        <input ref={inputRef} type="file" accept=".csv" multiple style={{ display: 'none' }} onChange={e => addFiles(e.target.files)} />
      </div>

      {/* File list */}
      {fileList.length > 0 && (
        <div className="chunked-file-list">
          {fileList.map(([fn, state]) => {
            const pct = filePct(state)
            const isActive = activeFile === fn
            const isRequired = fn === 'nodes.csv' || fn === 'relationships.csv'
            return (
              <div key={fn} className="chunked-file-row">
                <div className="chunked-file-info">
                  <span className="chunked-file-name">
                    {fn}
                    {isRequired && <span className="chunked-required">wajib</span>}
                  </span>
                  <span className="chunked-file-meta">
                    {state.done ? '✓ selesai' : isActive ? `${pct}% · chunk ${state.uploaded}/${state.total}` : `${fileMb(state)} MB · siap`}
                  </span>
                </div>
                {phase === 'uploading' && (
                  <div className="chunked-file-bar">
                    <div style={{ width: `${pct}%`, background: state.done ? '#22c55e' : '#3b82f6' }} />
                  </div>
                )}
                {phase !== 'uploading' && (
                  <button className="chunked-remove" onClick={() => removeFile(fn)} title="Hapus file ini">✕</button>
                )}
              </div>
            )
          })}
        </div>
      )}

      {/* Total progress */}
      {phase === 'uploading' && (
        <div className="chunked-progress">
          <div className="chunked-progress-label">
            <span>{activeFile ? `Mengupload: ${activeFile}` : 'Memproses…'}</span>
            <b>{uploadPct}%</b>
          </div>
          <div className="chunked-progress-bar">
            <div style={{ width: `${uploadPct}%` }} />
          </div>
          <p className="chunked-notice">Jangan tutup browser selama upload berlangsung.</p>
        </div>
      )}

      {phase === 'committed' && (
        <div className="chunked-success">
          <b>✓ Upload selesai — import berjalan di server</b>
          <p>Browser sudah aman untuk ditutup. Proses import tetap berjalan di background.</p>
        </div>
      )}

      {error && <p className="job-error">{error}</p>}

      {phase !== 'committed' && (
        <button className="secondary large" disabled={!hasRequired || phase === 'uploading' || disabled} onClick={() => void startUpload()}>
          {phase === 'uploading' ? `Mengunggah… ${uploadPct}%` : `Upload & Import CSV${fileList.length > 0 ? ` (${fileList.length} file)` : ''}`} <ChevronIcon />
        </button>
      )}
    </section>
  )
}

function ImportCenter({
  scan, job, busy, onScan, onStart, onZip, onCancel, onFolder, onNavigate,
  datasets, onRefreshDatasets,
}: {
  scan?: FolderScan
  job?: ImportJob
  busy: boolean
  onScan: () => void
  onStart: (name: string) => Promise<void>
  onZip: (file: File, name: string) => Promise<void>
  onCancel: () => void
  onFolder: (path: string) => Promise<void>
  onNavigate: (p: Page) => void
  datasets: DatasetSummary[]
  onRefreshDatasets: () => Promise<void>
}) {
  const [name, setName] = useState(() => {
    const now = new Date()
    const tgl = now.toLocaleDateString('id-ID')
    const jam = now.toLocaleTimeString('id-ID', { hour: '2-digit', minute: '2-digit' }).replace(/[.:]/g, '.')
    return `KG Kilang ${tgl} ${jam}`
  })
  const [folder, setFolder] = useState(scan?.folder ?? '')
  const [zipFile, setZipFile] = useState<File>()
  const [chunkedJob, setChunkedJob] = useState<ImportJob>()
  useEffect(() => { if (scan?.folder) setFolder(scan.folder) }, [scan?.folder])
  useEffect(() => {
    if (!chunkedJob || !['queued', 'running'].includes(chunkedJob.status)) return
    const id = setInterval(async () => {
      const next = await api.importStatus(chunkedJob.id).catch(() => null)
      if (next) setChunkedJob(next)
    }, 2000)
    return () => clearInterval(id)
  }, [chunkedJob?.id, chunkedJob?.status])
  const ready = scan?.files.some((item) => ['Ready', 'Changed'].includes(item.status))
  const requiredMissing = scan?.files.filter((item) => item.required && item.status === 'Missing').length ?? 0

  return (
    <section className="stack">
      <EtlUploadPanel name={name} onNavigate={onNavigate} datasets={datasets} onRefreshDatasets={onRefreshDatasets} />
    </section>
  )
}

function ExecutiveDashboard({ dataset }: { dataset?: DatasetSummary }) {
  const { data: summary, computing } = useWarmingData<RuSummary>(
    dataset?.id, () => api.ruSummary(dataset!.id),
  )
  if (!dataset) return <NoDataset />
  const rows = summary?.equipment_summary ?? []
  const coverage = summary?.data_coverage ?? []
  const quality = summary?.relationship_quality ?? []
  return (
    <section className="stack">
      <div className="hero-panel compact-hero">
        <div><span className="eyebrow">Executive RU dashboard</span><h2>Ringkasan Refinery Unit</h2><p>Dashboard ini memakai file summary output ETL, bukan menghitung ulang graph besar saat halaman dibuka.</p></div>
      </div>
      {computing && <ComputingBanner />}
      <div className="metrics">
        <Metric label="Refinery units" value={rows.length || summary?.refinery_units.length} accent="mint" />
        <Metric label="Equipment total" value={sum(rows, 'equipment_count')} accent="blue" />
        <Metric label="Maintenance orders" value={sum(rows, 'maintenance_orders')} accent="violet" />
        <Metric label="RKAP programs" value={sum(rows, 'rkap_programs')} accent="blue" />
        <Metric label="Unmatched identifiers" value={sum(rows, 'unmatched_identifiers')} accent="amber" />
      </div>
      <section className="panel table-panel">
        <PanelTitle title="RU equipment summary" subtitle="Coverage operasional per Refinery Unit" />
        <table><thead><tr><th>RU</th><th>Site</th><th>Equipment</th><th>Critical</th><th>Maintenance</th><th>Reliability</th><th>Readiness linked</th><th>Readiness tag</th><th>RU readiness</th><th>RKAP</th><th>Link %</th></tr></thead>
          <tbody>{rows.map((row, index) => {
            const readinessDirect = Number(row.readiness_records_direct_linked ?? 0)
            const readinessTag = Number(row.readiness_records_tag_matched ?? 0)
            const readinessTotal = Number(row.readiness_records_total ?? 0)
            const readinessEffective = readinessDirect || readinessTag
            const readinessIsTagFallback = !readinessDirect && readinessTag > 0
            const readinessStatus = String(row.readiness_semantic_status ?? (readinessDirect ? 'Direct linked' : readinessTag ? 'Tag matched' : readinessTotal ? 'RU only' : 'No readiness'))
            return <tr key={index}><td>{String(row.refinery_unit ?? '—')}</td><td>{String(row.site_name ?? '—')}</td><td>{format(Number(row.equipment_count ?? 0))}</td><td>{format(Number(row.critical_equipment_count ?? 0))}</td><td>{format(Number(row.maintenance_orders ?? 0))}</td><td>{format(Number(row.reliability_observations ?? 0))}</td><td>{format(readinessEffective)}{readinessIsTagFallback ? <small className="table-note">{readinessStatus}</small> : null}</td><td>{format(readinessTag)}</td><td>{format(readinessTotal)}</td><td>{format(Number(row.rkap_programs ?? 0))}</td><td>{String(row.overall_equipment_link_percentage ?? '—')}</td></tr>
          })}</tbody>
        </table>
      </section>
      <div className="two-column balanced">
        <section className="panel table-panel fit">
          <PanelTitle title="Data coverage" subtitle="Linked-to-equipment percentage per domain" />
          <Paged items={coverage}>{(rows) => (
            <table><thead><tr><th>RU</th><th>Domain</th><th>Total</th><th>Linked</th><th>%</th></tr></thead>
              <tbody>{rows.map((row, index) => <tr key={index}><td>{String(row.refinery_unit ?? '—')}</td><td>{human(String(row.domain ?? '—'))}</td><td>{format(Number(row.total_records ?? 0))}</td><td>{format(Number(row.linked_to_equipment ?? 0))}</td><td>{String(row.equipment_link_percentage ?? '—')}</td></tr>)}</tbody>
            </table>
          )}</Paged>
        </section>
        <section className="panel table-panel fit">
          <PanelTitle title="Relationship quality" subtitle="Metode match dan confidence per relationship" />
          <Paged items={quality}>{(rows) => (
            <table><thead><tr><th>RU</th><th>Relationship</th><th>Method</th><th>Count</th><th>Avg conf.</th></tr></thead>
              <tbody>{rows.map((row, index) => <tr key={index}><td>{String(row.refinery_unit ?? '—')}</td><td>{human(String(row.relationship_type ?? '—'))}</td><td>{String(row.match_method ?? '—')}</td><td>{format(Number(row.relationship_count ?? 0))}</td><td>{String(row.average_confidence ?? '—')}</td></tr>)}</tbody>
            </table>
          )}</Paged>
        </section>
      </div>
    </section>
  )
}

function ReliabilityInsightPage({ dataset, onNavigate }: { dataset?: DatasetSummary; onNavigate: (page: Page) => void }) {
  const [role, setRole] = useState<'manager' | 'vp'>('manager')
  const { data: insight, computing } = useWarmingData<ReliabilityInsight>(
    dataset?.id, () => api.reliabilityInsight(dataset!.id),
  )
  if (!dataset) return <NoDataset />
  const kpis = insight?.kpis ?? {}
  const crossKpis = insight?.cross_domain_kpis ?? {}
  const ruPortfolio = insight?.ru_reliability_portfolio ?? insight?.ru_ranking ?? []
  const mtbfRows = insight?.mtbf_mttr_by_ru ?? []
  const highRisk = insight?.high_risk_equipment ?? []
  const actionQueue = insight?.equipment_action_queue ?? []
  const statusRows = insight?.status_distribution ?? []
  const coverageAlerts = insight?.coverage_alerts ?? []
  const qualityAlerts = insight?.relationship_quality_alerts ?? []
  const dataQualityBacklog = insight?.data_quality_backlog ?? []
  return (
    <section className="stack insight-page">
      <div className="hero-panel insight-hero">
        <div>
          <span className="eyebrow">Reliability command center</span>
          <h2>Manager & VP Reliability Insight</h2>
          <p>Prioritas reliability lintas MTBF, MTTR, readiness, RKAP, issue, dan confidence data graph.</p>
        </div>
        <div className="segmented">
          <button className={role === 'manager' ? 'active' : ''} onClick={() => setRole('manager')}>Manager view</button>
          <button className={role === 'vp' ? 'active' : ''} onClick={() => setRole('vp')}>VP view</button>
        </div>
      </div>

      {computing && <ComputingBanner />}

      <div className="metrics">
        <Metric label="Reliability risk equipment" value={Number(crossKpis.reliability_risk_equipment ?? highRisk.length)} accent="blue" />
        <Metric label="Readiness-linked records" value={Number(crossKpis.readiness_linked_records ?? 0)} accent="mint" />
        <Metric label="RKAP programs" value={Number(crossKpis.rkap_linked_programs ?? 0)} accent="violet" />
        <Metric label="Data confidence" value={Number(crossKpis.data_confidence ?? 0)} accent="amber" />
      </div>

      {role === 'manager' ? (
        <div className="stack insight-layout">
          <section className="panel table-panel fit">
            <PanelTitle title="Equipment action queue" subtitle="Prioritas lintas reliability, readiness, issue, dan RKAP" />
            <Paged items={actionQueue}>{(rows) => (
              <table><thead><tr><th>Equipment</th><th>RU</th><th>Risk</th><th>MTBF</th><th>MTTR</th><th>Issue</th><th>Ready</th><th>RKAP</th><th>Action</th></tr></thead>
                <tbody>{rows.map((row, index) => <tr key={index}><td><span className="mono">{String(row.equipment_label ?? row.equipment_key ?? '—')}</span></td><td>{String(row.refinery_unit ?? '—')}</td><td><RiskPill value={Number(row.risk_score ?? 0)} /></td><td>{decimal(row.avg_mtbf)}</td><td>{decimal(row.avg_mttr)}</td><td>{format(Number(row.issue_count ?? 0))}</td><td>{format(Number(row.readiness_records ?? 0))}</td><td>{format(Number(row.rkap_programs ?? 0))}</td><td><button className="link-button" onClick={() => onNavigate('graph')}>Open graph</button></td></tr>)}</tbody>
              </table>
            )}</Paged>
          </section>
          <div className="two-column balanced">
            <section className="panel table-panel fit">
              <PanelTitle title="MTBF / MTTR by Refinery Unit" subtitle="Operational reliability health per RU" />
              <Paged items={mtbfRows}>{(rows) => (
                <table><thead><tr><th>RU</th><th>Obs.</th><th>Equipment</th><th>Avg MTBF</th><th>Avg MTTR</th><th>Avg RH</th><th>Abnormal</th></tr></thead>
                  <tbody>{rows.map((row, index) => <tr key={index}><td>{String(row.refinery_unit ?? '—')}</td><td>{format(Number(row.observations ?? 0))}</td><td>{format(Number(row.observed_equipment ?? 0))}</td><td>{decimal(row.avg_mtbf)}</td><td>{decimal(row.avg_mttr)}</td><td>{decimal(row.avg_running_hours)}</td><td>{format(Number(row.abnormal_status_count ?? 0))}</td></tr>)}</tbody>
                </table>
              )}</Paged>
            </section>
            <section className="panel table-panel fit">
              <PanelTitle title="Reliability-only high risk" subtitle="Baseline risk dari MTBF, MTTR, RH ekstrem, dan status abnormal" />
              <Paged items={highRisk}>{(rows) => (
                <table><thead><tr><th>Equipment</th><th>RU</th><th>Risk</th><th>MTBF</th><th>MTTR</th><th>Action</th></tr></thead>
                  <tbody>{rows.map((row, index) => <tr key={index}><td><span className="mono">{String(row.equipment_label ?? row.equipment_key ?? '—')}</span></td><td>{String(row.refinery_unit ?? '—')}</td><td><RiskPill value={Number(row.risk_score ?? 0)} /></td><td>{decimal(row.avg_mtbf)}</td><td>{decimal(row.avg_mttr)}</td><td><button className="link-button" onClick={() => onNavigate('graph')}>Open graph</button></td></tr>)}</tbody>
                </table>
              )}</Paged>
            </section>
          </div>
          <div className="two-column balanced">
            <section className="panel table-panel fit">
              <PanelTitle title="Status distribution" subtitle="Dominant reliability status per RU" />
              <Paged items={statusRows}>{(rows) => (
                <table><thead><tr><th>RU</th><th>Status</th><th>Count</th></tr></thead>
                  <tbody>{rows.map((row, index) => <tr key={index}><td>{String(row.refinery_unit ?? '—')}</td><td>{String(row.status ?? 'Unknown')}</td><td>{format(Number(row.count ?? 0))}</td></tr>)}</tbody>
                </table>
              )}</Paged>
            </section>
            <section className="panel insight-note">
              <PanelTitle title="Manager action notes" subtitle="Cara membaca insight v1" />
              <ul>
                <li>Prioritaskan action queue tertinggi untuk review RCA, PM strategy, atau readiness follow-up.</li>
                <li>MTBF = 0 atau sangat rendah biasanya perlu validasi data dan failure mode.</li>
                <li>MTTR tinggi mengarah ke isu maintainability, spare part, atau execution delay.</li>
                <li>RKAP kosong pada equipment berisiko tinggi dapat menjadi gap mitigation planning.</li>
              </ul>
            </section>
          </div>
        </div>
      ) : (
        <div className="stack insight-layout">
          <section className="panel table-panel fit">
            <PanelTitle title="RU reliability portfolio" subtitle="Executive attention list lintas reliability, readiness, RKAP, dan confidence" />
            <Paged items={ruPortfolio}>{(rows) => (
              <table><thead><tr><th>Rank</th><th>RU</th><th>Site</th><th>Risk</th><th>MTBF</th><th>MTTR</th><th>Ready</th><th>RKAP</th><th>Confidence</th></tr></thead>
                <tbody>{rows.map((row, index) => <tr key={index}><td>{ruPortfolio.indexOf(row) + 1}</td><td>{String(row.refinery_unit ?? '—')}</td><td>{String(row.site_name ?? '—')}</td><td><RiskPill value={Number(row.risk_score ?? 0)} /></td><td>{decimal(row.avg_mtbf)}</td><td>{decimal(row.avg_mttr)}</td><td>{format(Number(row.readiness_records ?? 0))}</td><td>{format(Number(row.rkap_programs ?? 0))}</td><td>{row.data_confidence != null ? `${decimal(row.data_confidence)}%` : String(row.overall_equipment_link_percentage ?? '—')}</td></tr>)}</tbody>
              </table>
            )}</Paged>
          </section>
          <div className="two-column balanced">
            <section className="panel table-panel fit">
              <PanelTitle title="Data confidence backlog" subtitle="Prioritas cleanup coverage rendah dan candidate relationship tinggi" />
              <Paged items={dataQualityBacklog}>{(rows) => (
                <table><thead><tr><th>Type</th><th>RU</th><th>Domain / Relationship</th><th>Volume</th><th>Score</th></tr></thead>
                  <tbody>{rows.map((row, index) => <tr key={index}><td>{human(String(row.backlog_type ?? '—'))}</td><td>{String(row.refinery_unit ?? '—')}</td><td>{human(String(row.domain ?? row.relationship_type ?? '—'))}</td><td>{format(Number(row.total_records ?? row.candidate_count ?? 0))}</td><td>{decimal(row.priority_score)}</td></tr>)}</tbody>
                </table>
              )}</Paged>
            </section>
            <section className="panel table-panel fit">
              <PanelTitle title="Relationship quality alerts" subtitle="Low confidence atau minimum confidence rendah" />
              <Paged items={qualityAlerts}>{(rows) => (
                <table><thead><tr><th>RU</th><th>Relationship</th><th>Method</th><th>Count</th><th>Avg / Min</th></tr></thead>
                  <tbody>{rows.map((row, index) => <tr key={index}><td>{String(row.refinery_unit ?? '—')}</td><td>{human(String(row.relationship_type ?? '—'))}</td><td>{String(row.match_method ?? '—')}</td><td>{format(Number(row.relationship_count ?? 0))}</td><td>{String(row.average_confidence ?? '—')} / {String(row.minimum_confidence ?? '—')}</td></tr>)}</tbody>
                </table>
              )}</Paged>
            </section>
          </div>
          <div className="two-column balanced">
            <section className="panel table-panel fit">
              <PanelTitle title="Coverage alerts" subtitle="Domain/RU dengan equipment link percentage rendah" />
              <Paged items={coverageAlerts}>{(rows) => (
                <table><thead><tr><th>RU</th><th>Domain</th><th>Total</th><th>Linked</th><th>%</th></tr></thead>
                  <tbody>{rows.map((row, index) => <tr key={index}><td>{String(row.refinery_unit ?? '—')}</td><td>{human(String(row.domain ?? '—'))}</td><td>{format(Number(row.total_records ?? 0))}</td><td>{format(Number(row.linked_to_equipment ?? 0))}</td><td>{String(row.equipment_link_percentage ?? '—')}</td></tr>)}</tbody>
                </table>
              )}</Paged>
            </section>
            <section className="panel insight-note">
              <PanelTitle title="VP decision lens" subtitle="Apa yang perlu ditindaklanjuti" />
              <ul>
                <li>RU portfolio memberi fokus governance bulanan dan challenge session reliability.</li>
                <li>Readiness dan RKAP menunjukkan exposure operasional dan mitigasi yang sudah terhubung.</li>
                <li>Data confidence backlog menunjukkan area cleanup sebelum keputusan produksi.</li>
                <li>Drill ke Manager view untuk equipment action queue per RU.</li>
              </ul>
            </section>
          </div>
        </div>
      )}
    </section>
  )
}

function GraphExplorer({ dataset, stats }: { dataset?: DatasetSummary; stats?: DatasetStats }) {
  const [query, setQuery] = useState('')
  const [results, setResults] = useState<GraphNode[]>([])
  const [root, setRoot] = useState<GraphNode>()
  const [selected, setSelected] = useState<GraphNode>()
  const [selectedEdge, setSelectedEdge] = useState<GraphEdge>()
  const [graph, setGraph] = useState<GraphSlice>(emptyGraph)
  const [graphSource, setGraphSource] = useState<GraphSource>('empty')
  const [graphQuery, setGraphQuery] = useState('')
  const [queryMetadata, setQueryMetadata] = useState<QueryMetadata>()
  const [queryBuilder, setQueryBuilder] = useState<QueryBuilderState>(defaultQueryBuilder)
  const generatedPropertyQuery = useMemo(() => buildPropertyQuery(queryBuilder), [queryBuilder])
  const [propertyQuery, setPropertyQuery] = useState(generatedPropertyQuery)
  const [advancedMode, setAdvancedMode] = useState(false)
  const [queryError, setQueryError] = useState('')
  const [queryLimit, setQueryLimit] = useState(200)
  const [mode, setMode] = useState<'neighborhood' | 'directed'>('neighborhood')
  const [depth, setDepth] = useState(1)
  const [includeCandidates, setIncludeCandidates] = useState(false)
  const [minConfidence, setMinConfidence] = useState(.8)
  const [relation, setRelation] = useState('')
  const [nodeType, setNodeType] = useState('')
  const [refineryUnit, setRefineryUnit] = useState('')
  const [equipmentCode, setEquipmentCode] = useState('')
  const [applied, setApplied] = useState({ mode: 'neighborhood' as 'neighborhood' | 'directed', depth: 1, includeCandidates: false, minConfidence: .8, relation: '', nodeType: '', refineryUnit: '', equipmentCode: '' })
  const [limit, setLimit] = useState(300)
  const [loading, setLoading] = useState(false)
  const [leftOpen, setLeftOpen] = useState(true)
  const [inspectorOpen, setInspectorOpen] = useState(true)
  const [queryPanelOpen, setQueryPanelOpen] = useState(false)

  const [searching, setSearching] = useState(false)
  const searchSeq = useRef(0)
  const search = useCallback(async () => {
    if (!dataset) return
    const seq = ++searchSeq.current
    setSearching(true)
    try {
      const found = await api.search(dataset.id, query, nodeType, '', 30, refineryUnit, equipmentCode)
      if (seq === searchSeq.current) setResults(found)
    } catch (reason) {
      console.error('Pencarian gagal', reason)
      if (seq === searchSeq.current) setResults([])
    } finally {
      if (seq === searchSeq.current) setSearching(false)
    }
  }, [dataset, query, nodeType, refineryUnit, equipmentCode])
  useEffect(() => {
    const timer = setTimeout(() => { void search() }, 250)
    return () => clearTimeout(timer)
  }, [search])
  useEffect(() => {
    if (!dataset) {
      setQueryMetadata(undefined)
      return
    }
    void api.queryMetadata(dataset.id).then(setQueryMetadata).catch(() => setQueryMetadata(undefined))
  }, [dataset])
  useEffect(() => {
    if (!advancedMode) setPropertyQuery(generatedPropertyQuery)
  }, [advancedMode, generatedPropertyQuery])

  const queryTypeOptions = Array.from(new Set(queryBuilder.entity === 'NODE'
    ? (queryMetadata?.node_types.map((item) => item.type) ?? stats?.node_types.map((item) => item.node_type) ?? [])
    : (queryMetadata?.edge_types.map((item) => item.type) ?? stats?.edge_types.map((item) => item.relationship_type) ?? [])))
  const selectedTypeFields = queryBuilder.entity === 'NODE'
    ? queryMetadata?.node_types.find((item) => item.type === queryBuilder.type)?.fields
    : queryMetadata?.edge_types.find((item) => item.type === queryBuilder.type)?.fields
  const queryFieldOptions = selectedTypeFields ?? (queryBuilder.entity === 'NODE'
    ? queryMetadata?.core_node_fields
    : queryMetadata?.core_edge_fields) ?? ['label', 'business_key', 'domain', 'source_file']

  const loadGraph = useCallback(async (node: GraphNode, nextLimit = limit) => {
    if (!dataset) return
    setLoading(true)
    try {
      setRoot(node)
      setSelected(node)
      setSelectedEdge(undefined)
      setLimit(nextLimit)
      setGraphSource(applied.mode)
      setGraphQuery('')
      setGraph(applied.mode === 'directed'
        ? await api.directedDescendants(dataset.id, node.id, {
          minDepth: 3,
          maxDepth: Math.max(3, applied.depth),
          relationshipType: applied.relation,
          includeCandidates: applied.includeCandidates,
          limit: nextLimit,
        })
        : await api.neighbors(dataset.id, node.id, {
          depth: applied.depth,
          includeCandidates: applied.includeCandidates,
          minConfidence: applied.minConfidence,
          relationshipType: applied.relation,
          // Filter node type/RU/equipment di panel kiri hanya untuk PENCARIAN start node.
          // Jangan dipakai men-scope neighborhood graph (mis. node type=Equipment akan
          // membuang semua tetangga -> graph runtuh jadi 1 node saat "Apply filters").
          nodeType: '',
          refineryUnit: '',
          equipmentCode: '',
          limit: nextLimit,
        }))
    } finally {
      setLoading(false)
    }
  }, [dataset, applied, limit])

  const applyFilters = async () => {
    const next = { mode, depth, includeCandidates, minConfidence, relation, nodeType, refineryUnit, equipmentCode }
    setApplied(next)
    if (root && dataset) {
      setLoading(true)
      try {
        setGraph(next.mode === 'directed'
          ? await api.directedDescendants(dataset.id, root.id, {
            minDepth: 3,
            maxDepth: Math.max(3, next.depth),
            relationshipType: next.relation,
            includeCandidates: next.includeCandidates,
            limit,
          })
          : await api.neighbors(dataset.id, root.id, {
            depth: next.depth,
            includeCandidates: next.includeCandidates,
            minConfidence: next.minConfidence,
            relationshipType: next.relation,
            // Lihat catatan di loadGraph: filter pencarian tak boleh men-scope graph.
            nodeType: '',
            refineryUnit: '',
            equipmentCode: '',
            limit,
          }))
        setGraphSource(next.mode)
        setGraphQuery('')
      } finally {
        setLoading(false)
      }
    }
  }

  const expandSelected = () => {
    if (selected) void loadGraph(selected, selected.kind === 'refinery_unit' ? 300 : 600)
  }

  const loadMore = () => {
    if (root) void loadGraph(root, Math.min(3000, limit + 300))
  }
  const applyPreset = (relationshipTypes: string[]) => {
    setMode('directed')
    setDepth(5)
    setRelation('')
    setApplied({ mode: 'directed', depth: 5, includeCandidates, minConfidence, relation: '', nodeType: '', refineryUnit, equipmentCode })
    if (root && dataset) {
      void api.directedDescendants(dataset.id, root.id, { minDepth: 3, maxDepth: 5, limit, includeCandidates }).then((slice) => {
        const allowed = new Set(relationshipTypes)
        setGraph({
          ...slice,
          edges: slice.edges.filter((edge) => allowed.has(edge.type)),
          paths: (slice.paths ?? []).filter((path) => path.relationship_path.some((rel) => allowed.has(rel))),
        })
        setGraphSource('directed')
        setGraphQuery(`Directed preset: ${relationshipTypes.join(' → ')}`)
      })
    }
  }
  const updateQueryCondition = (id: number, patch: Partial<QueryCondition>) => {
    setQueryBuilder((current) => ({
      ...current,
      conditions: current.conditions.map((condition) => condition.id === id ? { ...condition, ...patch } : condition),
    }))
    setQueryError('')
  }
  const addQueryCondition = () => {
    setQueryBuilder((current) => ({
      ...current,
      conditions: [...current.conditions, { id: Date.now(), field: queryFieldOptions[0] ?? 'label', operator: 'CONTAINS', value: '' }],
    }))
  }
  const removeQueryCondition = (id: number) => {
    setQueryBuilder((current) => ({
      ...current,
      conditions: current.conditions.length > 1 ? current.conditions.filter((condition) => condition.id !== id) : current.conditions,
    }))
  }
  const selectQueryEntity = (entity: QueryEntity) => {
    const firstType = entity === 'NODE'
      ? (queryMetadata?.node_types[0]?.type ?? stats?.node_types[0]?.node_type ?? 'equipment')
      : (queryMetadata?.edge_types[0]?.type ?? stats?.edge_types[0]?.relationship_type ?? 'EQUIPMENT_HAS_RKAP_PROGRAM')
    setQueryBuilder({ entity, type: firstType, conditions: [{ id: Date.now(), field: entity === 'NODE' ? 'label' : 'domain', operator: 'CONTAINS', value: '' }] })
    setQueryError('')
  }
  const applyQueryPreset = (builder: QueryBuilderState) => {
    setQueryBuilder({ ...builder, conditions: builder.conditions.map((condition, index) => ({ ...condition, id: Date.now() + index })) })
    setAdvancedMode(false)
    setQueryError('')
  }
  const runPropertyQuery = async () => {
    const nextQuery = advancedMode ? propertyQuery : generatedPropertyQuery
    const validation = advancedMode ? '' : validateQueryBuilder(queryBuilder)
    if (validation) {
      setQueryError(validation)
      return
    }
    if (!dataset || !nextQuery.trim()) return
    setLoading(true)
    setQueryError('')
    try {
      const slice = await api.propertyQuery(dataset.id, nextQuery, queryLimit)
      setGraph(slice)
      setRoot(slice.nodes[0])
      setSelected(slice.nodes[0])
      setSelectedEdge(undefined)
      setGraphSource('property')
      setGraphQuery(nextQuery)
      setApplied({ mode: 'neighborhood', depth, includeCandidates, minConfidence, relation: '', nodeType: '', refineryUnit: '', equipmentCode: '' })
    } catch (reason) {
      setQueryError(message(reason))
    } finally {
      setLoading(false)
    }
  }
  const selectNode = (node: GraphNode) => {
    setSelected(node)
    setSelectedEdge(undefined)
  }
  const selectEdge = (edge: GraphEdge) => {
    setSelectedEdge(edge)
    setSelected(undefined)
  }

  if (!dataset) return <NoDataset />
  return (
    <div className={`explorer-layout ${leftOpen ? '' : 'left-collapsed'} ${inspectorOpen ? '' : 'inspector-collapsed'}`}>
      <aside className="explorer-search panel">
        <div className="explorer-panel-title">
          <div><span className="eyebrow">Search & filters</span><strong>Start node</strong></div>
          <button className="icon-button mini" onClick={() => setLeftOpen(false)}>×</button>
        </div>
        <div className="search-box"><SearchIcon /><input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="RU, equipment, order, issue…" /></div>
        <div className="side-filters">
          <select value={nodeType} onChange={(event) => setNodeType(event.target.value)}>
            <option value="">Semua node type</option>
            {stats?.node_types.slice(0, 40).map((item) => <option key={item.node_type} value={item.node_type}>{human(item.node_type)}</option>)}
          </select>
          <input value={refineryUnit} onChange={(event) => setRefineryUnit(event.target.value)} placeholder="Filter RU, mis. RU II" />
          <input value={equipmentCode} onChange={(event) => setEquipmentCode(event.target.value)} placeholder="Equipment code" />
        </div>
        <div className="result-list">
          {searching && <div className="search-spinner"><span className="spinner-inline" />Mencari…</div>}
          {results.map((node) => (
            <button key={node.id} className={root?.id === node.id ? 'active' : ''} onClick={() => void loadGraph(node)}>
              <span className="result-icon"><EquipmentIcon /></span>
              <span><strong>{node.label}</strong><small>{node.subtitle || human(node.kind)}</small></span>
              <ChevronIcon />
            </button>
          ))}
        </div>
        <div className={`query-panel ${queryPanelOpen ? 'open' : 'collapsed'}`}>
          <div className="query-heading">
            <button className="query-toggle" onClick={() => setQueryPanelOpen((value) => !value)} aria-expanded={queryPanelOpen}>
              <span className="eyebrow">Property query</span>
              <ChevronIcon />
            </button>
            {queryPanelOpen && <button onClick={() => setAdvancedMode((value) => !value)}>{advancedMode ? 'Builder' : 'Advanced'}</button>}
          </div>
          {queryPanelOpen && <>
          {!advancedMode ? (
            <div className="query-builder">
              <div className="query-mode">
                <button className={queryBuilder.entity === 'NODE' ? 'active' : ''} onClick={() => selectQueryEntity('NODE')}>Node</button>
                <button className={queryBuilder.entity === 'EDGE' ? 'active' : ''} onClick={() => selectQueryEntity('EDGE')}>Edge</button>
              </div>
              <select value={queryBuilder.type} onChange={(event) => { setQueryBuilder((current) => ({ ...current, type: event.target.value })); setQueryError('') }}>
                {queryTypeOptions.map((item) => <option key={item} value={item}>{human(item)}</option>)}
                {!queryTypeOptions.includes(queryBuilder.type) && <option value={queryBuilder.type}>{human(queryBuilder.type)}</option>}
              </select>
              <div className="condition-list">
                {queryBuilder.conditions.map((condition) => (
                  <div className={`condition-row ${requiresValue(condition.operator) ? '' : 'no-value'}`} key={condition.id}>
                    <select value={condition.field} onChange={(event) => updateQueryCondition(condition.id, { field: event.target.value })}>
                      {queryFieldOptions.map((field) => <option key={field} value={field}>{human(field)}</option>)}
                      {!queryFieldOptions.includes(condition.field) && <option value={condition.field}>{human(condition.field)}</option>}
                    </select>
                    <select value={condition.operator} onChange={(event) => updateQueryCondition(condition.id, { operator: event.target.value as QueryOperator })}>
                      {queryOperators.map((operator) => <option key={operator} value={operator}>{human(operator.toLowerCase())}</option>)}
                    </select>
                    {requiresValue(condition.operator) && <input value={condition.value} onChange={(event) => updateQueryCondition(condition.id, { value: event.target.value })} placeholder={condition.operator === 'LIKE' || condition.operator === 'NOT LIKE' ? '%text%' : 'Value'} />}
                    <button className="icon-button mini" onClick={() => removeQueryCondition(condition.id)}>×</button>
                  </div>
                ))}
              </div>
              <button className="query-add" onClick={addQueryCondition}>+ Add condition</button>
              <div className="query-preview">{generatedPropertyQuery}</div>
            </div>
          ) : (
            <textarea value={propertyQuery} onChange={(event) => { setPropertyQuery(event.target.value); setQueryError('') }} spellCheck={false} />
          )}
          <div className="query-row">
            <input type="number" min="10" max="2000" value={queryLimit} onChange={(event) => setQueryLimit(Number(event.target.value))} />
            <button className="secondary small" onClick={() => void runPropertyQuery()}>Run query</button>
          </div>
          {queryError && <div className="query-error">{queryError}</div>}
          <div className="query-presets">
            {queryPresets.map((preset) => <button key={preset.label} onClick={() => applyQueryPreset(preset.builder)}>{preset.label}</button>)}
          </div>
          </>}
        </div>
      </aside>
      <section className="graph-stage panel">
        <div className="graph-header">
          <div className="graph-title-row">
            {!leftOpen && <button className="secondary small" onClick={() => setLeftOpen(true)}>Search</button>}
            <div><span className="eyebrow">{mode === 'directed' ? 'Directed descendants' : 'Neighborhood'}</span><h2>{root?.label ?? 'Pilih start node'}</h2></div>
          </div>
          <div className="graph-controls">
            <label>Mode <select value={mode} onChange={(event) => setMode(event.target.value as 'neighborhood' | 'directed')}><option value="neighborhood">Neighborhood</option><option value="directed">Directed descendants</option></select></label>
            <label>Hop <select value={depth} onChange={(event) => setDepth(Number(event.target.value))}>{[1, 2, 3, 4, 5].map((item) => <option key={item}>{item}</option>)}</select></label>
            <label>Relationship <select value={relation} onChange={(event) => setRelation(event.target.value)}><option value="">Semua</option>{stats?.edge_types.slice(0, 25).map((item) => <option key={`${item.relationship_type}-${item.is_candidate}`} value={item.relationship_type}>{human(item.relationship_type)}</option>)}</select></label>
            <label className="switch accent"><input type="checkbox" checked={includeCandidates} onChange={(event) => setIncludeCandidates(event.target.checked)} /><span />Candidate</label>
            <button className="secondary small" onClick={() => setInspectorOpen((value) => !value)}>{inspectorOpen ? 'Hide detail' : 'Show detail'}</button>
            <button className="primary small" onClick={() => void applyFilters()}>Apply filters</button>
          </div>
        </div>
        {includeCandidates && <div className="confidence"><span>Min confidence</span><input type="range" min="0" max="1" step=".05" value={minConfidence} onChange={(event) => setMinConfidence(Number(event.target.value))} /><b>{minConfidence.toFixed(2)}</b></div>}
        {mode === 'directed' && (
          <div className="graph-presets">
            <span>Directed presets</span>
            <button onClick={() => applyPreset(['REFINERY_UNIT_HAS_EQUIPMENT', 'EQUIPMENT_HAS_ISSUE', 'ISSUE_HAS_RCPS', 'RCPS_HAS_RECOMMENDATION'])}>RU → Issue → RCPS → Recommendation</button>
            <button onClick={() => applyPreset(['REFINERY_UNIT_HAS_EQUIPMENT', 'EQUIPMENT_HAS_RELIABILITY_OBSERVATION', 'OBSERVED_IN_PERIOD'])}>RU → Reliability → Time Period</button>
            <button onClick={() => applyPreset(['REFINERY_UNIT_HAS_EQUIPMENT', 'EQUIPMENT_HAS_MAINTENANCE_ORDER', 'MAINTENANCE_ORDER_HAS_NOTIFICATION'])}>RU → Maintenance → Notification</button>
            <button onClick={() => applyPreset(['REFINERY_UNIT_HAS_RKAP_PROGRAM', 'EQUIPMENT_HAS_RKAP_PROGRAM'])}>RU → RKAP Program</button>
          </div>
        )}
        {root && (
          <div className="graph-statusbar">
            <div className="graph-status-info">
              <span>{graph.nodes.length} nodes · {graph.edges.length} edges · limit {limit}</span>
              {applied.mode === 'directed' && graph.has_deep_descendants && <b>Has &gt;2 downstream levels · max depth {graph.max_depth_found}</b>}
              {graph.degree?.high_degree && <b>High-degree node: {format(graph.degree.total_edges)} edges</b>}
              {graph.high_degree_warning && <em>{graph.high_degree_warning}</em>}
            </div>
            <div className="graph-status-actions">
              <button className="secondary small" disabled={!selected || loading} onClick={expandSelected}>Expand selected node</button>
              <button className="secondary small" disabled={!graph.truncated || loading || limit >= 3000} onClick={loadMore}>Load more neighbors</button>
              <button className="secondary small" disabled={!root || loading} onClick={() => void loadGraph(root, root.kind === 'refinery_unit' ? 300 : 600)}>Center root</button>
            </div>
          </div>
        )}
        <GraphInsightHelper dataset={dataset} graph={graph} root={root} selected={selected} source={graphSource} applied={applied} queryText={graphQuery} />
      </section>
      {inspectorOpen && <EntityInspector node={selected} edge={selectedEdge} dataset={dataset} paths={graph.paths ?? []} onClose={() => setInspectorOpen(false)} />}
      <section className="graph-viewer panel">
        <div className="graph-content">
          {loading ? <div className="loading-state"><GraphIcon /><b>Querying graph…</b><span>Memuat neighborhood bertahap agar dataset besar tetap responsif.</span></div> : graph.nodes.length ? <GraphView graph={graph} rootId={root?.id ?? graph.nodes[0].id} selectedId={selected?.id} selectedEdgeId={selectedEdge?.id} onSelect={selectNode} onSelectEdge={selectEdge} /> : <div className="graph-empty"><GraphIcon /><p>Pilih node di sebelah kiri atau jalankan property query.</p></div>}
        </div>
        {applied.mode === 'directed' && graph.paths?.length ? <DirectedPathPanel paths={graph.paths} /> : null}
      </section>
    </div>
  )
}

function GraphInsightHelper({
  dataset,
  graph,
  root,
  selected,
  source,
  applied,
  queryText,
}: {
  dataset: DatasetSummary
  graph: GraphSlice
  root?: GraphNode
  selected?: GraphNode
  source: GraphSource
  applied: { mode: 'neighborhood' | 'directed'; depth: number; includeCandidates: boolean; minConfidence: number; relation: string; nodeType: string; refineryUnit: string; equipmentCode: string }
  queryText: string
}) {
  const [copied, setCopied] = useState('')
  const [insightOpen, setInsightOpen] = useState(false)
  const [readinessContext, setReadinessContext] = useState<ReadinessContext>()
  const [analysisEvidence, setAnalysisEvidence] = useState<AnalysisEvidence>({})
  const [analysisLoading, setAnalysisLoading] = useState(false)
  const [generating, setGenerating] = useState<DiagnosticRole | ''>('')
  const [generatedRole, setGeneratedRole] = useState<DiagnosticRole | ''>('')
  const [generatedText, setGeneratedText] = useState('')
  const [generateError, setGenerateError] = useState('')
  const [showDashboard, setShowDashboard] = useState(false)
  const abortRef = useRef<AbortController | null>(null)
  useEffect(() => {
    let cancelled = false
    setReadinessContext(undefined)
    const focusNode = selected ?? root
    if (!focusNode) return
    api.readinessContext(dataset.id, focusNode.id)
      .then((context) => { if (!cancelled) setReadinessContext(context) })
      .catch(() => { if (!cancelled) setReadinessContext(undefined) })
    return () => { cancelled = true }
  }, [dataset.id, root?.id, selected?.id])
  useEffect(() => {
    let cancelled = false
    setAnalysisLoading(true)
    Promise.all(diagnosisEvidenceTables.map((name) =>
      api.analysis(dataset.id, name)
        .then((rows) => [name, rows] as const)
        .catch(() => [name, []] as const),
    )).then((entries) => {
      if (!cancelled) setAnalysisEvidence(Object.fromEntries(entries))
    }).finally(() => {
      if (!cancelled) setAnalysisLoading(false)
    })
    return () => { cancelled = true }
  }, [dataset.id])
  const focusNode = selected ?? root
  const insight = useMemo(() => buildGraphInsight(graph, focusNode, source, applied, queryText, readinessContext, root), [graph, focusNode, root, source, applied, queryText, readinessContext])
  const diagnosisEvidence = useMemo(() => buildCmrpDiagnosisPack(analysisEvidence, focusNode, applied, queryText), [analysisEvidence, focusNode, applied, queryText])
  const relatedNodeEvidence = useMemo(() => buildRelatedNodeEvidence(graph, focusNode), [graph, focusNode])
  const copyPrompt = async (role: DiagnosticRole) => {
    const text = buildRoleDiagnosticPrompt(role, insight, diagnosisEvidence, relatedNodeEvidence)
    const didCopy = await copyText(text)
    setCopied(didCopy ? role : 'failed')
    window.setTimeout(() => setCopied(''), 1600)
  }
  const generateAnalysis = async (role: DiagnosticRole) => {
    if (generating) {
      abortRef.current?.abort()
      abortRef.current = null
      setGenerating('')
      return
    }
    const prompt = buildRoleDiagnosticPrompt(role, insight, diagnosisEvidence, relatedNodeEvidence)
    const ctrl = new AbortController()
    abortRef.current = ctrl
    setGenerating(role)
    setGeneratedRole(role)
    setGeneratedText('')
    setGenerateError('')
    setShowDashboard(true)
    try {
      await streamDiagnosis(prompt, role, (chunk) => {
        setGeneratedText((prev) => prev + chunk)
      }, ctrl.signal)
    } catch (err: unknown) {
      if ((err as { name?: string }).name !== 'AbortError') {
        setGenerateError((err as Error).message || 'Gagal menghubungi server.')
      }
    } finally {
      setGenerating('')
      abortRef.current = null
    }
  }
  return (
    <section className={`graph-insight ${insightOpen ? 'open' : 'collapsed'}`}>
      <div className="graph-insight-head">
        <div className="graph-insight-lead">
          <button className="graph-insight-toggle" onClick={() => setInsightOpen((value) => !value)} aria-expanded={insightOpen}>
            <span className="eyebrow">Reliability diagnosis prompt</span>
            <ChevronIcon />
          </button>
          <strong>Condition, readiness, risk, and action briefing for the selected node first.</strong>
          {insightOpen && (
            <div className="graph-insight-badges">
              <span>CMRP frame</span>
              <span>Role-specific lens</span>
              <span>Condition map</span>
              <span>Readiness blockers</span>
              <span>{relatedNodeEvidence.totalRelated} related nodes</span>
              <span>{insight.readinessAssociationLabel}</span>
              <span>Confidence: {insight.insightConfidence}</span>
              <span>{analysisLoading ? 'Loading diagnosis CSV' : `${diagnosisEvidence.primaryDiagnosis.length + diagnosisEvidence.confidenceAndCaveats.length} diagnosis facts`}</span>
              {insight.readinessAssociationMode === 'tag' ? <span>{format(insight.tagReadiness)} tag-secondary readiness facts</span> : null}
            </div>
          )}
        </div>
        <div className="graph-insight-actions">
          <span className="eyebrow">Copy prompt:</span>
          <button className="primary small" title="Copy engineer prompt" onClick={() => void copyPrompt('engineer')}>{copied === 'engineer' ? 'Copied ✓' : 'Engineer'}</button>
          <button className="primary small" title="Copy reliability manager prompt" onClick={() => void copyPrompt('reliability_manager')}>{copied === 'reliability_manager' ? 'Copied ✓' : 'Reliability Mgr'}</button>
          <button className="primary small" title="Copy maintenance manager prompt" onClick={() => void copyPrompt('maintenance_manager')}>{copied === 'maintenance_manager' ? 'Copied ✓' : 'Maintenance Mgr'}</button>
          <button className="primary small" title="Copy VP Reliability prompt" onClick={() => void copyPrompt('vp')}>{copied === 'vp' ? 'Copied ✓' : 'VP Reliability'}</button>
          {copied === 'failed' ? <small className="copy-error">Clipboard blocked</small> : null}
          {generatedText && !generating && (
            <button className="primary small" onClick={() => setShowDashboard(true)}>Lihat Hasil</button>
          )}
        </div>
      </div>
      {insightOpen && (
        <>
          <div className="graph-insight-grid">
            <article><span>Engineer view</span><p>{insight.engineer}</p></article>
            <article><span>Manager view</span><p>{insight.manager}</p></article>
            <article><span>VP view</span><p>{insight.vp}</p></article>
          </div>
          <div className="graph-insight-facts">
            {insight.facts.map((fact) => <span key={fact}>{fact}</span>)}
          </div>
        </>
      )}
      {showDashboard && (
        <DiagnosisDashboard
          role={generatedRole as DiagnosticRole}
          text={generatedText}
          generating={!!generating}
          error={generateError}
          onClose={() => setShowDashboard(false)}
        />
      )}
    </section>
  )
}

async function copyText(text: string) {
  try {
    await navigator.clipboard.writeText(text)
    return true
  } catch {
    const textarea = document.createElement('textarea')
    textarea.value = text
    textarea.setAttribute('readonly', 'true')
    textarea.style.position = 'fixed'
    textarea.style.left = '-9999px'
    document.body.appendChild(textarea)
    textarea.select()
    const ok = document.execCommand('copy')
    textarea.remove()
    return ok
  }
}

function DiagnosisDashboard({ role, text, generating, error, onClose }: {
  role: DiagnosticRole
  text: string
  generating: boolean
  error: string
  onClose: () => void
}) {
  const roleLabels: Record<DiagnosticRole, string> = {
    engineer: 'Engineer',
    reliability_manager: 'Reliability Manager',
    maintenance_manager: 'Maintenance Manager',
    vp: 'VP Reliability',
  }
  const sections = parseDiagnosisSections(text)
  return (
    <div className="diagnosis-modal-overlay" onClick={(e) => { if (e.target === e.currentTarget) onClose() }}>
      <div className="diagnosis-modal">
        <div className="diagnosis-modal-header">
          <div>
            <span className="eyebrow">Diagnosis Analysis</span>
            <h2>Laporan {roleLabels[role] ?? role}</h2>
          </div>
          <button className="diagnosis-modal-close" onClick={onClose} aria-label="Tutup">✕</button>
        </div>
        {generating && (
          <div className="diagnosis-generating">
            <span className="spinner" />
            <span>Sedang menganalisis…</span>
          </div>
        )}
        {error && <div className="diagnosis-error">{error}</div>}
        {!text && !generating && !error && (
          <div className="diagnosis-empty">Belum ada hasil.</div>
        )}
        {text && (
          <div className="diagnosis-body">
            {sections.length > 0 ? sections.map((sec, i) => (
              <div key={i} className={`diagnosis-section diagnosis-section-${sec.type}`}>
                {sec.heading && <h3 className="diagnosis-section-heading">{sec.heading}</h3>}
                <DiagnosisContent content={sec.content} />
              </div>
            )) : <DiagnosisContent content={text} />}
          </div>
        )}
      </div>
    </div>
  )
}

type DiagnosisSection = { type: 'fact-finding' | 'analysis' | 'summary' | 'other'; heading: string; content: string }

function parseDiagnosisSections(text: string): DiagnosisSection[] {
  const headingRe = /^#{1,3}\s+(.+)$/m
  const lines = text.split('\n')
  const sections: DiagnosisSection[] = []
  let current: DiagnosisSection | null = null
  for (const line of lines) {
    const m = line.match(/^#{1,3}\s+(.+)$/)
    if (m) {
      if (current) sections.push(current)
      const h = m[1].toLowerCase()
      const type: DiagnosisSection['type'] = h.includes('fact') || h.includes('finding') || h.includes('temuan') ? 'fact-finding'
        : h.includes('analisis') || h.includes('analysis') ? 'analysis'
        : h.includes('summary') || h.includes('ringkasan') || h.includes('diagnosis') ? 'summary'
        : 'other'
      current = { type, heading: m[1], content: '' }
    } else if (current) {
      current.content += line + '\n'
    } else if (line.trim()) {
      current = { type: 'other', heading: '', content: line + '\n' }
    }
  }
  if (current) sections.push(current)
  // drop empty headingRe usage to avoid unused var warning
  void headingRe
  return sections
}

function DiagnosisContent({ content }: { content: string }) {
  const lines = content.split('\n')
  const elements: React.ReactNode[] = []
  let tableLines: string[] = []

  const flushTable = () => {
    if (tableLines.length < 2) {
      tableLines.forEach((l) => elements.push(<p key={elements.length}>{l}</p>))
      tableLines = []
      return
    }
    const [header, , ...rows] = tableLines
    const cols = header.split('|').map(s => s.trim()).filter(Boolean)
    elements.push(
      <div key={elements.length} className="diagnosis-table-wrap">
        <table className="diagnosis-table">
          <thead><tr>{cols.map((c, i) => <th key={i}>{c}</th>)}</tr></thead>
          <tbody>{rows.map((row, ri) => {
            const cells = row.split('|').map(s => s.trim()).filter(Boolean)
            return <tr key={ri}>{cells.map((c, ci) => <td key={ci}>{c}</td>)}</tr>
          })}</tbody>
        </table>
      </div>
    )
    tableLines = []
  }

  for (const line of lines) {
    if (line.startsWith('|')) {
      tableLines.push(line)
      continue
    }
    if (tableLines.length) flushTable()
    if (!line.trim()) { elements.push(<br key={elements.length} />); continue }
    if (/^\d+\.\s/.test(line)) { elements.push(<p key={elements.length} className="diagnosis-list-item">{line}</p>); continue }
    if (/^[-*]\s/.test(line)) { elements.push(<p key={elements.length} className="diagnosis-bullet">{line.slice(2)}</p>); continue }
    if (/^\*\*(.+)\*\*$/.test(line)) { elements.push(<p key={elements.length}><strong>{line.replace(/\*\*/g, '')}</strong></p>); continue }
    elements.push(<p key={elements.length}>{line.replace(/\*\*([^*]+)\*\*/g, (_, t) => t)}</p>)
  }
  if (tableLines.length) flushTable()
  return <>{elements}</>
}

type GraphInsight = ReturnType<typeof buildGraphInsight>

function buildGraphInsight(
  graph: GraphSlice,
  root: GraphNode | undefined,
  source: GraphSource,
  applied: { mode: 'neighborhood' | 'directed'; depth: number; includeCandidates: boolean; minConfidence: number; relation: string; nodeType: string; refineryUnit: string; equipmentCode: string },
  queryText: string,
  readinessContext?: ReadinessContext,
  graphStart?: GraphNode,
) {
  const nodeKinds = countBy(graph.nodes.map((node) => node.kind))
  const edgeTypes = countBy(graph.edges.map((edge) => edge.type))
  const focusEdges = root ? graph.edges.filter((edge) => edge.source === root.id || edge.target === root.id) : []
  const focusEdgeTypes = countBy(focusEdges.map((edge) => edge.type))
  const dominantKinds = topEntries(nodeKinds, 4).map(([key, value]) => `${human(key)} ${format(value)}`)
  const dominantEdges = topEntries(edgeTypes, 4).map(([key, value]) => `${human(key)} ${format(value)}`)
  const visibleDirectReadiness = focusEdges.filter((edge) => edge.type === 'EQUIPMENT_HAS_READINESS_RECORD').length
  const visibleRuReadiness = focusEdges.filter((edge) => edge.type === 'REFINERY_UNIT_HAS_READINESS_RECORD').length
  const visibleTagReadiness = countVisibleTagAssociatedReadiness(graph)
  const directReadiness = readinessContext?.direct_count ?? visibleDirectReadiness
  const ruReadiness = readinessContext?.ru_level_count ?? visibleRuReadiness
  const tagReadiness = readinessContext?.tag_match_count ?? visibleTagReadiness
  const readinessAssociationMode = directReadiness ? 'direct' : tagReadiness ? 'tag' : ruReadiness ? 'ru' : 'none'
  const readinessAssociationLabel = readinessAssociationMode === 'direct'
    ? 'Directly linked'
    : readinessAssociationMode === 'tag'
      ? 'Operationally associated'
      : readinessAssociationMode === 'ru'
        ? 'RU context only'
        : 'No readiness context'
  const insightConfidence = readinessAssociationMode === 'direct' ? 'High' : readinessAssociationMode === 'tag' ? 'Medium' : readinessAssociationMode === 'ru' ? 'Low' : 'None'
  const deepPaths = graph.paths?.filter((path) => path.depth >= 3) ?? []
  const hasRecommendationChain = (graph.paths ?? []).some((path) => path.relationship_path.includes('ISSUE_HAS_RCPS') && path.relationship_path.includes('RCPS_HAS_RECOMMENDATION'))
  const isLarge = graph.nodes.length >= 100 || graph.edges.length >= 150
  const sourceLabel = source === 'property' ? 'Property query' : source === 'directed' ? 'Directed descendants' : source === 'neighborhood' ? 'Neighborhood' : 'No graph'
  const headline = !graph.nodes.length
    ? 'Belum ada graph context untuk disimpulkan.'
    : source === 'property'
      ? 'Query ini menemukan populasi record yang perlu diprioritaskan.'
      : source === 'directed'
        ? 'Graph ini menunjukkan chain downstream dan dampak turunan.'
        : 'Graph ini menunjukkan konteks sekitar node terpilih.'
  const facts = [
    `${format(graph.nodes.length)} nodes`,
    `${format(graph.edges.length)} edges`,
    `${sourceLabel}`,
    graph.max_depth_found ? `Max depth ${graph.max_depth_found}` : '',
    graph.truncated ? 'Result truncated' : '',
    graph.degree?.high_degree ? `High-degree ${format(graph.degree.total_edges)} edges` : '',
    dominantEdges.length ? `Dominant edges: ${dominantEdges.join(', ')}` : '',
    dominantKinds.length ? `Node mix: ${dominantKinds.join(', ')}` : '',
    tagReadiness ? `Catatan kesiapan pada tag yang sama: ${format(tagReadiness)}` : '',
    directReadiness ? `Catatan kesiapan menempel langsung: ${format(directReadiness)}` : '',
    ruReadiness ? `Catatan kesiapan tingkat unit: ${format(ruReadiness)}` : '',
    `Keterhubungan: ${readinessAssociationLabel}`,
    `Insight confidence: ${insightConfidence}`,
  ].filter(Boolean)
  const readinessText = directReadiness
    ? `Readiness punya ${format(directReadiness)} direct equipment-readiness edge; evidence confidence tinggi.`
    : tagReadiness
      ? `Readiness punya ${format(tagReadiness)} tag-associated secondary facts untuk equipment ini; gunakan sebagai tambahan fakta readiness/risk/action, dengan catatan direct edge verified belum tersedia.`
      : ruReadiness
        ? `Readiness baru tersedia sebagai konteks RU; jangan overclaim sebagai equipment-specific tanpa tag match.`
        : `Readiness belum terlihat pada graph aktif.`
  const engineer = !graph.nodes.length
    ? 'Pilih RU/equipment/issue untuk membuat diagnosis reliability berbasis evidence.'
    : `Gunakan context ini untuk diagnosis failure exposure, work-management mechanism, dan corrective path. ${readinessText} ${tagReadiness ? 'Validasi mapping tag/equipment sebagai bagian dari data-lineage check, bukan alasan menunda triage.' : ''} ${deepPaths.length ? `${format(deepPaths.length)} deep path tersedia untuk RCA/RCPS evidence chain.` : 'Jika deep path belum muncul, mulai dari issue, maintenance history, dan RKAP scope.'}`
  const manager = !graph.nodes.length
    ? 'Belum ada decision queue; mulai dari RU/equipment/program yang perlu owner dan action cadence.'
    : `${isLarge ? 'Exposure cukup luas; tetapkan prioritas, owner, dan cadence eksekusi. ' : ''}${tagReadiness ? `${format(tagReadiness)} tag-associated readiness facts harus dipakai sebagai secondary facts untuk triage equipment; jangan diperlakukan sebagai sekadar data gap. ` : ''}${hasRecommendationChain ? 'Chain Issue → RCPS → Recommendation mendukung follow-up closure discipline. ' : 'Jika issue belum sampai recommendation, manager perlu memaksa action plan dan owner. '}Fokus pada bottleneck reliability, RKAP alignment, dan work-management discipline.`
  const vp = !graph.nodes.length
    ? 'Belum ada exposure strategis; pilih RU/program/equipment untuk executive diagnosis.'
    : `${isLarge || graph.truncated ? 'Exposure cukup besar untuk governance review dan assurance question. ' : ''}${tagReadiness ? `${format(tagReadiness)} tag-associated readiness facts adalah secondary operational evidence; governance issue-nya adalah promosi ke direct edge, bukan mengabaikan readiness exposure. ` : ''}${deepPaths.length ? 'Deep chain memberi evidence lintas domain untuk keputusan eskalasi atau program assurance. ' : 'Jika chain lintas domain belum kuat, minta manager memperjelas linkage, owner, dan investment question.'}`
  return { root, graphStart, source, applied, queryText, headline, engineer, manager, vp, facts, nodeKinds, edgeTypes, focusEdgeTypes, deepPaths, directReadiness, ruReadiness, tagReadiness, readinessAssociationMode, readinessAssociationLabel, insightConfidence, readinessContext, hasRecommendationChain }
}

function normalizeRuText(value: unknown) {
  return String(value ?? '').toUpperCase().replace(/[^A-Z0-9]+/g, ' ').replace(/\s+/g, ' ').trim()
}

function inferFocusRu(root: GraphNode | undefined, applied: { refineryUnit: string }, queryText: string) {
  const candidates = [
    root?.refinery_unit,
    root?.properties?.refinery_unit,
    root?.properties?.ru,
    applied.refineryUnit,
    queryText.match(/RU\s*(?:II|III|IV|V|VI|VII|[2-7])/i)?.[0],
  ]
  return String(candidates.find((value) => String(value ?? '').trim()) ?? '').trim()
}

function rowMatchesFocusRu(row: Record<string, unknown>, focusRu: string) {
  if (!focusRu) return true
  const rowRu = normalizeRuText(row.refinery_unit)
  const focus = normalizeRuText(focusRu)
  return rowRu === focus || rowRu.includes(focus) || focus.includes(rowRu)
}

function evidenceValue(row: Record<string, unknown>, key: string) {
  const value = row[key]
  if (value === undefined || value === null || value === '') return ''
  return `${key}=${String(value)}`
}

function compactEvidenceRow(row: Record<string, unknown>, keys: string[]) {
  return keys.map((key) => evidenceValue(row, key)).filter(Boolean).join('; ')
}

function takeRelevantRows(rows: Record<string, unknown>[], focusRu: string, limit: number) {
  const relevant = rows.filter((row) => rowMatchesFocusRu(row, focusRu))
  return (relevant.length ? relevant : rows).slice(0, limit)
}

function buildCmrpDiagnosisPack(
  evidence: AnalysisEvidence,
  root: GraphNode | undefined,
  applied: { refineryUnit: string },
  queryText: string,
): DiagnosisEvidencePack {
  const focusRu = inferFocusRu(root, applied, queryText)
  const missing: string[] = []
  const reasoningRows = evidence.analysis_ready_reasoning_evidence_index ?? []
  const primaryDiagnosis: string[] = []
  const businessRisk: string[] = []
  const processReliabilitySignal: string[] = []
  const equipmentReliabilitySignal: string[] = []
  const workManagementSignal: string[] = []
  const leadershipDecision: string[] = []
  const confidenceAndCaveats: string[] = []

  const addRows = (target: string[], table: string, label: string, keys: string[], limit = 4) => {
    const sourceRows = evidence[table] ?? []
    if (!sourceRows.length) {
      missing.push(table)
      return
    }
    takeRelevantRows(sourceRows, focusRu, limit).forEach((row) => {
      const text = compactEvidenceRow(row, keys)
      if (text) target.push(`${label}: ${text}`)
    })
  }

  addRows(primaryDiagnosis, 'analysis_ready_ru_cross_domain_assessment', 'Cross-domain diagnosis', [
    'refinery_unit', 'site_name', 'primary_assessment_theme', 'diagnostic_prompt',
    'maintenance_orders', 'reliability_observations', 'equipment_issues', 'rkap_programs',
    'avg_domain_link_percentage',
  ])
  addRows(businessRisk, 'analysis_ready_rkap_cost_alignment_signal', 'Business and RKAP risk', [
    'refinery_unit', 'rkap_records', 'rkap_linked_to_equipment', 'rkap_equipment_link_percentage',
    'verified_rkap_programs', 'verified_equipment_issues', 'diagnostic_signal',
  ])
  addRows(equipmentReliabilitySignal, 'analysis_ready_reliability_performance_signal', 'Equipment reliability signal', [
    'refinery_unit', 'reliability_records', 'reliability_linked_to_equipment',
    'reliability_equipment_link_percentage', 'maintenance_orders', 'diagnostic_signal',
  ])
  addRows(equipmentReliabilitySignal, 'analysis_ready_defect_elimination_signal', 'Defect elimination signal', [
    'refinery_unit', 'issue_records', 'linked_to_equipment', 'equipment_link_percentage',
    'verified_equipment_issues', 'rcps_count', 'recommendation_count', 'diagnostic_signal',
  ])
  addRows(processReliabilitySignal, 'analysis_ready_readiness_operation_signal', 'Manufacturing process/readiness signal', [
    'refinery_unit', 'readiness_records', 'linked_to_equipment',
    'equipment_link_percentage', 'diagnostic_signal',
  ])
  addRows(workManagementSignal, 'analysis_ready_work_management_health', 'Work management signal', [
    'refinery_unit', 'maintenance_records', 'notification_records', 'closed_like_records',
    'open_like_records', 'open_high_criticality_records', 'avg_open_age_days',
    'equipment_link_percentage', 'caveat',
  ])
  addRows(leadershipDecision, 'analysis_ready_program_effectiveness_signal', 'Program effectiveness decision', [
    'refinery_unit', 'rkap_records', 'rkap_equipment_link_percentage',
    'issue_records', 'issue_equipment_link_percentage', 'diagnostic_signal',
  ])
  addRows(confidenceAndCaveats, 'analysis_ready_data_confidence_by_ru_domain', 'Evidence confidence', [
    'refinery_unit', 'domain', 'total_records', 'linked_to_equipment', 'equipment_link_percentage',
    'candidate_relationships', 'evidence_confidence', 'caveat',
  ], 10)

  const reasoning = reasoningRows.slice(0, 12).map((row) =>
    compactEvidenceRow(row, ['evidence_table', 'use_case', 'reasoning_instruction']),
  ).filter(Boolean)

  return {
    focusRu: focusRu || 'all / current graph context',
    primaryDiagnosis: primaryDiagnosis.slice(0, 6),
    businessRisk: businessRisk.slice(0, 6),
    processReliabilitySignal: processReliabilitySignal.slice(0, 6),
    equipmentReliabilitySignal: equipmentReliabilitySignal.slice(0, 8),
    workManagementSignal: workManagementSignal.slice(0, 6),
    leadershipDecision: leadershipDecision.slice(0, 6),
    confidenceAndCaveats: confidenceAndCaveats.slice(0, 12),
    reasoning,
    missing,
  }
}

function bulletLines(values: string[]) {
  return values.map((value) => `- ${value}`).join('\n') || '- none'
}

function compactPropertyValue(value: unknown) {
  if (value === undefined || value === null || value === '') return ''
  const text = typeof value === 'object' ? JSON.stringify(value) : String(value)
  return text.length > 90 ? `${text.slice(0, 87)}...` : text
}

function nodeEvidenceSortKey(node: GraphNode) {
  const order: Record<string, number> = {
    equipment_issue: 1,
    readiness_record: 2,
    maintenance_order: 3,
    maintenance_notification: 4,
    reliability_observation: 5,
    inspection: 6,
    rkap_program: 7,
    rcps: 8,
    rcps_recommendation: 9,
  }
  return order[node.kind] ?? 99
}

function nodeEvidenceLine(node: GraphNode, relationships: GraphEdge[]) {
  const keys = propertyPriority(node.kind)
  const propertyPairs = keys
    .map((key) => {
      const value = compactPropertyValue(node.properties?.[key])
      return value ? `${key}=${value}` : ''
    })
    .filter(Boolean)
    .slice(0, 10)
  const relText = relationships
    .map((edge) => `${edge.type}${edge.is_candidate ? '(candidate)' : ''}${edge.match_method ? `/${edge.match_method}` : ''}`)
    .slice(0, 3)
    .join(', ')
  const source = [node.source.workbook, node.source.sheet, node.source.row ? `row ${node.source.row}` : ''].filter(Boolean).join(' / ')
  return `${node.kind}: ${node.label}${relText ? ` | rel=${relText}` : ''}${propertyPairs.length ? ` | ${propertyPairs.join('; ')}` : ''}${source ? ` | source=${source}` : ''}`
}

function buildRelatedNodeEvidence(graph: GraphSlice, root?: GraphNode): RelatedNodeEvidence {
  if (!root) return { lines: [], truncated: false, totalRelated: 0 }
  const relationByNode = new Map<string, GraphEdge[]>()
  graph.edges.forEach((edge) => {
    const otherId = edge.source === root.id ? edge.target : edge.target === root.id ? edge.source : ''
    if (!otherId || otherId === root.id) return
    const list = relationByNode.get(otherId) ?? []
    list.push(edge)
    relationByNode.set(otherId, list)
  })
  const related = graph.nodes
    .filter((node) => node.id !== root.id && relationByNode.has(node.id))
    .sort((a, b) => nodeEvidenceSortKey(a) - nodeEvidenceSortKey(b) || a.label.localeCompare(b.label))
  const limit = 24
  return {
    lines: related.slice(0, limit).map((node) => nodeEvidenceLine(node, relationByNode.get(node.id) ?? [])),
    truncated: related.length > limit,
    totalRelated: related.length,
  }
}

type DiagnosticRole = 'engineer' | 'reliability_manager' | 'maintenance_manager' | 'vp'

function buildRoleDiagnosticPrompt(role: DiagnosticRole, insight: GraphInsight, diagnosisEvidence: DiagnosisEvidencePack, relatedNodeEvidence: RelatedNodeEvidence) {
  if (role === 'engineer') return buildEngineerDiagnosticPrompt(insight, diagnosisEvidence, relatedNodeEvidence)
  if (role === 'reliability_manager') return buildReliabilityManagerDiagnosticPrompt(insight, diagnosisEvidence, relatedNodeEvidence)
  if (role === 'maintenance_manager') return buildMaintenanceManagerDiagnosticPrompt(insight, diagnosisEvidence, relatedNodeEvidence)
  return buildVpDiagnosticPrompt(insight, diagnosisEvidence, relatedNodeEvidence)
}

function promptEvidenceSource(source?: { workbook?: string; sheet?: string; row?: number | null; record_id?: string | null }) {
  if (!source) return ''
  return [source.workbook, source.sheet, source.row ? `row ${source.row}` : '', source.record_id ? `record ${source.record_id}` : ''].filter(Boolean).join(' / ')
}

function promptEvidenceLine(item: NonNullable<ReadinessContext['domain_evidence']>[string][number]) {
  const propertyPairs = Object.entries(item.properties ?? {})
    .map(([key, value]) => {
      const compactValue = compactPropertyValue(value)
      return compactValue ? `${key}=${compactValue}` : ''
    })
    .filter(Boolean)
    .slice(0, 12)
  const relationship = [
    item.association_type,
    item.relationship_type,
    item.is_candidate ? 'candidate' : '',
    item.match_method ? `match=${item.match_method}` : '',
    item.confidence != null ? `confidence=${item.confidence}` : '',
    item.matched_token ? `matched_token=${item.matched_token}` : '',
  ].filter(Boolean).join('; ')
  const source = promptEvidenceSource(item.source)
  return `- ${item.node_type}: ${item.label}${relationship ? ` | ${relationship}` : ''}${propertyPairs.length ? ` | ${propertyPairs.join('; ')}` : ''}${source ? ` | source=${source}` : ''}`
}

function promptDomainEvidenceLines(context?: ReadinessContext) {
  const grouped = context?.domain_evidence ?? {}
  const domainOrder = ['asset', 'reliability', 'maintenance', 'readiness', 'issue_rcps', 'cost_program_rkap', 'inspection_operational', 'other']
  const lines: string[] = []
  domainOrder
    .filter((domain) => grouped[domain]?.length)
    .forEach((domain) => {
      lines.push(`${human(domain)}:`)
      lines.push(...grouped[domain].map(promptEvidenceLine))
    })
  Object.keys(grouped)
    .filter((domain) => !domainOrder.includes(domain) && grouped[domain]?.length)
    .sort()
    .forEach((domain) => {
      lines.push(`${human(domain)}:`)
      lines.push(...grouped[domain].map(promptEvidenceLine))
    })
  return lines
}

function reliabilityEngineeringLines(insight: GraphInsight): string[] {
  const re = insight.readinessContext?.reliability_engineering
  if (!re) return ['- Sinyal keandalan turunan belum tersedia untuk equipment ini.']
  const present = (value: unknown) => value !== null && value !== undefined && value !== ''
  const lines: string[] = []
  // (a) keandalan / fungsi
  if (re.observations) lines.push(`- Catatan keandalan: ${format(re.observations)} observasi${present(re.function_status) ? `; status fungsi/redundansi dominan: ${re.function_status}` : ''}.`)
  if (present(re.avg_mtbf)) lines.push(`- MTBF rata-rata tercatat: ${re.avg_mtbf} (perlakukan 0/kosong sebagai "belum tercatat", bukan keandalan sempurna).`)
  if (present(re.avg_mttr)) lines.push(`- MTTR rata-rata tercatat: ${re.avg_mttr}.`)
  if (present(re.max_running_hours)) lines.push(`- Running hours tertinggi: ${re.max_running_hours}.`)
  if (re.abnormal_status_count) lines.push(`- Observasi berstatus tidak normal (bukan running/standby): ${format(re.abnormal_status_count)}.`)
  if (re.issue_count) lines.push(`- Riwayat issue equipment (sinyal bad actor / FRACAS): ${format(re.issue_count)}.`)
  if (present(re.criticality)) lines.push(`- Kritikalitas equipment: ${re.criticality}.`)
  // (b) work-management
  if (re.total_orders) lines.push(`- Work order: total ${format(re.total_orders)} (open ${format(re.open_orders ?? 0)}, closed ${format(re.closed_orders ?? 0)}).`)
  if (present(re.backlog_age_median)) lines.push(`- Umur backlog order open: median ${re.backlog_age_median} hari${present(re.backlog_age_p90) ? `, p90 ${re.backlog_age_p90} hari` : ''}.`)
  if (re.material_blocked_count) lines.push(`- Order open tertahan material/spare (6-Tepat Material): ${format(re.material_blocked_count)}.`)
  if (re.priority_high_count) lines.push(`- Order prioritas tinggi: ${format(re.priority_high_count)}.`)
  if (present(re.planned_cost) || present(re.actual_cost)) lines.push(`- Biaya pemeliharaan: rencana ${present(re.planned_cost) ? re.planned_cost : '-'} vs aktual ${present(re.actual_cost) ? re.actual_cost : '-'}.`)
  // kesiapan / readiness (tag-match exact-boundary; bawa tingkat keyakinan + tag dasar)
  const tagDasar = re.readiness_tag_samples?.length ? ` (mis. ${re.readiness_tag_samples.slice(0, 3).join(', ')})` : ''
  if (re.readiness_direct) {
    lines.push(`- Readiness records menempel langsung pada equipment: ${format(re.readiness_direct)} — keyakinan kuat, jadikan dasar kesimpulan.`)
  } else if (re.readiness_tag_match) {
    lines.push(`- Readiness records pada tag/nomor yang sama${tagDasar}: ${format(re.readiness_tag_match)} — indikasi kuat, perlu dikonfirmasi ke rencana kerja equipment; jangan diperlakukan sebagai tidak ada.`)
  } else if (re.readiness_ru_level) {
    lines.push(`- Readiness records baru tersedia di tingkat unit (${format(re.readiness_ru_level)}) — konteks, belum spesifik equipment ini.`)
  } else {
    lines.push(`- Readiness records: belum terlihat pada data yang tersedia untuk equipment ini.`)
  }
  // (d) inspeksi (tag-match exact-boundary, indikatif)
  if (re.inspection_match_count) lines.push(`- Temuan inspeksi pada tag yang sama: ${format(re.inspection_match_count)} — indikasi, verifikasi ke equipment${re.inspection_findings?.length ? `; contoh hasil: ${re.inspection_findings.slice(0, 3).join('; ')}` : ''}.`)
  // (e) ICU issue
  if (re.icu_count) lines.push(`- ICU Issue (integrity/condition): ${format(re.icu_count)} total${re.icu_open_count ? `, ${format(re.icu_open_count)} masih open` : ', semua closed'} — jadikan dasar evaluasi condition monitoring dan integritas aset.`)
  // (f) critical equipment
  if (re.ce_count) lines.push(`- Equipment ini terdaftar sebagai Critical Equipment${re.ce_class ? ` (kelas: ${re.ce_class})` : ''} — gunakan sebagai bobot prioritas dalam setiap rekomendasi tindakan.`)
  // (g) zero clamp
  if (re.zc_count) lines.push(`- Zero Clamp terpasang: ${format(re.zc_count)} total${re.zc_active_count ? `, ${format(re.zc_active_count)} masih aktif/belum dilepas` : ', semua sudah dilepas'}${re.zc_dominant_damage ? `; jenis kerusakan dominan: ${re.zc_dominant_damage}` : ''} — sinyal adanya kerusakan mekanis aktif yang belum sepenuhnya diselesaikan.`)
  // (h) pipeline inspection
  if (re.pi_count) {
    const eolNote = re.pi_near_eol ? ` — PERINGATAN: ${format(re.pi_near_eol)} segmen remaining life < 5 tahun` : ''
    lines.push(`- Pipeline inspection: ${format(re.pi_count)} rekaman${present(re.pi_min_rem_life) ? `; remaining life minimum: ${re.pi_min_rem_life} tahun` : ''}${eolNote}.`)
  }
  // (i) power & steam
  if (re.ps_count) lines.push(`- Monitoring power & steam: ${format(re.ps_count)} rekaman terkait — pertimbangkan sebagai konteks utilitas dan ketersediaan energi pendukung.`)
  // (i2) metering
  if (re.meter_count) lines.push(`- Metering/sertifikasi alat ukur: ${format(re.meter_count)} alat ukur terkait${re.meter_not_normal ? `, ${format(re.meter_not_normal)} berstatus tidak normal` : ', semua Operasi Normal'}${re.meter_nearest_expired ? `; sertifikat terdekat kedaluwarsa: ${re.meter_nearest_expired}` : ''} — pertimbangkan kepatuhan kalibrasi dan legalitas operasi.`)
  // (j) readiness infrastruktur
  if (re.readiness_infra && Object.keys(re.readiness_infra).length) {
    const infraLabels: Record<string, string> = { readiness_jetty: 'Jetty', readiness_spm: 'SPM', readiness_tank: 'Tangki' }
    for (const [key, val] of Object.entries(re.readiness_infra as Record<string, { count: number; not_normal: number }>)) {
      const label = infraLabels[key] ?? key
      lines.push(`- Readiness ${label}: ${format(val.count)} rekaman${val.not_normal ? `, ${format(val.not_normal)} berstatus tidak normal` : ', semua dalam kondisi normal'}.`)
    }
  }
  // (c) business case RKAP (pisah pasti vs kemungkinan)
  if (re.rkap_program_count) {
    const exact = re.rkap_exact_count ?? 0
    const cand = re.rkap_candidate_count ?? 0
    const breakdown = exact || cand ? ` (${format(exact)} terhubung pasti, ${format(cand)} kemungkinan terhubung)` : ''
    lines.push(`- Program RKAP terkait: ${format(re.rkap_program_count)}${breakdown}${present(re.rkap_total_cost) ? `, exposure biaya ~${re.rkap_total_cost}` : ''}. Perlakukan "kemungkinan terhubung" sebagai indikatif.`)
  }
  if (re.rkap_top_risk_count) lines.push(`- Program RKAP top-risk: ${format(re.rkap_top_risk_count)}.`)
  if (re.rkap_delayed_count) lines.push(`- Program RKAP delay: ${format(re.rkap_delayed_count)}.`)
  if (re.rkap_high_value_count) lines.push(`- Program RKAP high-value: ${format(re.rkap_high_value_count)}.`)
  if (present(re.confidence_note)) lines.push(`- Tingkat dukungan data keandalan: ${re.confidence_note}.`)
  return lines.length ? lines : ['- Belum ada sinyal keandalan turunan yang tercatat untuk equipment ini.']
}

function outputFormatLines(): string[] {
  return [
    `Format output WAJIB — jawab dalam TIGA bagian berurutan dengan judul markdown persis berikut:`,
    ``,
    `## 1. Fact Finding`,
    `- Sajikan fakta terverifikasi sebagai SATU tabel markdown dengan kolom: | Aspek | Temuan | Nilai/Status | Confidence Level |.`,
    `- Kolom "Confidence Level" HANYA diisi salah satu dari tiga rating berikut, tanpa kata tinggi/sedang/rendah dan tanpa teks lain: ■■■ untuk confidence tinggi, ■■□ untuk confidence sedang, ■□□ untuk confidence rendah.`,
    `- PENGECUALIAN aturan angka: HANYA di dalam tabel Fact Finding ini, angka dan satuan boleh ditulis apa adanya.`,
    `- Setelah tabel, tulis 1 paragraf singkat (2-4 kalimat) merangkum gambaran fakta dalam bahasa operasional.`,
    ``,
    `## 2. Analysis`,
    `- Sajikan sebagai poin-poin (bullet list) — setiap poin membahas satu aspek analitis secara profesional dan langsung ke inti. WAJIB bahasa operasional — DILARANG angka mentah, nama kolom/tabel, "= 0", atau istilah struktur data.`,
    `- (Opsional) Tabel pendukung kualitatif bila memperjelas analisis.`,
    ``,
    `## 3. Summary`,
    `- Baris PALING ATAS Summary WAJIB berformat persis "Diagnosis/Confidence Level: [icon diagnosis] [rating confidence level]" — hanya label ini plus dua icon, tanpa teks lain. Icon diagnosis: 🔴 jika kondisi kritis/perlu tindakan segera, 🟡 jika perlu perhatian/waspada, 🟢 jika terkendali/baik — pilih sesuai kesimpulan diagnosis Anda. Rating confidence level: ■■■ tinggi, ■■□ sedang, ■□□ rendah (skema sama seperti kolom Confidence Level di Fact Finding).`,
    `- Setelah baris icon, tulis paragraf kesimpulan diagnosis, langsung diikuti satu kalimat Confidence Level (posisinya SETELAH diagnosis, SEBELUM rekomendasi).`,
    `- Tutup dengan rekomendasi/tindakan prioritas sesuai peran Anda sebagai daftar bernomor (1. 2. 3. dst, satu tindakan per nomor). Ringkas dan actionable, tanpa angka mentah.`,
  ]
}

function buildSharedDiagnosticContext(insight: GraphInsight, diagnosisEvidence: DiagnosisEvidencePack, relatedNodeEvidence: RelatedNodeEvidence) {
  const paths = insight.deepPaths.slice(0, 10).map((path) => `- ${path.depth} hop: ${path.label_path.join(' -> ')} [${path.relationship_path.join(' -> ')}]`)
  const edgeMix = topEntries(insight.edgeTypes, 10).map(([key, value]) => `- ${key}: ${value}`)
  const nodeMix = topEntries(insight.nodeKinds, 10).map(([key, value]) => `- ${key}: ${value}`)
  const issueCount = insight.focusEdgeTypes.EQUIPMENT_HAS_ISSUE ?? 0
  const orderCount = insight.focusEdgeTypes.EQUIPMENT_HAS_MAINTENANCE_ORDER ?? 0
  const notificationCount = insight.focusEdgeTypes.MAINTENANCE_ORDER_HAS_NOTIFICATION ?? 0
  const readinessSamples = (insight.readinessContext?.tag_match_samples ?? [])
    .map(promptEvidenceLine)
  const domainEvidence = promptDomainEvidenceLines(insight.readinessContext)
  const conclusionRule = insight.tagReadiness
    ? `Ada ${format(insight.tagReadiness)} catatan operasional pada tag/nomor yang sama dengan equipment ini; pakai sebagai indikasi kuat untuk menyimpulkan readiness blockers, fokus pemantauan, risiko, dan prioritas tindakan. Jangan menjawab "tidak bisa menyimpulkan" hanya karena catatannya belum menempel langsung; catatan kehati-hatian soal keyakinan hanya muncul setelah diagnosis.`
    : insight.ruReadiness
      ? `Yang ada baru gambaran readiness di tingkat unit tanpa kecocokan ke equipment ini; boleh sebut belum cukup untuk menyimpulkan readiness equipment secara spesifik.`
      : `Belum terlihat readiness records apa pun; sebutkan apa adanya secara natural pada data yang tersedia.`
  const secondaryFactRule = insight.tagReadiness
    ? `- Catatan operasional pada tag yang sama dengan ${insight.root?.label ?? 'equipment ini'} adalah indikasi pendukung: pakai untuk memperkuat kesimpulan soal readiness blockers, fokus pemantauan, risiko, dan prioritas tindakan (sampaikan secara natural, jangan sebut jenis bukti datanya).`
    : `- Belum ada catatan operasional pendukung pada tag yang sama untuk equipment ini.`
  const isFocusRu = insight.root?.kind === 'refinery_unit'
  const focusLabel = insight.root ? `${insight.root.label} (${insight.root.kind})` : 'none'
  const graphStartLabel = insight.graphStart && insight.graphStart.id !== insight.root?.id
    ? `${insight.graphStart.label} (${insight.graphStart.kind})`
    : ''
  return [
    `Fokus jawaban:`,
    `- Subjek utama jawaban: ${focusLabel}.`,
    graphStartLabel ? `- Titik awal eksplorasi: ${graphStartLabel}; jangan biarkan ini menggeser subjek yang dibahas.` : `- Titik awal eksplorasi: sama dengan subjek yang dibahas atau tidak tersedia.`,
    `- Setiap kesimpulan utama harus lebih dulu menjawab artinya bagi ${insight.root?.label ?? 'equipment yang dibahas'}.`,
    isFocusRu
      ? `- Karena subjek yang dibahas adalah sebuah RU, kesimpulan di tingkat unit boleh menjadi jawaban utama.`
      : `- Karena subjek yang dibahas bukan RU, jangan jadikan gambaran tingkat unit sebagai kesimpulan utama. Pakai gambaran tingkat unit hanya sebagai latar, pembanding, konteks eskalasi, atau catatan.`,
    `- Jika bukti pendukung hanya berlaku di tingkat unit, terjemahkan menjadi pertanyaan atau implikasi untuk equipment yang dibahas; jangan menggeser subjek menjadi "Untuk RU ...".`,
    `- Hindari frasa umum seperti "Untuk RU II..." kecuali subjeknya memang RU II. Utamakan "Untuk ${insight.root?.label ?? 'equipment terpilih'}..." dan sebutkan properti equipment terkait yang mendukungnya.`,
    secondaryFactRule,
    ``,
    `CMRP/SMRP diagnostic frame:`,
    `- Business & Management: production/business risk, RKAP/cost-to-risk alignment, budget priority.`,
    `- Manufacturing Process Reliability: process constraint, readiness exposure, production-loss risk.`,
    `- Equipment Reliability: critical assets, bad actors, reliability observations, defect/RCPS chain.`,
    `- Organization & Leadership: owner, escalation, governance gap, cross-functional decision.`,
    `- Work Management: backlog, WO/notification health, planning quality, closure/action discipline.`,
    ``,
    `Diagnostic stance required:`,
    `- Treat the prompt as an operational condition-mapping task, not a data explanation task.`,
    `- First map current equipment condition: what appears unhealthy, unresolved, constrained, or uncertain now.`,
    `- Then map readiness blockers: what may prevent the equipment from being ready/available, including unresolved readiness records, WO backlog, inspection/defect status, spares/access/permit/outage constraints when supported by evidence.`,
    `- Then state operational risk: production/process-safety/reliability exposure if the condition remains unresolved.`,
    `- Then state effort and priority: what to do first, who owns it, and what decision is needed.`,
    `- Avoid making the response mainly about graph paths, node counts, or data quality. Those are evidence, not the diagnosis.`,
    ``,
    `Diagnosis evidence from analysis_ready CSV summaries:`,
    `- Focus RU/context: ${diagnosisEvidence.focusRu}`,
    ``,
    `Primary diagnosis:`,
    bulletLines(diagnosisEvidence.primaryDiagnosis),
    ``,
    `Business & Management evidence:`,
    bulletLines(diagnosisEvidence.businessRisk),
    ``,
    `Manufacturing Process Reliability evidence:`,
    bulletLines(diagnosisEvidence.processReliabilitySignal),
    ``,
    `Equipment Reliability evidence:`,
    bulletLines(diagnosisEvidence.equipmentReliabilitySignal),
    ``,
    `Work Management evidence:`,
    bulletLines(diagnosisEvidence.workManagementSignal),
    ``,
    `Organization & Leadership evidence:`,
    bulletLines(diagnosisEvidence.leadershipDecision),
    ``,
    `Confidence and caveats:`,
    bulletLines(diagnosisEvidence.confidenceAndCaveats),
    ``,
    `How to use the summary evidence:`,
    bulletLines(diagnosisEvidence.reasoning),
    diagnosisEvidence.missing.length ? `- Missing/unavailable evidence tables: ${diagnosisEvidence.missing.join(', ')}` : '- All expected diagnosis evidence tables loaded or not required for this prompt.',
    ``,
    `Bahasa & gaya jawaban (WAJIB — ini menentukan kualitas jawaban):`,
    `- Tulislah seperti seorang reliability & maintenance engineer bersertifikat CMRP yang menulis catatan kondisi dan rekomendasi untuk rekan teknis dan atasannya. Bahasa Indonesia teknis yang mengalir dan natural, seperti manusia ahli menulis — bukan seperti analis data, bukan daftar metrik.`,
    `- Bagian "bahan penalaran internal" di bawah (angka, hitungan, istilah relasi) HANYA untuk Anda pakai bernalar dan menakar keyakinan. DILARANG mengutipnya mentah ke jawaban.`,
    `- Istilah teknis keandalan/maintenance DIPERSILAKAN tetap dalam bahasa Inggris bila lebih natural (mis. MTBF, MTTR, bad actor, root cause/RCA, P-F interval, condition monitoring, FMECA, FRACAS, RCM, preventive/predictive maintenance, backlog, schedule compliance, wrench time, business case, asset integrity, criticality, work order).`,
    `- Yang DILARANG hanyalah istilah STRUKTUR DATA/graph dan angka mentah di luar tabel Fact Finding: "direct edge", "direct readiness edge", "readiness edge", "tag-associated", "tag-associated secondary readiness", "secondary fact", "RU-level readiness", "node", "graph", "edge", "candidate relationship", "association", "count", nama kolom/tabel/CSV, serta pola angka mentah seperti "x = 0", "= 0", atau "readiness = 0".`,
    `- Jangan pernah menyatakan sesuatu sebagai "= 0" atau "tidak ada edge". Ungkapkan ketiadaan secara natural: "belum ada", "belum terlihat", "belum tercatat pada data yang tersedia".`,
    `- Mulailah dari diagnosis reliability/operasional, bukan dari struktur atau kualitas data. Jangan membuka dengan "data menunjukkan", hitungan, atau mekanika data.`,
    `- Terjemahkan tingkat keyakinan menjadi bahasa operasional, JANGAN menyebut jenis bukti datanya:`,
    insight.directReadiness
      ? `  • Ada readiness records yang jelas menempel pada ${insight.root?.label ?? 'equipment ini'} — sampaikan temuannya sebagai hal yang sudah pasti dan jadikan dasar utama kesimpulan.`
      : insight.tagReadiness
        ? `  • Ada catatan operasional pada tag/nomor equipment yang sama dengan ${insight.root?.label ?? 'equipment ini'} — perlakukan sebagai indikasi kuat yang masih perlu dikonfirmasi ke rencana kerja equipment; jangan menjawab "tidak bisa menyimpulkan" hanya karena belum ada catatan yang menempel langsung.`
        : insight.ruReadiness
          ? `  • Yang tersedia baru gambaran di tingkat unit, belum spesifik untuk ${insight.root?.label ?? 'equipment ini'} — sampaikan sebagai konteks unit dan sebut bahwa untuk equipment ini sendiri catatannya belum terlihat.`
          : `  • Belum terlihat readiness records untuk ${insight.root?.label ?? 'equipment ini'} pada data yang tersedia — sampaikan apa adanya secara natural, lalu arahkan ke langkah konfirmasi.`,
    ``,
    `Rules for conclusion quality:`,
    `- ${conclusionRule}`,
    `- Jangan menurunkan derajat bukti yang spesifik ke equipment di bawah gambaran tingkat unit. Jika keduanya bertentangan, jawab diagnosis untuk equipment-nya dulu, baru sebut konteks unit sebagai latar.`,
    `- Pakai tingkat keyakinan data hanya sebagai catatan keyakinan/kehati-hatian SETELAH kesimpulan operasional, dan ungkapkan dengan bahasa biasa ("masih perlu dipastikan", "indikasi awal", "sudah cukup kuat").`,
    `- Jika RKAP/program alignment rendah, sebut sebagai masalah keselarasan/keyakinan data, bukan otomatis berarti tidak ada program.`,
    `- Jika keyakinan pada suatu domain rendah, beri catatan kehati-hatian tetapi tetap rekomendasikan langkah validasi berikutnya.`,
    ``,
    `Bahan penalaran internal (JANGAN dikutip mentah di jawaban — pakai hanya untuk menakar keyakinan):`,
    `- Equipment yang dibahas: ${focusLabel}`,
    graphStartLabel ? `- Titik awal eksplorasi: ${graphStartLabel}` : `- Titik awal eksplorasi: sama dengan equipment yang dibahas atau tidak tersedia`,
    `- Sumber: ${insight.source}`,
    `- Query/filter: ${insight.queryText || JSON.stringify(insight.applied)}`,
    `- Fakta: ${insight.facts.join('; ') || 'tidak ada fakta graph'}`,
    `- Sinyal kondisi pada equipment ini (untuk kalibrasi, jangan dikutip): issue=${issueCount}, work order=${orderCount}, notifikasi=${notificationCount}, readiness records via tag sama=${insight.tagReadiness}, readiness records tingkat unit=${insight.ruReadiness}`,
    `- Keterhubungan readiness: ${insight.readinessAssociationLabel}`,
    `- Tingkat keyakinan: ${insight.insightConfidence}`,
    `- Kalibrasi keyakinan readiness: menempel langsung=${insight.directReadiness}, via tag sama=${insight.tagReadiness}, tingkat unit=${insight.ruReadiness}`,
    `- Cara membaca keyakinan ini: ${insight.directReadiness ? 'ada readiness records yang menempel langsung pada equipment — kuat' : insight.tagReadiness ? `ada ${format(insight.tagReadiness)} catatan pada tag yang sama — indikasi kuat, masih perlu dikonfirmasi` : 'belum ada kecocokan spesifik untuk equipment ini'}`,
    ``,
    `Node mix:`,
    nodeMix.join('\n') || '- none',
    ``,
    `Relationship mix:`,
    edgeMix.join('\n') || '- none',
    ``,
    `Sample deep paths:`,
    paths.join('\n') || '- none',
    ``,
    `Contoh catatan kesiapan pada tag yang sama (bahan internal, jangan dikutip mentah):`,
    readinessSamples.join('\n') || '- none',
    ``,
    `Selected-node associated evidence by domain:`,
    domainEvidence.join('\n') || '- none',
    `- Bukti yang menempel langsung pada equipment adalah fakta terkuat; bukti pada tag yang sama atau yang masih mungkin terhubung diperlakukan sebagai hipotesis operasional dengan catatan kehati-hatian; gambaran tingkat unit hanya dipakai sebagai latar, pembanding, konteks eskalasi, atau bahan pertanyaan validasi. Sampaikan semuanya dalam bahasa operasional natural.`,
    ``,
    `Related node properties for current equipment/root context:`,
    `- Total directly related visible nodes: ${relatedNodeEvidence.totalRelated}${relatedNodeEvidence.truncated ? ' (truncated in prompt)' : ''}`,
    bulletLines(relatedNodeEvidence.lines),
    `- Use these node properties to infer current condition, readiness blockers, backlog status, action maturity, and risk. Do not treat them as raw data only.`,
    ``,
    `Bahan penalaran keandalan & eksekusi (turunan, faktual — boleh dipakai mengisi tabel Fact Finding dan menakar keyakinan):`,
    ...reliabilityEngineeringLines(insight),
    `- Catatan: pencocokan equipment ke riwayatnya bersifat indikatif; bawa selalu catatan keyakinan, jangan klaim presisi absolut. Nilai 0/kosong berarti "belum tercatat", bukan "nol sebenarnya".`,
    ``,
    ...outputFormatLines(),
  ].join('\n')
}

function buildEngineerDiagnosticPrompt(insight: GraphInsight, diagnosisEvidence: DiagnosisEvidencePack, relatedNodeEvidence: RelatedNodeEvidence) {
  const focus = insight.root?.label ?? 'equipment terpilih'
  return [
    `Anda adalah Reliability Engineer bersertifikat CMRP untuk kinerja aset kilang.`,
    `Pola pikir Anda: berorientasi pada pelestarian FUNGSI aset (kegagalan fungsional, redundansi), deteksi dini melalui kurva P-F, analisis bad actor dan root cause (FRACAS/RCA), statistik keandalan (MTBF/MTTR), serta kritikalitas dan risiko.`,
    `Tugas Anda bukan menjelaskan data, melainkan mendiagnosis kondisi teknis ${focus} dan menetapkan tindakan prioritas. Jangan menggeser subjek menjadi RU kecuali ${focus} memang sebuah RU.`,
    buildSharedDiagnosticContext(insight, diagnosisEvidence, relatedNodeEvidence),
    ``,
    `Isi setiap bagian output (mengikuti struktur Fact Finding → Analysis → Summary di atas) dengan altitude seorang reliability engineer:`,
    `- Fact Finding: tabel kondisi ${focus} — fungsi & status redundansi, sinyal MTBF/MTTR & running hours, riwayat issue/bad actor, status work order & readiness, kritikalitas. Angka hanya di tabel ini.`,
    `- Analysis (poin-poin): kondisi fungsi dan ancaman kegagalan fungsional; posisi pada kurva P-F dan implikasi interval inspeksi; arah root cause untuk kegagalan berulang; risiko operasional bila kondisi tidak diselesaikan. Teknik condition monitoring hanya boleh DIREKOMENDASIKAN — jangan mengklaim hasilnya bila tidak tersedia.`,
    `- Summary: ikuti format icon + diagnosis + Confidence Level + rekomendasi bernomor di atas. Paragraf diagnosis membahas kondisi ${focus}; daftar bernomor berisi tindakan teknis prioritas (bedakan stabilisasi segera, perbaikan korektif jangka pendek, dan eliminasi cacat jangka menengah).`,
    `\nDi dalam bagian Fact Finding — TEPAT setelah tabel dan paragraf ringkasan, SEBELUM judul "## 2. Analysis" — sisipkan hingga 3 chart widget interaktif inline (bar/line/pie) dari data numerik di atas, dirender LANGSUNG di dalam alur teks jawaban pada titik itu. Pilih hingga 3 dari: status work order (open/closed/tertahan material/prioritas tinggi), biaya rencana vs aktual, status operasi normal vs tidak normal, atau distribusi issue/bad actor. Hanya buat chart untuk data yang benar-benar tersedia — lewati chart bila datanya 0/kosong, JANGAN mengarang atau memaksakan chart. WAJIB gunakan chart widget interaktif inline bawaan Anda yang tampil menyatu di dalam pesan — BUKAN gambar/PNG statis, BUKAN output matplotlib atau code interpreter, BUKAN kartu/panel/canvas terpisah di luar alur pesan. JANGAN render chart apa pun sebelum judul "## 1. Fact Finding", di awal jawaban, atau di lokasi lain mana pun — semua chart hanya muncul sekali, dikelompokkan di titik yang ditentukan. JANGAN sajikan data ini sebagai tabel markdown, tabel Excel, daftar dua kolom, atau ASCII art.`,
  ].join('\n')
}

function buildReliabilityManagerDiagnosticPrompt(insight: GraphInsight, diagnosisEvidence: DiagnosisEvidencePack, relatedNodeEvidence: RelatedNodeEvidence) {
  const focus = insight.root?.label ?? 'equipment terpilih'
  return [
    `Anda adalah Reliability Manager yang berfokus pada EFEKTIVITAS — memastikan ${focus} terus menjalankan fungsinya (melakukan hal yang benar).`,
    `Pola pikir Anda: analisis akar masalah (RCA/FRACAS) untuk mencegah kegagalan berulang dan mengidentifikasi bad actor; mengevaluasi apakah strategi pemeliharaan saat ini (preventive/predictive/run-to-failure) masih tepat lewat pendekatan RCM; membaca posisi P-F dan interval inspeksi optimal; serta memprioritaskan berdasarkan matriks kritikalitas. Tujuan akhir Anda adalah menetapkan REKOMENDASI tindakan dan owner agar keandalan jangka panjang terjaga.`,
    `RU hanya konteks eskalasi/pembanding kecuali ${focus} memang sebuah RU.`,
    buildSharedDiagnosticContext(insight, diagnosisEvidence, relatedNodeEvidence),
    ``,
    `Isi setiap bagian output (Fact Finding → Analysis → Summary) dengan altitude seorang reliability manager (strategi/efektivitas):`,
    `- Fact Finding: tabel sinyal keandalan & bad actor ${focus} — MTBF/MTTR, riwayat issue, kritikalitas, status fungsi. Angka hanya di tabel ini.`,
    `- Analysis (poin-poin): kesesuaian strategi pemeliharaan saat ini (preventive/predictive/run-to-failure — pakai kerangka RCM); arah root cause dan identifikasi bad actor utama; posisi P-F dan interval inspeksi yang direkomendasikan; prioritas tindakan berdasarkan kritikalitas dan konsekuensi (keselamatan, lingkungan, produksi).`,
    `- Summary: ikuti format icon + diagnosis + Confidence Level + rekomendasi bernomor di atas; daftar bernomor berisi rekomendasi tindakan keandalan beserta owner fungsi yang jelas. Tutup dengan satu kalimat keterkaitan (di luar nomor): rekomendasi ini diteruskan ke Maintenance untuk dieksekusi, dan hasil as-found/as-left kembali menyempurnakan strategi.`,
    `\nDi dalam bagian Fact Finding — TEPAT setelah tabel dan paragraf ringkasan, SEBELUM judul "## 2. Analysis" — sisipkan hingga 3 chart widget interaktif inline (bar/line/pie) dari data numerik di atas, dirender LANGSUNG di dalam alur teks jawaban pada titik itu. Pilih hingga 3 dari: distribusi issue/bad actor (plus MTBF/MTTR bila tercatat), status operasi normal vs tidak normal, cakupan risiko RKAP (top-risk/delay/high-value), atau work order open vs closed sebagai disiplin closure. Hanya buat chart untuk data yang benar-benar tersedia — lewati chart bila datanya 0/kosong, JANGAN mengarang atau memaksakan chart. WAJIB gunakan chart widget interaktif inline bawaan Anda yang tampil menyatu di dalam pesan — BUKAN gambar/PNG statis, BUKAN output matplotlib atau code interpreter, BUKAN kartu/panel/canvas terpisah di luar alur pesan. JANGAN render chart apa pun sebelum judul "## 1. Fact Finding", di awal jawaban, atau di lokasi lain mana pun — semua chart hanya muncul sekali, dikelompokkan di titik yang ditentukan. JANGAN sajikan data ini sebagai tabel markdown, tabel Excel, daftar dua kolom, atau ASCII art.`,
  ].join('\n')
}

function buildMaintenanceManagerDiagnosticPrompt(insight: GraphInsight, diagnosisEvidence: DiagnosisEvidencePack, relatedNodeEvidence: RelatedNodeEvidence) {
  const focus = insight.root?.label ?? 'equipment terpilih'
  return [
    `Anda adalah Maintenance Manager yang berfokus pada EFISIENSI — mengeksekusi pekerjaan dengan benar dan sumber daya seminimal mungkin (melakukan hal dengan benar).`,
    `Pola pikir Anda: menerjemahkan rekomendasi menjadi Planning & Scheduling; memastikan readiness sumber daya "6-Tepat" (Material, Alat, Informasi, Waktu, Izin, Orang yang tepat) sebelum kerja dimulai; memaksimalkan wrench time (mengurangi waktu non-produktif); menjaga schedule compliance kelas dunia (>90%); serta menjaga biaya tetap optimal dengan prinsip "do it right the first time".`,
    `RU hanya konteks eskalasi/pembanding kecuali ${focus} memang sebuah RU.`,
    buildSharedDiagnosticContext(insight, diagnosisEvidence, relatedNodeEvidence),
    ``,
    `Isi setiap bagian output (Fact Finding → Analysis → Summary) dengan altitude seorang maintenance manager (eksekusi/efisiensi):`,
    `- Fact Finding: tabel beban kerja ${focus} — total/open/closed work order, umur backlog, order tertahan material/spare, order prioritas tinggi, dan biaya rencana vs aktual. Angka hanya di tabel ini.`,
    `- Analysis (poin-poin): disiplin eksekusi dan kepatuhan jadwal (dari status dan umur order); readiness 6-Tepat (Material dari data faktual; Alat/Izin/Orang/Informasi sebagai checklist yang harus dipastikan — recommend-only bila data belum ada); indikasi wrench time dari disiplin penyelesaian; efisiensi biaya dan variance rencana vs aktual.`,
    `- Summary: ikuti format icon + diagnosis + Confidence Level + rekomendasi bernomor di atas; daftar bernomor berisi rencana Planning & Scheduling 30/60/90 hari dengan owner serta KPI eksekusi yang dipantau (target schedule compliance >90%, backlog aging menurun). Tutup dengan satu kalimat keterkaitan (di luar nomor): eksekusi ini menindaklanjuti rekomendasi Reliability, dan hasil as-found/as-left dikembalikan ke Reliability.`,
    `\nDi dalam bagian Fact Finding — TEPAT setelah tabel dan paragraf ringkasan, SEBELUM judul "## 2. Analysis" — sisipkan hingga 3 chart widget interaktif inline (bar/line/pie) dari data numerik di atas, dirender LANGSUNG di dalam alur teks jawaban pada titik itu. Pilih hingga 3 dari: backlog WO per umur (<30 hari, 30-90 hari, >90 hari), work order open vs closed (proxy schedule compliance), biaya rencana vs aktual, atau porsi order tertahan material/spare. Hanya buat chart untuk data yang benar-benar tersedia — lewati chart bila datanya 0/kosong, JANGAN mengarang atau memaksakan chart. WAJIB gunakan chart widget interaktif inline bawaan Anda yang tampil menyatu di dalam pesan — BUKAN gambar/PNG statis, BUKAN output matplotlib atau code interpreter, BUKAN kartu/panel/canvas terpisah di luar alur pesan. JANGAN render chart apa pun sebelum judul "## 1. Fact Finding", di awal jawaban, atau di lokasi lain mana pun — semua chart hanya muncul sekali, dikelompokkan di titik yang ditentukan. JANGAN sajikan data ini sebagai tabel markdown, tabel Excel, daftar dua kolom, atau ASCII art.`,
  ].join('\n')
}

function buildVpDiagnosticPrompt(insight: GraphInsight, diagnosisEvidence: DiagnosisEvidencePack, relatedNodeEvidence: RelatedNodeEvidence) {
  const focus = insight.root?.label ?? 'equipment terpilih'
  return [
    `Anda adalah VP Reliability/Asset Management — jembatan antara realitas teknis di lapangan dan visi strategis perusahaan.`,
    `Pola pikir Anda: menilai dampak strategis & finansial setiap rekomendasi sebagai business case (apakah memberi return yang memadai); mengelola risiko korporat multi-dimensi (finansial, integritas aset, keselamatan/HSE, dan — bila relevan — legal/citra); menyelaraskan empat pilar (People, Process, Technology, Financial) sepanjang siklus hidup aset; memantau lagging KPI tingkat direksi; serta mendorong perbaikan berkelanjutan dan kematangan informasi aset menuju level optimising.`,
    `Altitude jawaban: MULAI dari ${focus} sebagai pemicu, lalu ANGKAT ke implikasi RU/portfolio dan keputusan investasi. PENTING — Anda tidak memiliki angka OEE, ROI, RoNA, atau profitabilitas: perlakukan keempatnya sebagai PERTANYAAN assurance yang harus dijawab, JANGAN mengarang nilainya. Exposure biaya program yang tersedia hanyalah satu sisi dari business case.`,
    buildSharedDiagnosticContext(insight, diagnosisEvidence, relatedNodeEvidence),
    ``,
    `Isi setiap bagian output (Fact Finding → Analysis → Summary) dengan altitude eksekutif:`,
    `- Fact Finding: tabel paparan strategis — exposure biaya program (RKAP), jumlah program top-risk/high-value/delay, beban keandalan & backlog yang material, serta indikasi kematangan informasi aset. Angka hanya di tabel ini. Naikkan ke level RU bila polanya berulang.`,
    `- Analysis (poin-poin): business case — paparan biaya dan risiko, ajukan ROI/RoNA sebagai pertanyaan bukan angka; risiko korporat multi-dimensi (finansial & integritas aset faktual; keselamatan/HSE & produksi kualitatif); penyelarasan empat pilar (Financial & Process faktual; People & Technology sebagai pertanyaan organizational readiness); OEE sebagai pertanyaan assurance, bukan klaim.`,
    `- Summary: ikuti format icon + diagnosis + Confidence Level + rekomendasi bernomor di atas; paragraf diagnosis ditutup dengan arah perbaikan berkelanjutan (Living Program). Daftar bernomor berisi tiga keputusan/prioritas investasi paling penting beserta trade-off (eksekusi cepat vs assurance vs penyelarasan program) dan pertanyaan assurance kunci (termasuk ROI/OEE) yang harus terjawab sebelum risiko dinyatakan terkendali.`,
    `\nDi dalam bagian Fact Finding — TEPAT setelah tabel dan paragraf ringkasan, SEBELUM judul "## 2. Analysis" — sisipkan hingga 3 chart widget interaktif inline (bar/line/pie) dari data numerik di atas, dirender LANGSUNG di dalam alur teks jawaban pada titik itu. Pilih hingga 3 dari: distribusi exposure biaya RKAP, cakupan risiko RKAP (top-risk/delay/high-value), biaya pemeliharaan rencana vs aktual, atau porsi program RKAP terhubung pasti vs kemungkinan. Hanya buat chart untuk data yang benar-benar tersedia — lewati chart bila datanya 0/kosong, JANGAN mengarang atau memaksakan chart. WAJIB gunakan chart widget interaktif inline bawaan Anda yang tampil menyatu di dalam pesan — BUKAN gambar/PNG statis, BUKAN output matplotlib atau code interpreter, BUKAN kartu/panel/canvas terpisah di luar alur pesan. JANGAN render chart apa pun sebelum judul "## 1. Fact Finding", di awal jawaban, atau di lokasi lain mana pun — semua chart hanya muncul sekali, dikelompokkan di titik yang ditentukan. JANGAN sajikan data ini sebagai tabel markdown, tabel Excel, daftar dua kolom, atau ASCII art.`,
  ].join('\n')
}

function EntityInspector({ node, edge, dataset, paths, onClose }: { node?: GraphNode; edge?: GraphEdge; dataset: DatasetSummary; paths?: NonNullable<GraphSlice['paths']>; onClose?: () => void }) {
  const [detail, setDetail] = useState<(GraphNode & { domain_record?: Record<string, unknown> })>()
  const [edgeDetail, setEdgeDetail] = useState<GraphEdgeDetail>()
  const [tab, setTab] = useState<'details' | 'provenance' | 'properties' | 'raw' | 'paths'>('details')
  useEffect(() => { if (node) void api.node(dataset.id, node.id).then(setDetail) }, [node, dataset.id])
  useEffect(() => { if (edge) void api.relationship(dataset.id, edge.id).then(setEdgeDetail) }, [edge, dataset.id])
  useEffect(() => { setTab('details') }, [node?.id, edge?.id])
  const current = detail?.id === node?.id ? detail : node
  const currentEdge = edgeDetail?.id === edge?.id ? edgeDetail : edge
  return (
    <aside className="inspector panel">
      {!current && !currentEdge ? <div className="inspector-empty"><span>◎</span><p>Pilih node atau edge untuk melihat properties dan provenance.</p></div> : currentEdge ? <>
        <div className="inspector-title"><span className="node-pip edge" /><div><span className="eyebrow">Relationship</span><h3>{human(currentEdge.type)}</h3><small>{currentEdge.id}</small></div>{onClose && <button className="icon-button mini" onClick={onClose}>×</button>}</div>
        <div className="inspector-tabs">
          {(['details', 'properties', 'provenance', 'raw'] as const).map((item) => <button key={item} className={tab === item ? 'active' : ''} onClick={() => setTab(item)}>{human(item)}</button>)}
        </div>
        {tab === 'details' && <div className="inspector-summary">
          <div><b>Source</b><span>{currentEdge.source_node?.label ?? String(currentEdge.properties?.derived_source_label ?? currentEdge.source)}</span></div>
          <div><b>Target</b><span>{currentEdge.target_node?.label ?? String(currentEdge.properties?.derived_target_label ?? currentEdge.target)}</span></div>
          <div><b>Domain</b><span>{currentEdge.domain || '—'}</span></div>
          <div><b>Confidence</b><span>{currentEdge.confidence ?? '—'}</span></div>
          <div><b>Match method</b><span>{currentEdge.match_method || '—'}</span></div>
          <div><b>Candidate</b><span>{currentEdge.is_candidate ? 'Yes' : 'No'}</span></div>
        </div>}
        {tab === 'properties' && <PropertyList values={currentEdge.properties ?? {}} priority={['ru_consistency', 'source_ru', 'target_ru', 'match_quality_bucket', 'match_rule', 'match_source_column', 'match_token', 'match_token_compact', 'match_equipment_code', 'review_priority', 'reject_reason', 'derived_match_explain', 'derived_match_token', 'derived_match_quality_bucket', 'equipment_code_normalized', 'readiness_equipment_raw', 'readiness_equipment_token', 'inspection_equipment_raw', 'inspection_equipment_token', 'equipment_code_raw', 'matched_token', 'shortcut', 'derived_review_priority', 'derived_candidate_reason', 'derived_shortcut_warning']} />}
        {tab === 'provenance' && <div className="provenance">
          <span className="eyebrow">Provenance</span>
          <div><b>Workbook</b><span>{currentEdge.source_ref.workbook || '—'}</span></div>
          <div><b>Sheet</b><span>{currentEdge.source_ref.sheet || '—'}</span></div>
          <div><b>Row</b><span>{currentEdge.source_ref.row ?? '—'}</span></div>
          <div><b>Record ID</b><span className="mono">{currentEdge.source_ref.record_id || '—'}</span></div>
        </div>}
        {tab === 'raw' && <PropertyList values={currentEdge as unknown as Record<string, unknown>} />}
      </> : current ? <>
        <div className="inspector-title"><span className="node-pip" /><div><span className="eyebrow">{human(current.kind)}</span><h3>{current.label}</h3><small>{current.subtitle}</small></div>{onClose && <button className="icon-button mini" onClick={onClose}>×</button>}</div>
        <div className="inspector-tabs">
          {(['details', 'properties', 'provenance', 'paths', 'raw'] as const).map((item) => <button key={item} className={tab === item ? 'active' : ''} onClick={() => setTab(item)}>{human(item)}</button>)}
        </div>
        {tab === 'details' && <div className="inspector-summary"><div><b>Node ID</b><span className="mono">{current.id}</span></div><div><b>Type</b><span>{human(current.kind)}</span></div><div><b>Domain</b><span>{current.domain || '—'}</span></div><div><b>RU</b><span>{current.refinery_unit || String(current.properties.refinery_unit ?? current.properties.ru ?? '—')}</span></div></div>}
        {tab === 'properties' && <PropertyList values={current.properties} priority={propertyPriority(current.kind)} />}
        {tab === 'provenance' && <div className="provenance">
          <span className="eyebrow">Provenance</span>
          <div><b>Workbook</b><span>{current.source.workbook || '—'}</span></div>
          <div><b>Sheet</b><span>{current.source.sheet || '—'}</span></div>
          <div><b>Row</b><span>{current.source.row ?? '—'}</span></div>
          <div><b>Record ID</b><span className="mono">{current.source.record_id || '—'}</span></div>
        </div>}
        {tab === 'paths' && <div className="inspector-paths">{(paths ?? []).filter((path) => path.node_id_path.includes(current.id)).slice(0, 8).map((path, index) => <div key={index}><b>{path.depth} hop</b><span>{path.label_path.map((label) => shortText(label, 22)).join(' → ')}</span></div>)}{!(paths ?? []).some((path) => path.node_id_path.includes(current.id)) && <p>Tidak ada deep path yang melibatkan node ini pada tampilan aktif.</p>}</div>}
        {tab === 'raw' && <PropertyList values={current as unknown as Record<string, unknown>} />}
      </> : null}
    </aside>
  )
}

function DirectedPathPanel({ paths }: { paths: NonNullable<GraphSlice['paths']> }) {
  return (
    <div className="directed-path-panel">
      <div className="directed-path-heading"><span className="eyebrow">Deep directed paths</span><b>{paths.length} path dengan &gt;2 turunan</b></div>
      <div className="directed-path-list">
        {paths.slice(0, 8).map((path, index) => (
          <div key={index}>
            <span>{path.depth} hop</span>
            <strong>{path.label_path.map((label) => shortText(label, 28)).join(' → ')}</strong>
            <small>{path.relationship_path.map(human).join(' → ')}</small>
          </div>
        ))}
      </div>
    </div>
  )
}

function Equipment360({ dataset }: { dataset?: DatasetSummary }) {
  const [query, setQuery] = useState('')
  const [results, setResults] = useState<GraphNode[]>([])
  const [view, setView] = useState<{ equipment: GraphNode; related: EquipmentRelated[] }>()
  useEffect(() => {
    if (!dataset) return
    const timer = window.setTimeout(() => void api.search(dataset.id, query, 'equipment', '', 20).then(setResults), 180)
    return () => window.clearTimeout(timer)
  }, [dataset, query])
  if (!dataset) return <NoDataset />
  const grouped = (view?.related ?? []).reduce<Record<string, EquipmentRelated[]>>((result, item) => {
    const group = domainGroup(item)
    result[group] = [...(result[group] ?? []), item]
    return result
  }, {})
  // Kandidat (tag-match) ditampilkan setelah relasi tercatat di tiap kartu domain.
  const groupCounts = (items: EquipmentRelated[]) => {
    const candidate = items.filter((item) => item.is_candidate).length
    return { recorded: items.length - candidate, candidate }
  }
  const groupSubtitle = (items: EquipmentRelated[]) => {
    const { recorded, candidate } = groupCounts(items)
    return candidate ? `${recorded} tercatat · ${candidate} kandidat` : `${recorded} connected record`
  }
  return (
    <section className="stack">
      <div className="equipment-search panel">
        <div><span className="eyebrow">Equipment lookup</span><h2>Temukan aset dan seluruh konteksnya</h2></div>
        <div className="search-box wide"><SearchIcon /><input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Cari tag, equipment ID, atau deskripsi…" /></div>
        {/* Dropdown tetap aktif walau detail equipment sedang tampil — pilih hasil langsung mengganti equipment tanpa tombol "Ganti equipment". */}
        {query && results.length > 0 && <div className="search-dropdown">{results.map((node) => <button key={node.id} onClick={() => { void api.equipment360(dataset.id, node.id).then(setView); setQuery('') }}><strong>{node.label}</strong><span>{node.subtitle}</span></button>)}</div>}
      </div>
      {!view ? <EmptyState icon={<EquipmentIcon />} title="Equipment 360 siap digunakan" text="Cari equipment untuk membuka hierarchy, maintenance, reliability, inspection, issue, readiness, RKAP program, dan timeline." /> : <>
        <div className="equipment-hero">
          <div className="equipment-avatar"><EquipmentIcon /></div>
          <div><span className="eyebrow">{view.equipment.subtitle}</span><h2>{view.equipment.label}</h2><p>{String(view.equipment.properties.description ?? view.equipment.domain ?? 'Equipment master')}</p></div>
          <button className="secondary" onClick={() => { setView(undefined); setQuery('') }}>Ganti equipment</button>
        </div>
        <div className="metrics">
          {Object.entries(grouped).slice(0, 4).map(([group, items], index) => <Metric key={group} label={human(group)} value={items?.length ?? 0} accent={['mint', 'blue', 'violet', 'amber'][index]} />)}
        </div>
        <div className="record-columns">
          {Object.entries(grouped).map(([group, items]) => (
            <section className="panel record-group" key={`${view.equipment.id}-${group}`}>
              <PanelTitle title={human(group)} subtitle={groupSubtitle(items ?? [])} />
              <Paged items={[...(items ?? [])].sort((a, b) => Number(a.is_candidate ?? false) - Number(b.is_candidate ?? false))}>
                {(rows) => rows.map((item) => (
                  <div className={`record-card${item.is_candidate ? ' is-candidate' : ''}`} key={item.id}>
                    <span className="record-date">{recordDate(item)}</span>
                    <strong>{item.label}{item.is_candidate && <span className="candidate-badge">kandidat</span>}</strong>
                    <small>{human(item.relationship_type)}{item.is_candidate ? ' · cocok tag, belum tercatat' : ''}</small>
                  </div>
                ))}
              </Paged>
            </section>
          ))}
        </div>
      </>}
    </section>
  )
}

function DepthExplorer({ dataset }: { dataset?: DatasetSummary }) {
  const [paths, setPaths] = useState<Record<string, unknown>[]>([])
  const [schema, setSchema] = useState<Record<string, unknown>[]>([])
  const [ontology, setOntology] = useState<Record<string, unknown>[]>([])
  useEffect(() => {
    if (!dataset) return
    void api.schema(dataset.id).then((bundle) => { setPaths(bundle.deepest_paths); setSchema(bundle.graph_schema); setOntology(bundle.ontology_depth) })
  }, [dataset])
  const domainSummary = useMemo(() => buildDepthDomainSummary(schema, ontology), [schema, ontology])
  const pathRows = useMemo(() => [...deriveSchemaPathFallback(schema, ontology, paths), ...paths], [schema, ontology, paths])
  if (!dataset) return <NoDataset />
  return (
    <section className="stack">
      <div className="hero-panel compact-hero"><div><span className="eyebrow">Path analysis</span><h2>Depth & ontology explorer</h2><p>Jalur ditampilkan sebagai sequence dan graph bersiklus tidak dipaksa menjadi tree.</p></div></div>
      {!pathRows.length && !schema.length ? <EmptyState icon={<ChevronIcon />} title="Analisis depth belum tersedia" text="Paket ETL belum menyertakan deepest_paths atau graph_schema. Graph tetap dapat ditelusuri dari Graph Explorer." /> : (
        <>
        <section className="domain-path-grid">
          {domainSummary.map((item) => <article className={`domain-path-card ${item.key}`} key={item.key}>
            <span>{item.label}</span>
            <strong>{format(item.relationshipCount)}</strong>
            <small>{format(item.pathCount)} ontology path · {format(item.schemaCount)} relationship type</small>
          </article>)}
        </section>
        <div className="two-column depth-layout">
          <section className="panel"><PanelTitle title="Path families & samples" subtitle="Fallback schema path ditampilkan saat sample deepest path belum lengkap" />{pathRows.slice(0, 28).map((path, index) => <div className={`path-card ${path.is_schema_fallback ? 'schema-fallback' : ''}`} key={String(path.path_id ?? `${path.path_pattern}-${index}`)}><div><b>{String(path.path_pattern ?? path.start_node_type ?? 'Start')}</b>{path.is_schema_fallback ? <StatusBadge status="Schema path" /> : null}</div><span>{String(path.path_depth ?? path.depth ?? '—')} hop · {String(path.label_path ?? path.node_path ?? path.analysis_scope ?? 'bounded search')}</span></div>)}</section>
          <section className="panel"><PanelTitle title="Graph schema" subtitle="Relasi aktual antar node type" />{schema.slice(0, 30).map((item, index) => <div className="schema-row" key={index}><span>{String(item.source_node_type)}</span><b>{human(String(item.relationship_type))}</b><span>{String(item.target_node_type)}</span><em>{format(Number(item.relationship_count ?? 0))}</em></div>)}</section>
        </div>
        </>
      )}
    </section>
  )
}

function DataReview({ dataset }: { dataset?: DatasetSummary }) {
  const [type, setType] = useState('unmatched_identifier')
  const [data, setData] = useState<{ total: number; items: unknown[] }>({ total: 0, items: [] })
  useEffect(() => {
    if (!dataset) return
    void (type ? api.audit(dataset.id, type) : api.issues(dataset.id, '')).then(setData)
  }, [dataset, type])
  if (!dataset) return <NoDataset />
  const rows = data.items as Array<Record<string, unknown>>
  return (
    <section className="stack">
      <div className="review-toolbar">
        <div><span className="eyebrow">Quality queue</span><h2>{format(data.total)} issue ditemukan</h2></div>
        <div>
          <select value={type} onChange={(event) => setType(event.target.value)}><option value="">Semua issue</option>{['unmatched_identifier', 'ambiguous_match', 'invalid_value', 'relationship_candidates', 'broken_relationship'].map((item) => <option key={item}>{item}</option>)}</select>
          <a className="secondary button-link" href={api.exportUrl(dataset.id, 'review')}><DownloadIcon />Export CSV</a>
        </div>
      </div>
      <section className="panel table-panel">
        <table><thead><tr><th>Type</th><th>Identifier / Edge</th><th>Message / Relationship</th><th>Source</th><th>Confidence / Row</th></tr></thead>
          <tbody>{rows.map((item, index) => {
            const issue = item as unknown as ReviewIssue
            const edge = item as { id?: string; type?: string; source_ref?: { workbook?: string; sheet?: string; row?: number }; confidence?: number }
            return <tr key={`${issue.issue_type ?? edge.id}-${index}`}><td><StatusBadge status={human(String(issue.issue_type ?? 'candidate'))} /></td><td className="mono">{String(issue.identifier ?? edge.id ?? '—')}</td><td>{String(issue.message ?? edge.type ?? '—')}</td><td>{String(issue.source_file ?? edge.source_ref?.workbook ?? '')}<small>{String(issue.source_sheet ?? edge.source_ref?.sheet ?? '')}</small></td><td>{String(edge.confidence ?? issue.source_row ?? '—')}</td></tr>
          })}</tbody>
        </table>
        {!data.items.length && <div className="table-empty"><CheckIcon />Tidak ada issue pada filter ini.</div>}
      </section>
    </section>
  )
}

function DatasetManager({ datasets, activeId, onActivate, onRefresh, onResetAll }: { datasets: DatasetSummary[]; activeId: string; onActivate: (id: string) => void; onRefresh: () => Promise<void>; onResetAll: () => void }) {
  const [deleting, setDeleting] = useState<string | null>(null)
  const [resetting, setResetting] = useState(false)
  const [recovering, setRecovering] = useState(false)
  const [recoverMsg, setRecoverMsg] = useState('')
  const [expanded, setExpanded] = useState<string | null>(null)
  const [fileRows, setFileRows] = useState<Record<string, LoadSummaryRow[]>>({})
  const [fileLoading, setFileLoading] = useState<string | null>(null)
  const [syncTarget, setSyncTarget] = useState<DatasetSummary | null>(null)
  const [syncMode, setSyncMode] = useState<'replace' | 'append'>('replace')
  const [syncing, setSyncing] = useState(false)
  const [syncJob, setSyncJob] = useState<ImportJob | null>(null)
  const syncInputRef = useRef<HTMLInputElement>(null)
  const [rebuilding, setRebuilding] = useState<string | null>(null)
  const [rebuildResult, setRebuildResult] = useState<{ id: string; count: number } | null>(null)

  const toggle = async (id: string) => {
    if (expanded === id) { setExpanded(null); return }
    setExpanded(id)
    if (fileRows[id]) return
    setFileLoading(id)
    try {
      const rows = await api.loadSummary(id)
      setFileRows(prev => ({ ...prev, [id]: rows }))
    } finally {
      setFileLoading(null)
    }
  }

  const rename = async (dataset: DatasetSummary) => {
    const name = window.prompt('Nama dataset baru', dataset.name)
    if (!name) return
    await api.renameDataset(dataset.id, name)
    await onRefresh()
  }
  const remove = async (dataset: DatasetSummary) => {
    if (!window.confirm(
      `Hapus dataset "${dataset.name}"?\n\nSemua node (${format(dataset.node_count)}) dan relasi (${format(dataset.edge_count)}) di knowledge graph akan dihapus permanen.`
    )) return
    setDeleting(dataset.id)
    try {
      await api.deleteDataset(dataset.id)
      setFileRows(prev => { const n = { ...prev }; delete n[dataset.id]; return n })
      await onRefresh()
    } finally {
      setDeleting(null)
    }
  }

  const resetAll = async () => {
    const totalNodes = datasets.reduce((s, d) => s + d.node_count, 0)
    const totalEdges = datasets.reduce((s, d) => s + d.edge_count, 0)
    if (!window.confirm(
      `⚠️ RESET TOTAL — hapus SEMUA data?\n\n` +
      `• ${datasets.length} dataset\n` +
      `• ${format(totalNodes)} node\n` +
      `• ${format(totalEdges)} relasi\n\n` +
      `Ketik OK untuk konfirmasi. Tindakan ini tidak bisa dibatalkan.`
    )) return
    setResetting(true)
    try {
      await api.resetAll()
      setFileRows({})
      setExpanded(null)
      await onRefresh()
      onResetAll()
    } finally {
      setResetting(false)
    }
  }

  const rebuildRelationships = async (dataset: DatasetSummary) => {
    setRebuilding(dataset.id)
    setRebuildResult(null)
    try {
      const job = await api.rebuildRelationships(dataset.id)
      // Poll sampai selesai
      let j = job
      while (j.status === 'queued' || j.status === 'running') {
        await new Promise(r => setTimeout(r, 2000))
        j = await api.importStatus(j.id)
      }
      if (j.status === 'completed') {
        const count = parseInt(j.message?.match(/[\d,.]+/)?.[0]?.replace(/[,.]/g, '') ?? '0')
        setRebuildResult({ id: dataset.id, count })
        await onRefresh()
      } else {
        alert('Gagal rebuild relasi: ' + (j.error ?? 'Unknown error'))
      }
    } catch (err) {
      alert('Gagal rebuild relasi: ' + String(err))
    } finally {
      setRebuilding(null)
    }
  }

  const startSync = (dataset: DatasetSummary, mode: 'replace' | 'append' = 'replace') => {
    setSyncTarget(dataset)
    setSyncMode(mode)
    setSyncJob(null)
    setTimeout(() => syncInputRef.current?.click(), 50)
  }

  const handleSyncFiles = async (e: React.ChangeEvent<HTMLInputElement>) => {
    if (!syncTarget || !e.target.files?.length) return
    const files = Array.from(e.target.files)
    e.target.value = ''
    setSyncing(true)
    try {
      const entries = files.map(f => ({
        file: f,
        name: f.name,
        total_chunks: Math.max(1, Math.ceil(f.size / ETL_CHUNK_SIZE)),
      }))
      const { upload_id } = await api.initChunkedUpload(
        syncTarget.name,
        entries.map(en => ({ name: en.name, total_chunks: en.total_chunks })),
        syncMode === 'append' ? 'etl_append' : 'etl',
        syncTarget.id,
      )
      for (const entry of entries) {
        for (let i = 0; i < entry.total_chunks; i++) {
          const chunk = entry.file.slice(i * ETL_CHUNK_SIZE, (i + 1) * ETL_CHUNK_SIZE)
          await api.uploadChunk(upload_id, entry.name, i, chunk)
        }
      }
      const job = await api.commitChunkedUpload(upload_id)
      setSyncJob(job)
      const poll = setInterval(async () => {
        const updated = await api.importStatus(job.id)
        setSyncJob(updated)
        if (updated.status === 'completed' || updated.status === 'failed' || updated.status === 'cancelled') {
          clearInterval(poll)
          setSyncing(false)
          if (updated.status === 'completed') {
            setFileRows(prev => { const n = { ...prev }; delete n[syncTarget.id]; return n })
            await onRefresh()
          }
        }
      }, 1500)
    } catch (err) {
      setSyncing(false)
      alert('Sinkronisasi gagal: ' + String(err))
    }
  }

  const fmtDate = (iso: string) => {
    try { return new Date(iso).toLocaleString('id-ID', { day: '2-digit', month: 'short', year: 'numeric', hour: '2-digit', minute: '2-digit' }) }
    catch { return iso }
  }

  const modeLabel: Record<string, string> = {
    graph_contract: 'ETL contract',
    exact_match_fallback: 'Exact match',
    etl_csv_graph: 'ETL CSV',
  }

  const statusIcon = (s: string) => {
    if (!s) return '—'
    if (s === 'ok' || s === 'success') return '✅'
    if (s === 'partial') return '⚠️'
    if (s === 'error' || s === 'failed') return '❌'
    return s
  }

  return (
    <section className="stack">
      <div className="hero-panel compact-hero">
        <div>
          <span className="eyebrow">Knowledge graph</span>
          <h2>Daftar dataset</h2>
          <p>Setiap dataset menyimpan node dan relasi knowledge graph di PostgreSQL. Menghapus dataset menghapus seluruh datanya secara permanen.</p>
        </div>
        <div className="hero-stat">
          <span className="hero-stat-num">{datasets.length}</span>
          <span className="eyebrow">dataset tersedia</span>
          {false && datasets.length > 0 && (
            <button
              className="reset-all-btn"
              onClick={() => void resetAll()}
              disabled={resetting}
              title="Hapus semua dataset, node, dan relasi — kembali ke kondisi kosong"
            >
              {resetting ? 'Mereset…' : '🗑 Reset semua'}
            </button>
          )}
        </div>
      </div>

      {!datasets.length && <NoDataset />}

      <div style={{ display: 'flex', alignItems: 'center', gap: 12, padding: '12px 0' }}>
        <button
          className="secondary small"
          disabled={recovering}
          onClick={async () => {
            setRecovering(true)
            setRecoverMsg('')
            try {
              const res = await fetch('/api/recover-datasets', { method: 'POST' })
              const data = await res.json()
              if (data.count > 0) {
                setRecoverMsg(`Berhasil memulihkan ${data.count} dataset.`)
                await onRefresh()
              } else {
                setRecoverMsg('Tidak ada data yang bisa dipulihkan di database.')
              }
            } catch {
              setRecoverMsg('Gagal menghubungi server.')
            } finally {
              setRecovering(false)
            }
          }}
        >{recovering ? 'Memeriksa database…' : 'Pulihkan dataset dari database'}</button>
        {recoverMsg && <span style={{ fontSize: 'var(--fs-sm)', color: 'var(--text-muted)' }}>{recoverMsg}</span>}
      </div>

      {datasets.length > 0 && (
        <div className="dm-table-wrap">
          <table className="dm-table">
            <thead>
              <tr>
                <th>Nama dataset</th>
                <th className="num">Node</th>
                <th className="num">Relasi</th>
                <th>Dibuat</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              {datasets.map((dataset) => (
                <>
                  <tr key={dataset.id} className={`dm-row ${dataset.id === activeId ? 'dm-active' : ''}`}>
                    <td>
                      <button className="dm-name-btn" onClick={() => void toggle(dataset.id)}>
                        <span className="dm-chevron">{expanded === dataset.id ? '▾' : '▸'}</span>
                        <span>{dataset.name}</span>
                        {dataset.id === activeId && <span className="dm-badge">Aktif</span>}
                      </button>
                    </td>
                    <td className="num">{format(dataset.node_count)}</td>
                    <td className="num">{format(dataset.edge_count)}</td>
                    <td className="dm-date">{fmtDate(dataset.created_at)}</td>
                    <td>
                      <div className="dm-actions">
                        <button className="primary small" onClick={() => onActivate(dataset.id)}>
                          {dataset.id === activeId ? 'Aktif' : 'Buka'}
                        </button>
                        <button className="secondary small" onClick={() => void rename(dataset)}>Rename</button>
                        <button
                          className="secondary small"
                          onClick={() => void rebuildRelationships(dataset)}
                          disabled={rebuilding === dataset.id}
                          title="Rebuild relasi antar node dari data yang sudah ada di database"
                        >
                          {rebuilding === dataset.id ? '⏳ Membangun…' : '🔗 Rebuild Relasi'}
                        </button>
                      </div>
                    </td>
                  </tr>
                  {expanded === dataset.id && (
                    <tr key={`${dataset.id}-files`} className="dm-files-row">
                      <td colSpan={5}>
                        <div className="dm-files">
                          {dataset.workbooks.length > 0 ? (
                            <div className="dm-file-groups">
                              <div className="dm-workbook-list-title">File yang diupload ({dataset.workbooks.length} file)</div>
                              {dataset.workbooks.map((wb, i) => (
                                <div key={i} className="dm-file-group">
                                  <div className="dm-file-group-header">
                                    <span className="dm-file-icon">📄</span>
                                    <span className="dm-file-group-name">{wb}</span>
                                  </div>
                                </div>
                              ))}
                            </div>
                          ) : (
                            <span className="dm-no-files">Tidak ada file yang tercatat.</span>
                          )}
                          <div className="dm-sync-actions" style={{ display: 'flex', gap: '8px', flexWrap: 'wrap', marginTop: '10px', alignItems: 'center' }}>
                            <button
                              className="secondary small"
                              disabled={syncing}
                              onClick={() => startSync(dataset, 'append')}
                              title="Upload file Excel domain tambahan — data lama dipertahankan, hanya menambah node/relasi baru. Jalankan Rebuild Relasi setelah semua file masuk."
                            >
                              ➕ Tambah data (gabung)
                            </button>
                            <button
                              className="secondary small"
                              disabled={syncing}
                              onClick={() => startSync(dataset, 'replace')}
                              title="Ganti seluruh isi dataset dengan file Excel baru (data lama dihapus)."
                            >
                              🔄 Upload ulang (timpa semua)
                            </button>
                            <span style={{ fontSize: '12px', color: 'var(--muted)' }}>
                              Tambah data = upload domain satu per satu tanpa menghapus yang lama, lalu klik <b>Rebuild Relasi</b>.
                            </span>
                          </div>
                        </div>
                      </td>
                    </tr>
                  )}
                </>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Hidden file input untuk sync */}
      <input
        ref={syncInputRef}
        type="file"
        accept=".xlsx,.xls"
        multiple
        style={{ display: 'none' }}
        onChange={e => void handleSyncFiles(e)}
      />

      {/* Hasil rebuild relasi */}
      {rebuildResult && (
        <div className="sync-status-panel">
          <div className="sync-status-header">
            <span>✅ Rebuild selesai — <strong>{rebuildResult.count.toLocaleString('id-ID')} relasi</strong> berhasil dibuat</span>
            <button className="secondary small" onClick={() => setRebuildResult(null)}>✕</button>
          </div>
        </div>
      )}

      {/* Status sinkronisasi */}
      {syncTarget && syncJob && (
        <div className="sync-status-panel">
          <div className="sync-status-header">
            <span>Sinkronisasi: <strong>{syncTarget.name}</strong></span>
            {(syncJob.status === 'completed' || syncJob.status === 'failed') && (
              <button className="secondary small" onClick={() => { setSyncTarget(null); setSyncJob(null) }}>✕ Tutup</button>
            )}
          </div>
          <div className="sync-progress-bar">
            <div className="sync-progress-fill" style={{ width: `${syncJob.progress}%` }} />
          </div>
          <div className="sync-phase">{syncJob.phase}</div>
          {syncJob.message && <div className="sync-message">{syncJob.message}</div>}
          {syncJob.status === 'completed' && <div className="sync-ok">✅ Sinkronisasi selesai</div>}
          {syncJob.status === 'failed' && <div className="sync-err">❌ Gagal: {syncJob.error}</div>}
        </div>
      )}
    </section>
  )
}

// Pagination standar: maksimal `pageSize` baris per halaman (default 10).
// Render-prop supaya tiap pemakaian punya state page sendiri (hooks tetap aman
// walau jumlah tabel per halaman berbeda antar view).
function Paged<T>({ items, pageSize = 10, children }: { items: T[]; pageSize?: number; children: (rows: T[]) => ReactNode }) {
  const [page, setPage] = useState(0)
  const pageCount = Math.max(1, Math.ceil(items.length / pageSize))
  const safe = Math.min(page, pageCount - 1)
  return <>
    {children(items.slice(safe * pageSize, (safe + 1) * pageSize))}
    {pageCount > 1 && (
      <div className="pager">
        <button className="icon-button mini" disabled={safe === 0} onClick={() => setPage(safe - 1)} aria-label="Halaman sebelumnya">‹</button>
        <span>{safe + 1} / {pageCount} · {format(items.length)} baris</span>
        <button className="icon-button mini" disabled={safe >= pageCount - 1} onClick={() => setPage(safe + 1)} aria-label="Halaman berikutnya">›</button>
      </div>
    )}
  </>
}

function Metric({ label, value, accent }: { label: string; value?: number; accent: string }) {
  return <div className={`metric ${accent}`}><span>{label}</span><strong>{value == null ? '—' : format(value)}</strong><i /></div>
}
function PanelTitle({ title, subtitle }: { title: string; subtitle: string }) { return <div className="panel-heading"><h2>{title}</h2><p>{subtitle}</p></div> }
function StatusBadge({ status }: { status: string }) { return <span className={`status-badge ${slug(status)}`}><span />{status}</span> }
function RiskPill({ value }: { value: number }) {
  const level = value >= 50 ? 'high' : value >= 25 ? 'medium' : 'low'
  return <span className={`risk-pill ${level}`}>{decimal(value)}</span>
}
function NoDataset() { return <EmptyState icon={<DatabaseIcon />} title="Pilih atau import dataset" text="Fitur ini memerlukan dataset aktif." /> }
function ComputingBanner() {
  return (
    <div className="panel computing-banner">
      <span className="spinner" aria-hidden />
      <span>Menyiapkan ringkasan dari dataset besar (sekali hitung, hasil di-cache). Angka akan muncul otomatis dalam beberapa saat…</span>
    </div>
  )
}
function EmptyState({ icon, title, text, action, onAction }: { icon: ReactNode; title: string; text: string; action?: string; onAction?: () => void }) {
  return <div className="empty-state panel"><div>{icon}</div><h2>{title}</h2><p>{text}</p>{action && <button className="primary" onClick={onAction}>{action}</button>}</div>
}
function PropertyList({ values, priority = [] }: { values: Record<string, unknown>; priority?: string[] }) {
  const present = Object.entries(values).filter(([, value]) => value != null && value !== '')
  const priorityEntries = priority
    .map((key) => present.find(([itemKey]) => itemKey === key))
    .filter((entry): entry is [string, unknown] => Boolean(entry))
  const ordered = [
    ...priorityEntries,
    ...present.filter(([key]) => !priority.includes(key)),
  ]
  const entries = ordered.slice(0, 32)
  return <div className="property-list">{entries.map(([key, value]) => <div key={key}><b>{human(key)}</b><span>{typeof value === 'object' ? JSON.stringify(value) : String(value)}</span></div>)}</div>
}

function propertyPriority(kind: string) {
  const common = ['refinery_unit', 'plant', 'functional_location']
  const byKind: Record<string, string[]> = {
    equipment: [...common, 'criticallity', 'equipment_group', 'plant_area', 'description', 'derived_risk_score', 'derived_issue_count', 'derived_open_issue_count', 'derived_avg_mtbf', 'derived_avg_mttr', 'derived_abnormal_status_count', 'derived_inspection_count', 'derived_readiness_record_count', 'derived_rkap_program_count', 'derived_high_value_rkap_count'],
    reliability_observation: ['equipment', 'status', 'running_hours', 'mtbf', 'mttr', 'year', 'month', 'week', 'derived_period_key', 'derived_is_abnormal_status', 'derived_status_bucket', 'derived_mtbf_bucket', 'derived_mttr_bucket', 'derived_low_mtbf_flag', 'derived_high_mttr_flag'],
    maintenance_order: ['order', 'order_type', 'priority', 'status', 'plant', 'refinery_unit', 'derived_reference_date', 'derived_order_age_days', 'derived_is_open_order', 'derived_planned_cost', 'derived_actual_cost', 'derived_cost_variance', 'derived_priority_bucket', 'derived_status_bucket', 'derived_work_center'],
    inspection: ['tag', 'inspection_type', 'work_type', 'plan_date', 'actual_date', 'result', 'derived_tag_compact', 'derived_inspection_delay_days', 'derived_is_overdue', 'derived_is_late_actual', 'derived_is_nonconformity', 'derived_work_type_bucket'],
    rkap_program: ['program_number', 'program_name', 'fiscal_year', 'total_equivalent_idr', 'cost_group', 'discipline', 'status_actual', 'status_prognosa', 'step_long_desc', 'top_risk', 'derived_total_equivalent_idr_num', 'derived_schedule_variance_days', 'derived_budget_bucket', 'derived_is_high_value', 'derived_is_top_risk', 'derived_is_delayed', 'derived_progress_stage_bucket'],
    equipment_issue: ['tag', 'status', 'report_date', 'mitigation', 'permanent_solution', 'irkap_mitigation', 'irkap_solution', 'derived_issue_age_days', 'derived_status_bucket', 'derived_has_mitigation', 'derived_has_permanent_solution', 'derived_has_irkap_reference', 'derived_actionability_score'],
    readiness_record: ['record_type', 'period', 'equipment_or_tag', 'refinery_unit', 'derived_readiness_tag_compact', 'derived_readiness_family', 'derived_record_month', 'derived_has_bad_status', 'derived_has_rtl', 'derived_has_external_resource', 'derived_action_category'],
    rcps: ['rcps_no', 'criticality', 'progress', 'refinery_unit', 'derived_progress_num', 'derived_criticality_bucket'],
    rcps_recommendation: ['rcps_no', 'pic', 'target', 'category', 'derived_target_date', 'derived_is_overdue', 'derived_owner_pic', 'derived_recommendation_category', 'derived_has_irkap'],
  }
  return byKind[kind] ?? common
}

type DepthDomainSummary = { key: string; label: string; relationshipCount: number; pathCount: number; schemaCount: number }

const depthDomainOrder = ['reliability', 'maintenance', 'readiness', 'cost_program', 'issue']
const depthDomainLabels: Record<string, string> = {
  reliability: 'Reliability',
  maintenance: 'Maintenance',
  readiness: 'Readiness',
  cost_program: 'RKAP',
  issue: 'Issue / RCPS',
}

// ─── Coverage Equipment ───────────────────────────────────────────────────────

const DOMAIN_LABELS: Record<string, string> = {
  reliability_observation: 'Reliability Observation',
  rkap_program:            'RKAP Program',
  icu_issue:               'ICU Issue',
  equipment_issue:         'Equipment Issue',
  readiness_record:        'Readiness Record',
  readiness_jetty:         'Readiness Jetty',
  readiness_spm:           'Readiness SPM',
  readiness_tank:          'Readiness Tank',
  bad_actor:               'Bad Actor',
  critical_equipment:      'Critical Equipment',
  metering:                'Metering',
  inspection_plan:         'Inspection Plan',
  ppms:                    'PPMS',
  monitoring_operasi:      'Monitoring Operasi',
  rotor:                   'Rotor',
  atg:                     'ATG',
  atg_program:             'ATG Program',
  zero_clamp:              'Zero Clamp',
  power_steam:             'Power Steam',
  paf_issue:               'PAF Issue',
  paf:                     'PAF',
  tkdn:                    'TKDN',
  oa_availability:         'OA Availability',
  plo_permit:              'PLO Permit',
  pipeline_inspection:     'Pipeline Inspection',
  rcps:                    'RCPS',
  work_order:              'Work Order',
  notification:            'Notification',
  jetty_workplan:          'Jetty Workplan',
  spm_workplan:            'SPM Workplan',
  tank_workplan:           'Tank Workplan',
}

function domainLabel(domain: string): string {
  return DOMAIN_LABELS[domain] ??
    domain.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase())
}

function coverageColor(pct: number) {
  if (pct >= 80) return 'cov-green'
  if (pct >= 50) return 'cov-yellow'
  return 'cov-red'
}

function EquipmentCoveragePage({ dataset }: { dataset?: DatasetSummary }) {
  const [data, setData] = useState<EquipmentCoverageDomain[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [sheet, setSheet] = useState<{ domain: string; ru: string } | null>(null)
  const [unmatched, setUnmatched] = useState<UnmatchedEquipment[]>([])
  const [unmatchedLoading, setUnmatchedLoading] = useState(false)
  const [filterRu, setFilterRu] = useState<string>('Semua')

  useEffect(() => {
    if (!dataset) return
    setLoading(true)
    setError('')
    api.equipmentCoverage(dataset.id)
      .then(setData)
      .catch(e => setError(message(e)))
      .finally(() => setLoading(false))
  }, [dataset])

  const openSheet = async (domain: string, ru: string) => {
    if (!dataset) return
    setSheet({ domain, ru })
    setUnmatchedLoading(true)
    setUnmatched([])
    try {
      const result = await api.equipmentCoverageUnmatched(dataset.id, domain, ru === 'Semua' ? '' : ru)
      setUnmatched(result)
    } catch {
      setUnmatched([])
    } finally {
      setUnmatchedLoading(false)
    }
  }

  const closeSheet = () => { setSheet(null); setUnmatched([]) }

  const exportUnmatched = () => {
    if (!dataset || unmatched.length === 0 || !sheet) return
    const header = 'equipment_raw,refinery_unit,jumlah'
    const csv = [header, ...unmatched.map(r => `"${r.equipment_raw_value}","${r.ru}",${r.jumlah}`)].join('\n')
    const a = Object.assign(document.createElement('a'), {
      href: URL.createObjectURL(new Blob([csv], { type: 'text/csv' })),
      download: `unmatched_${sheet.domain}_${sheet.ru || 'semua'}.csv`,
    })
    a.click()
    URL.revokeObjectURL(a.href)
  }

  if (!dataset) return <NoDataset />

  const allRus = Array.from(new Set(data.flatMap(d => d.rows.map(r => r.ru)))).sort()

  const COVERAGE_EXCLUDE = new Set(['notification', 'work_order', 'maintenance_notification', 'readiness_record'])
  const aggregated = data
    .filter(d => !COVERAGE_EXCLUDE.has(d.domain))
    .map(d => {
      const rows = filterRu === 'Semua' ? d.rows : d.rows.filter(r => r.ru === filterRu)
      const total = rows.reduce((s, r) => s + Number(r.total), 0)
      const matched = rows.reduce((s, r) => s + Number(r.matched), 0)
      const pct = total > 0 ? Math.round((matched / total) * 100) : 0
      return { domain: d.domain, total, matched, unmatched: total - matched, pct }
    }).filter(d => d.total > 0)

  const grandTotal = aggregated.reduce((s, d) => s + d.total, 0)
  const grandMatched = aggregated.reduce((s, d) => s + d.matched, 0)
  const grandPct = grandTotal > 0 ? Math.round((grandMatched / grandTotal) * 100) : 0

  return (
    <div className="coverage-page">
      {/* Hero summary */}
      <div className="coverage-hero">
        <div className={`coverage-hero-gauge ${coverageColor(grandPct)}`}>
          <strong>{grandPct}%</strong>
          <span>Coverage Total</span>
        </div>
        <div className="coverage-hero-stats">
          <div className="coverage-hero-bar-row">
            <div className="coverage-hero-bar">
              <div className={`cov-bar-fill ${coverageColor(grandPct)}`} style={{ width: `${grandPct}%` }} />
            </div>
            <span className={`cov-pct ${coverageColor(grandPct)}`}>{grandPct}%</span>
          </div>
          <div className="coverage-hero-counts">
            <div className="coverage-hero-chip">
              <b style={{ color: '#16a34a' }}>{format(grandMatched)}</b>
              <span>Sama dengan master data</span>
            </div>
            <div className="coverage-hero-chip">
              <b style={{ color: '#dc2626' }}>{format(grandTotal - grandMatched)}</b>
              <span>Berbeda / tidak ditemukan</span>
            </div>
            <div className="coverage-hero-chip">
              <b>{format(grandTotal)}</b>
              <span>Total baris laporan non-SAP</span>
            </div>
          </div>
        </div>
        <div className="coverage-hero-filter">
          <label>Filter RU</label>
          <select value={filterRu} onChange={e => setFilterRu(e.target.value)}>
            <option value="Semua">Semua RU</option>
            {allRus.map(ru => <option key={ru} value={ru}>{ru}</option>)}
          </select>
        </div>
      </div>

      {loading && <div className="coverage-loading">Memuat data coverage…</div>}
      {error && <div className="coverage-error">{error}</div>}

      {!loading && !error && (
        <div className="coverage-cards">
          {aggregated.map(d => (
            <div key={d.domain} className="coverage-card">
              <div className="coverage-card-header">
                <span className="coverage-card-title">{domainLabel(d.domain)}</span>
                <span className={`coverage-card-badge ${coverageColor(d.pct)}`}>{d.pct}%</span>
              </div>
              <div className="coverage-card-bar-row">
                <div className="cov-bar">
                  <div className={`cov-bar-fill ${coverageColor(d.pct)}`} style={{ width: `${d.pct}%` }} />
                </div>
              </div>
              <div className="coverage-card-footer">
                <div className="coverage-card-nums">
                  <span><span className="c-matched">{format(d.matched)}</span> sama</span>
                  <span><span className="c-unmatched">{format(d.unmatched)}</span> tidak sama</span>
                  <span style={{ color: 'var(--muted)' }}>{format(d.total)} total</span>
                </div>
                {d.unmatched > 0 && (
                  <button className="coverage-btn-see" onClick={() => openSheet(d.domain, filterRu)}>
                    Lihat tidak cocok
                  </button>
                )}
              </div>
            </div>
          ))}
        </div>
      )}

      {/* Modal */}
      {sheet && (
        <div className="cov-modal-overlay" onClick={e => { if (e.target === e.currentTarget) closeSheet() }}>
          <div className="cov-modal">
            <div className="cov-modal-header">
              <div>
                <div className="cov-modal-title">
                  {domainLabel(sheet.domain)}
                  {sheet.ru && sheet.ru !== 'Semua' && <span style={{ color: 'var(--muted)', fontWeight: 400 }}> · {sheet.ru}</span>}
                </div>
                <div className="cov-modal-sub">Penulisan equipment di laporan yang berbeda dari master data</div>
              </div>
              <div className="cov-modal-actions">
                {unmatched.length > 0 && (
                  <button className="secondary small" onClick={exportUnmatched}>Export CSV</button>
                )}
                <button className="cov-modal-close" onClick={closeSheet} title="Tutup">✕</button>
              </div>
            </div>
            <div className="cov-modal-body">
              {unmatchedLoading && <div className="coverage-loading">Memuat data…</div>}
              {!unmatchedLoading && unmatched.length === 0 && (
                <div className="coverage-loading">Tidak ada data untuk filter ini.</div>
              )}
              {!unmatchedLoading && unmatched.length > 0 && (
                <Paged items={unmatched} pageSize={20}>
                  {rows => (
                    <table style={{ width: '100%', borderCollapse: 'collapse' }}>
                      <thead>
                        <tr>
                          <th style={{ width: 40, textAlign: 'right', padding: '10px 16px', fontSize: '0.7rem', letterSpacing: '.06em', textTransform: 'uppercase', color: 'var(--muted)', fontWeight: 600, background: 'var(--panel-2)', borderBottom: '1px solid var(--line)' }}>#</th>
                          <th style={{ padding: '10px 16px', fontSize: '0.7rem', letterSpacing: '.06em', textTransform: 'uppercase', color: 'var(--muted)', fontWeight: 600, background: 'var(--panel-2)', borderBottom: '1px solid var(--line)' }}>Penulisan di Laporan</th>
                          <th style={{ padding: '10px 16px', fontSize: '0.7rem', letterSpacing: '.06em', textTransform: 'uppercase', color: 'var(--muted)', fontWeight: 600, background: 'var(--panel-2)', borderBottom: '1px solid var(--line)' }}>Penulisan di Master Data</th>
                          <th style={{ padding: '10px 16px', fontSize: '0.7rem', letterSpacing: '.06em', textTransform: 'uppercase', color: 'var(--muted)', fontWeight: 600, background: 'var(--panel-2)', borderBottom: '1px solid var(--line)', whiteSpace: 'nowrap' }}>Refinery Unit</th>
                          <th style={{ padding: '10px 16px', fontSize: '0.7rem', letterSpacing: '.06em', textTransform: 'uppercase', color: 'var(--muted)', fontWeight: 600, background: 'var(--panel-2)', borderBottom: '1px solid var(--line)', textAlign: 'right', whiteSpace: 'nowrap' }}>Frekuensi</th>
                        </tr>
                      </thead>
                      <tbody>
                        {rows.map((r, i) => (
                          <tr key={i} style={{ borderBottom: '1px solid var(--line)' }}>
                            <td style={{ padding: '9px 16px', fontSize: '0.72rem', color: 'var(--muted)', textAlign: 'right', width: 40 }}>{i + 1}</td>
                            <td style={{ padding: '9px 16px' }}>
                              <span className="cov-raw-val">
                                {r.equipment_raw_value || <em style={{ color: 'var(--muted)' }}>kosong</em>}
                              </span>
                            </td>
                            <td style={{ padding: '9px 16px' }}>
                              {r.closest_key
                                ? <div className="cov-match-found">
                                    <span className="cov-match-key">{r.closest_key}</span>
                                    {r.closest_label && r.closest_label !== r.closest_key && <span className="cov-match-label">{r.closest_label}</span>}
                                  </div>
                                : <span className="cov-no-match">Tidak ditemukan</span>}
                            </td>
                            <td style={{ padding: '9px 16px', fontSize: '0.825rem', whiteSpace: 'nowrap' }}>{r.ru}</td>
                            <td style={{ padding: '9px 16px', textAlign: 'right' }}><span className="cov-num">{format(r.jumlah)}</span></td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  )}
                </Paged>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

// ─── Rantai Relasi ────────────────────────────────────────────────────────────

type ChainStep = { nodeType: string; label: string; relType?: string }
type Chain = { id: string; title: string; description: string; color: string; steps: ChainStep[] }

const CHAINS: Chain[] = [
  {
    id: 'reliability',
    title: 'Jalur Keandalan',
    description: 'Equipment → Observasi keandalan → ICU Issue → RKAP Program',
    color: '#3b82f6',
    steps: [
      { nodeType: 'equipment', label: 'Equipment' },
      { nodeType: 'reliability_observation', label: 'Reliability Observation', relType: 'EQUIPMENT_HAS_RELIABILITY_OBSERVATION' },
      { nodeType: 'icu_issue', label: 'ICU Issue', relType: 'EQUIPMENT_HAS_ICU_ISSUE' },
      { nodeType: 'rkap_program', label: 'RKAP Program', relType: 'EQUIPMENT_HAS_RKAP_PROGRAM' },
    ],
  },
  {
    id: 'critical-bad-actor',
    title: 'Jalur Bad Actor',
    description: 'Equipment → Critical Equipment → Bad Actor → RCPS → Rekomendasi',
    color: '#ef4444',
    steps: [
      { nodeType: 'equipment', label: 'Equipment' },
      { nodeType: 'critical_equipment', label: 'Critical Equipment', relType: 'EQUIPMENT_HAS_CRITICAL_EQUIPMENT' },
      { nodeType: 'bad_actor', label: 'Bad Actor', relType: 'CRITICAL_EQUIPMENT_HAS_BAD_ACTOR' },
    ],
  },
  {
    id: 'zero-clamp',
    title: 'Jalur Zero Clamp & Inspeksi',
    description: 'Equipment → Zero Clamp → Inspection → Pipeline Inspection',
    color: '#f59e0b',
    steps: [
      { nodeType: 'equipment', label: 'Equipment' },
      { nodeType: 'zero_clamp', label: 'Zero Clamp', relType: 'EQUIPMENT_HAS_ZERO_CLAMP' },
      { nodeType: 'inspection', label: 'Inspection', relType: 'ZERO_CLAMP_HAS_INSPECTION' },
      { nodeType: 'pipeline_inspection', label: 'Pipeline Inspection', relType: 'ZERO_CLAMP_HAS_PIPELINE_INSPECTION' },
    ],
  },
  {
    id: 'monitoring',
    title: 'Jalur Monitoring Operasi',
    description: 'Equipment → Power Steam → Monitoring Operasi',
    color: '#10b981',
    steps: [
      { nodeType: 'equipment', label: 'Equipment' },
      { nodeType: 'power_steam', label: 'Power Steam', relType: 'EQUIPMENT_HAS_POWER_STEAM' },
      { nodeType: 'monitoring_operasi', label: 'Monitoring Operasi', relType: 'POWER_STEAM_HAS_MONITORING_OPERASI' },
    ],
  },
  {
    id: 'maintenance',
    title: 'Jalur Pemeliharaan',
    description: 'Equipment → Work Order / Notification → RKAP Program',
    color: '#8b5cf6',
    steps: [
      { nodeType: 'equipment', label: 'Equipment' },
      { nodeType: 'work_order', label: 'Work Order', relType: 'EQUIPMENT_HAS_WORK_ORDER' },
      { nodeType: 'notification', label: 'Notification', relType: 'EQUIPMENT_HAS_NOTIFICATION' },
    ],
  },
  {
    id: 'readiness',
    title: 'Jalur Kesiapan Aset',
    description: 'Equipment → Readiness Record / Rotor / ATG',
    color: '#06b6d4',
    steps: [
      { nodeType: 'equipment', label: 'Equipment' },
      { nodeType: 'readiness_record', label: 'Readiness Record', relType: 'EQUIPMENT_HAS_READINESS_RECORD' },
      { nodeType: 'rotor', label: 'Rotor', relType: 'EQUIPMENT_HAS_ROTOR' },
      { nodeType: 'atg', label: 'ATG', relType: 'EQUIPMENT_HAS_ATG' },
    ],
  },
  {
    id: 'infrastruktur',
    title: 'Jalur Kesiapan Infrastruktur',
    description: 'Readiness Jetty / SPM / Tangki → RTL Workplan',
    color: '#0ea5e9',
    steps: [
      { nodeType: 'readiness_jetty', label: 'Readiness Jetty' },
      { nodeType: 'readiness_spm', label: 'Readiness SPM' },
      { nodeType: 'readiness_tank', label: 'Readiness Tank' },
      { nodeType: 'jetty_workplan', label: 'Jetty Workplan', relType: 'READINESS_JETTY_HAS_WORKPLAN' },
      { nodeType: 'spm_workplan', label: 'SPM Workplan', relType: 'READINESS_SPM_HAS_WORKPLAN' },
      { nodeType: 'tank_workplan', label: 'Tank Workplan', relType: 'READINESS_TANK_HAS_WORKPLAN' },
    ],
  },
  {
    id: 'perizinan',
    title: 'Jalur Perizinan & Inspeksi',
    description: 'PLO Permit → Pipeline Inspection → TKDN / OA Availability',
    color: '#d946ef',
    steps: [
      { nodeType: 'plo_permit', label: 'PLO Permit' },
      { nodeType: 'pipeline_inspection', label: 'Pipeline Inspection' },
      { nodeType: 'tkdn', label: 'TKDN' },
      { nodeType: 'oa_availability', label: 'OA Availability' },
    ],
  },
]

function ChainExplorer({ dataset }: { dataset?: DatasetSummary }) {
  const [selectedChain, setSelectedChain] = useState<Chain | null>(null)
  const [selectedStep, setSelectedStep] = useState<ChainStep | null>(null)
  const [graph, setGraph] = useState<GraphSlice>(emptyGraph)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [selectedNode, setSelectedNode] = useState<GraphNode | null>(null)
  const [eqQuery, setEqQuery] = useState('')
  const [eqResults, setEqResults] = useState<GraphNode[]>([])
  const [eqSearching, setEqSearching] = useState(false)
  const [pickedEq, setPickedEq] = useState<GraphNode | null>(null)
  // Cache: chain.id → equipment node yang sudah ditemukan, hindari re-search
  const chainEqCache = useRef<Map<string, GraphNode>>(new Map())

  if (!dataset) return <NoDataset />

  const searchEquipment = async (q: string) => {
    setEqQuery(q)
    if (q.length < 2) { setEqResults([]); return }
    setEqSearching(true)
    try {
      const res = await api.search(dataset.id, q, 'equipment', '', 10)
      setEqResults(res)
    } finally {
      setEqSearching(false)
    }
  }

  // Fetch targeted: untuk tiap step dengan relType, ambil neighbors dari rootId
  // filtered hanya ke node_type step tersebut (via hasil neighbors)
  const loadGraph = async (chain: Chain, rootNode: GraphNode) => {
    setLoading(true)
    setError('')
    setSelectedNode(null)
    try {
      const allNodes = new Map<string, GraphNode>()
      const allEdges = new Map<string, GraphEdge>()
      allNodes.set(rootNode.id, rootNode)

      const MAX_PER_TYPE = 5 // beberapa node per domain sudah cukup untuk lihat end-to-end jalur

      // Tiap step: query neighbors dari frontier dengan filter relationship_type spesifik
      // → backend hanya kembalikan edges dengan rel_type itu, tidak kena limit gabungan
      let frontier: string[] = [rootNode.id]

      for (const step of chain.steps.slice(1)) {
        if (!step.relType) continue
        const nextFrontier: string[] = []

        for (const nodeId of frontier.slice(0, 3)) {
          const slice = await api.neighbors(dataset.id, nodeId, {
            depth: 1,
            includeCandidates: false,
            minConfidence: 0,
            relationshipType: step.relType, // filter ke rel_type spesifik ini saja
            limit: MAX_PER_TYPE * 2,
          })
          const matchNodes = slice.nodes.filter(n => n.kind === step.nodeType).slice(0, MAX_PER_TYPE)
          matchNodes.forEach(n => { allNodes.set(n.id, n); nextFrontier.push(n.id) })
          slice.edges.forEach(e => allEdges.set(e.id, e))
          if (nextFrontier.length >= MAX_PER_TYPE) break
        }

        if (nextFrontier.length > 0) frontier = nextFrontier
      }

      if (allNodes.size <= 1) {
        setGraph(emptyGraph)
        setError('Tidak ada relasi ditemukan untuk equipment ini di jalur ini. Pastikan relasi sudah direbuild.')
        return
      }

      setGraph({ nodes: [...allNodes.values()], edges: [...allEdges.values()], truncated: false })
    } catch (e) {
      setError(message(e))
    } finally {
      setLoading(false)
    }
  }

  const autoFindAndLoad = async (chain: Chain) => {
    // Pakai cache kalau sudah pernah ketemu
    const cached = chainEqCache.current.get(chain.id)
    if (cached) {
      setPickedEq(cached)
      setEqQuery(cached.label)
      void loadGraph(chain, cached)
      return
    }
    setLoading(true)
    setError('')
    setSelectedNode(null)
    setPickedEq(null)
    setEqQuery('')
    try {
      const rootStep = chain.steps[0]
      const firstRelStep = chain.steps.find(s => s.relType)
      // Ambil banyak kandidat untuk dicari yang benar-benar punya koneksi ke jalur
      const candidates = await api.search(dataset.id, '', rootStep.nodeType, '', 100)
      if (candidates.length === 0) {
        setGraph(emptyGraph)
        setLoading(false)
        setError('Tidak ada equipment ditemukan.')
        return
      }
      if (!firstRelStep) {
        // Tidak ada rel_type di jalur ini, pakai equipment pertama saja
        const eq = candidates[0]
        chainEqCache.current.set(chain.id, eq)
        setPickedEq(eq); setEqQuery(eq.label)
        await loadGraph(chain, eq)
        return
      }
      // Cek paralel 10 equipment sekaligus, tapi kali ini pakai filter relationshipType
      // agar tidak kena limit dari ribuan direct-neighbors lain
      const BATCH = 10
      for (let i = 0; i < candidates.length; i += BATCH) {
        const batch = candidates.slice(i, i + BATCH)
        const checks = await Promise.all(
          batch.map(eq =>
            api.neighbors(dataset.id, eq.id, {
              depth: 1, includeCandidates: false, minConfidence: 0,
              relationshipType: firstRelStep.relType, limit: 5,
            })
            .then(s => ({ eq, hasRel: s.nodes.length > 0 }))
            .catch(() => ({ eq, hasRel: false }))
          )
        )
        const found = checks.find(c => c.hasRel)
        if (found) {
          chainEqCache.current.set(chain.id, found.eq)
          setPickedEq(found.eq)
          setEqQuery(found.eq.label)
          await loadGraph(chain, found.eq)
          return
        }
      }
      setGraph(emptyGraph)
      setLoading(false)
      setError(`Tidak ditemukan equipment yang terhubung ke "${firstRelStep.label}" di dataset ini. Pastikan data sudah diupload dan relasi sudah direbuild.`)
    } catch (e) {
      setError(message(e))
      setLoading(false)
    }
  }

  const handleChainClick = (chain: Chain) => {
    setSelectedChain(chain)
    setSelectedStep(chain.steps[0])
    void autoFindAndLoad(chain)
  }

  const handlePickEq = (eq: GraphNode) => {
    if (selectedChain) chainEqCache.current.set(selectedChain.id, eq)
    setPickedEq(eq)
    setEqQuery(eq.label)
    setEqResults([])
    if (selectedChain) void loadGraph(selectedChain, eq)
  }

  return (
    <div className="chain-explorer">
      <div className="chain-list">
        {CHAINS.map(chain => (
          <div
            key={chain.id}
            className={`chain-card ${selectedChain?.id === chain.id ? 'active' : ''}`}
            style={{ '--chain-color': chain.color } as React.CSSProperties}
            onClick={() => handleChainClick(chain)}
          >
            <div className="chain-card-title">{chain.title}</div>
            <div className="chain-card-desc">{chain.description}</div>
            <div className="chain-steps-row">
              {chain.steps.map((step, i) => (
                <span key={step.nodeType} className="chain-step-pill">
                  {i > 0 && <span className="chain-arrow">→</span>}
                  <span className="chain-pill">{step.label}</span>
                </span>
              ))}
            </div>
          </div>
        ))}
      </div>

      <div className="chain-graph-panel">
        {!selectedChain && (
          <div className="chain-empty">
            <ChainIcon style={{ width: 48, height: 48, opacity: 0.3 }} />
            <p>Pilih rantai relasi di kiri untuk melihat knowledge graph-nya</p>
          </div>
        )}
        {selectedChain && (
          <>
            <div className="chain-graph-header">
              <div>
                <strong>{selectedChain.title}</strong>
              </div>
              <div className="chain-steps-nav">
                {selectedChain.steps.map((step, i) => (
                  <span key={step.nodeType} className="chain-step-legend">
                    {i > 0 && <span className="chain-arrow">→</span>}
                    <span className="chain-pill selected" style={{ '--chain-color': selectedChain.color } as React.CSSProperties}>
                      {step.label}
                    </span>
                  </span>
                ))}
              </div>
            </div>

            {/* Search equipment */}
            <div className="chain-eq-search">
              <div style={{ position: 'relative' }}>
                <input
                  className="search-input"
                  placeholder="Cari equipment (min. 2 karakter)…"
                  value={eqQuery}
                  onChange={e => void searchEquipment(e.target.value)}
                  style={{ width: '100%' }}
                />
                {eqSearching && <span style={{ position: 'absolute', right: 8, top: 8, fontSize: 12, opacity: 0.5 }}>…</span>}
                {eqResults.length > 0 && (
                  <div className="search-dropdown">
                    {eqResults.map(eq => (
                      <div key={eq.id} className="search-dropdown-item" onClick={() => handlePickEq(eq)}>
                        <strong>{eq.label}</strong>
                        <span style={{ marginLeft: 8, opacity: 0.5, fontSize: 12 }}>{eq.subtitle ?? eq.id}</span>
                      </div>
                    ))}
                  </div>
                )}
              </div>
              {pickedEq && <div className="chain-picked-eq">Equipment: <strong>{pickedEq.label}</strong></div>}
              {!pickedEq && <div style={{ fontSize: 12, opacity: 0.5, marginTop: 4 }}>Pilih equipment untuk memuat graph jalur ini</div>}
            </div>

            {loading && <div className="chain-loading">Memuat graph…</div>}
            {error && <div className="chain-error">{error}</div>}
            {!loading && !error && graph.nodes.length > 0 && (
              <div className="chain-graph-wrap">
                <GraphView
                  graph={graph}
                  rootId={pickedEq?.id ?? graph.nodes[0]?.id ?? ''}
                  selectedId={selectedNode?.id}
                  onSelect={setSelectedNode}
                  onSelectEdge={() => {}}
                />
                {selectedNode && (
                  <div className="chain-node-detail">
                    <div className="chain-node-type">{selectedNode.kind}</div>
                    <div className="chain-node-label">{selectedNode.label}</div>
                    <div className="chain-node-id">{selectedNode.id}</div>
                    {selectedNode.properties && Object.entries(selectedNode.properties).slice(0, 8).map(([k, v]) => (
                      <div key={k} className="chain-node-prop"><span>{k}</span><span>{String(v)}</span></div>
                    ))}
                  </div>
                )}
              </div>
            )}
            {!loading && !error && graph.nodes.length === 0 && pickedEq && (
              <div className="chain-empty"><p>Tidak ada relasi ditemukan untuk equipment ini. Coba rebuild relasi terlebih dahulu.</p></div>
            )}
          </>
        )}
      </div>
    </div>
  )
}

// ─── Analisis AI ─────────────────────────────────────────────────────────────

const FOCUS_OPTIONS = [
  { value: 'general',     label: 'Analisis Menyeluruh' },
  { value: 'reliability', label: 'Keandalan & Reliability' },
  { value: 'readiness',   label: 'Kesiapan Operasi' },
  { value: 'risk',        label: 'Manajemen Risiko' },
  { value: 'coverage',    label: 'Kualitas Data' },
]

function renderMarkdown(text: string) {
  const lines = text.split('\n')
  const elements: JSX.Element[] = []
  let key = 0
  for (const line of lines) {
    if (line.startsWith('### ')) {
      elements.push(<h3 key={key++} className="ai-h3">{line.slice(4)}</h3>)
    } else if (line.startsWith('## ')) {
      elements.push(<h2 key={key++} className="ai-h2">{line.slice(3)}</h2>)
    } else if (line.startsWith('# ')) {
      elements.push(<h1 key={key++} className="ai-h1">{line.slice(2)}</h1>)
    } else if (/^[-*] /.test(line)) {
      elements.push(<li key={key++} className="ai-li">{line.slice(2)}</li>)
    } else if (/^\d+\. /.test(line)) {
      elements.push(<li key={key++} className="ai-li ai-li-num">{line.replace(/^\d+\. /, '')}</li>)
    } else if (line.trim() === '') {
      elements.push(<div key={key++} className="ai-gap" />)
    } else {
      const parts = line.split(/(\*\*[^*]+\*\*)/g)
      elements.push(
        <p key={key++} className="ai-p">
          {parts.map((part, i) =>
            part.startsWith('**') && part.endsWith('**')
              ? <strong key={i}>{part.slice(2, -2)}</strong>
              : part
          )}
        </p>
      )
    }
  }
  return elements
}

type SavedAnalysis = { id: number; scope: string; focus: string; ru: string; equipment_id: string; title: string; created_at: string }

function AnalisisPage({ dataset }: { dataset?: DatasetSummary }) {
  const [scope, setScope] = useState<'dataset' | 'ru' | 'equipment'>('dataset')
  const [selectedRu, setSelectedRu] = useState('')
  const [focus, setFocus] = useState('general')
  const [equipmentSearch, setEquipmentSearch] = useState('')
  const [equipmentResults, setEquipmentResults] = useState<GraphNode[]>([])
  const [selectedEquipment, setSelectedEquipment] = useState<GraphNode | null>(null)
  const [ruList, setRuList] = useState<string[]>([])
  const [generating, setGenerating] = useState(false)
  const [result, setResult] = useState('')
  const [error, setError] = useState('')
  const [saving, setSaving] = useState(false)
  const [savedOk, setSavedOk] = useState(false)
  const [history, setHistory] = useState<SavedAnalysis[]>([])
  const [historyOpen, setHistoryOpen] = useState(false)
  const abortRef = useRef<AbortController | null>(null)

  const loadHistory = () => {
    if (!dataset) return
    api.listSavedAnalyses(dataset.id).then(setHistory).catch(() => {})
  }

  useEffect(() => { loadHistory() }, [dataset])

  useEffect(() => {
    if (!dataset) return
    api.ruSummary(dataset.id).then(data => {
      const rus = (data.refinery_units as Array<{ refinery_unit: string }>)
        .map(r => r.refinery_unit).filter(Boolean).sort()
      setRuList(rus)
    }).catch(() => {})
  }, [dataset])

  useEffect(() => {
    if (scope !== 'equipment' || !dataset || equipmentSearch.length < 2) {
      setEquipmentResults([])
      return
    }
    const t = setTimeout(() => {
      api.search(dataset.id, equipmentSearch, 'equipment', '', 20)
        .then(setEquipmentResults).catch(() => {})
    }, 300)
    return () => clearTimeout(t)
  }, [equipmentSearch, scope, dataset])

  const generate = async () => {
    if (!dataset) return
    if (scope === 'ru' && !selectedRu) { setError('Pilih Refinery Unit terlebih dahulu.'); return }
    if (scope === 'equipment' && !selectedEquipment) { setError('Pilih equipment terlebih dahulu.'); return }
    setError('')
    setResult('')
    setGenerating(true)
    abortRef.current = new AbortController()
    try {
      await streamAnalysis(
        dataset.id, scope,
        selectedRu,
        selectedEquipment?.id ?? '',
        focus,
        (chunk) => setResult(prev => prev + chunk),
        abortRef.current.signal,
      )
    } catch (e) {
      if ((e as Error).name !== 'AbortError') setError(message(e))
    } finally {
      setGenerating(false)
    }
  }

  const stop = () => { abortRef.current?.abort(); setGenerating(false) }

  const saveResult = async () => {
    if (!dataset || !result) return
    setSaving(true)
    const focusLabel = FOCUS_OPTIONS.find(f => f.value === focus)?.label ?? focus
    const scopeLabel = scope === 'dataset' ? 'Dataset' : scope === 'ru' ? selectedRu : (selectedEquipment?.label ?? '')
    const title = `${scopeLabel} · ${focusLabel}`
    try {
      await api.saveAnalysis(dataset.id, { scope, focus, ru: selectedRu, equipment_id: selectedEquipment?.id ?? '', title, content: result })
      setSavedOk(true)
      setTimeout(() => setSavedOk(false), 2500)
      loadHistory()
    } catch { /* ignore */ } finally {
      setSaving(false)
    }
  }

  const loadSaved = async (id: number) => {
    if (!dataset) return
    try {
      const saved = await api.getSavedAnalysis(dataset.id, id)
      setResult(saved.content)
      setScope(saved.scope as 'dataset' | 'ru' | 'equipment')
      setFocus(saved.focus)
      setSelectedRu(saved.ru ?? '')
      setHistoryOpen(false)
    } catch { /* ignore */ }
  }

  const deleteSaved = async (id: number) => {
    if (!dataset) return
    try {
      await api.deleteSavedAnalysis(dataset.id, id)
      setHistory(h => h.filter(a => a.id !== id))
    } catch { /* ignore */ }
  }

  if (!dataset) return <NoDataset />

  const scopeReady = scope === 'dataset' || (scope === 'ru' && !!selectedRu) || (scope === 'equipment' && !!selectedEquipment)

  return (
    <div className="analisis-page">
      {/* Config panel */}
      <div className="analisis-config">
        <div className="analisis-config-title">
          <SparkleIcon style={{ width: 18, height: 18 }} />
          Konfigurasi Analisis
        </div>

        {/* Scope tabs */}
        <div className="analisis-field">
          <label className="analisis-label">Cakupan Analisis</label>
          <div className="analisis-scope-tabs">
            {(['dataset', 'ru', 'equipment'] as const).map(s => (
              <button
                key={s}
                className={`analisis-scope-tab ${scope === s ? 'active' : ''}`}
                onClick={() => { setScope(s); setResult(''); setError('') }}
              >
                {s === 'dataset' ? 'Seluruh Dataset' : s === 'ru' ? 'Per Refinery Unit' : 'Per Equipment'}
              </button>
            ))}
          </div>
        </div>

        {/* RU selector */}
        {scope === 'ru' && (
          <div className="analisis-field">
            <label className="analisis-label">Refinery Unit</label>
            <select className="analisis-select" value={selectedRu} onChange={e => setSelectedRu(e.target.value)}>
              <option value="">— Pilih RU —</option>
              {ruList.map(ru => <option key={ru} value={ru}>{ru}</option>)}
            </select>
          </div>
        )}

        {/* Equipment selector */}
        {scope === 'equipment' && (
          <div className="analisis-field">
            <label className="analisis-label">Equipment</label>
            {selectedEquipment
              ? <div className="analisis-eq-selected">
                  <span><strong>{selectedEquipment.label}</strong><span style={{ color: 'var(--muted)', marginLeft: 8 }}>{selectedEquipment.id}</span></span>
                  <button className="link-button" onClick={() => { setSelectedEquipment(null); setEquipmentSearch('') }}>Ganti</button>
                </div>
              : <>
                  <input
                    className="analisis-input"
                    placeholder="Cari nama atau kode equipment…"
                    value={equipmentSearch}
                    onChange={e => setEquipmentSearch(e.target.value)}
                  />
                  {equipmentResults.length > 0 && (
                    <div className="analisis-eq-dropdown">
                      {equipmentResults.map(eq => (
                        <button key={eq.id} className="analisis-eq-option" onClick={() => { setSelectedEquipment(eq); setEquipmentResults([]) }}>
                          <span className="analisis-eq-label">{eq.label}</span>
                          <span className="analisis-eq-id">{eq.id}</span>
                        </button>
                      ))}
                    </div>
                  )}
                </>
            }
          </div>
        )}

        {/* Focus */}
        <div className="analisis-field">
          <label className="analisis-label">Fokus Analisis</label>
          <div className="analisis-focus-grid">
            {FOCUS_OPTIONS.map(f => (
              <button
                key={f.value}
                className={`analisis-focus-btn ${focus === f.value ? 'active' : ''}`}
                onClick={() => setFocus(f.value)}
              >
                {f.label}
              </button>
            ))}
          </div>
        </div>

        {/* Actions */}
        <div className="analisis-actions">
          {generating
            ? <button className="secondary" onClick={stop}>⏹ Stop</button>
            : <button className="primary" onClick={generate} disabled={!scopeReady}>
                <SparkleIcon style={{ width: 14, height: 14 }} /> Generate Analisis
              </button>
          }
          <button className="secondary" onClick={() => { setHistoryOpen(o => !o); loadHistory() }} style={{ position: 'relative' }}>
            Riwayat {history.length > 0 && <b style={{ marginLeft: 4 }}>{history.length}</b>}
          </button>
        </div>

        {historyOpen && (
          <div className="analisis-history">
            {history.length === 0
              ? <p className="analisis-history-empty">Belum ada analisis tersimpan.</p>
              : history.map(a => (
                <div key={a.id} className="analisis-history-item">
                  <button className="analisis-history-load" onClick={() => void loadSaved(a.id)}>
                    <span className="analisis-history-title">{a.title}</span>
                    <span className="analisis-history-date">{new Date(a.created_at).toLocaleString('id-ID', { dateStyle: 'short', timeStyle: 'short' })}</span>
                  </button>
                  <button className="analisis-history-del" title="Hapus" onClick={() => void deleteSaved(a.id)}>×</button>
                </div>
              ))
            }
          </div>
        )}

        {error && <div className="analisis-error">{error}</div>}
      </div>

      {/* Result panel */}
      <div className="analisis-result-wrap">
        {!result && !generating && (
          <div className="analisis-empty">
            <SparkleIcon style={{ width: 48, height: 48, opacity: 0.2 }} />
            <p>Pilih cakupan dan fokus analisis, lalu klik <strong>Generate Analisis</strong>.</p>
            <p style={{ fontSize: 'var(--fs-xs)', marginTop: 4 }}>AI akan menganalisis data knowledge graph dan menghasilkan narasi mendalam dalam Bahasa Indonesia.</p>
          </div>
        )}
        {(result || generating) && (
          <div className="analisis-result">
            <div className="analisis-result-header">
              <span className="analisis-result-title">
                {scope === 'dataset' ? 'Analisis Dataset' : scope === 'ru' ? `Analisis ${selectedRu}` : `Analisis ${selectedEquipment?.label ?? ''}`}
                {' · '}{FOCUS_OPTIONS.find(f => f.value === focus)?.label}
              </span>
              {result && !generating && (
                <div style={{ display: 'flex', gap: 8 }}>
                  <button className="secondary small" onClick={() => void saveResult()} disabled={saving}>
                    {savedOk ? '✓ Tersimpan' : saving ? 'Menyimpan…' : 'Simpan'}
                  </button>
                  <button className="secondary small" onClick={() => navigator.clipboard.writeText(result)}>Salin Teks</button>
                </div>
              )}
            </div>
            <div className="analisis-result-body">
              {renderMarkdown(result)}
              {generating && <span className="analisis-cursor">▌</span>}
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

function titleFor(page: Page) {
  return ({ overview: 'Operational overview', import: 'Import center', executive: 'Executive RU', insight: 'Reliability insight', equipment: 'Equipment 360', graph: 'Graph explorer', depth: 'Depth explorer', review: 'Data review', datasets: 'Daftar dataset', chains: 'Rantai Relasi', coverage: 'Coverage Equipment', analisis: 'Analisis AI' })[page]
}
function message(reason: unknown) {
  if (reason instanceof Error) return reason.message
  if (reason && typeof reason === 'object' && 'message' in reason) return String((reason as {message: unknown}).message)
  if (typeof reason === 'string') return reason
  return 'Terjadi kesalahan tidak dikenal.'
}
function format(value: number) { return new Intl.NumberFormat('id-ID').format(value || 0) }
function compact(value: number) { return new Intl.NumberFormat('en', { notation: 'compact' }).format(value) }
function bytes(value: number) { if (!value) return '0 B'; const units = ['B', 'KB', 'MB', 'GB']; const i = Math.min(Math.floor(Math.log(value) / Math.log(1024)), 3); return `${(value / 1024 ** i).toFixed(i ? 1 : 0)} ${units[i]}` }
function human(value: string) { return value.replaceAll('_', ' ').replace(/\b\w/g, (letter) => letter.toUpperCase()) }
function slug(value: string) { return value.toLowerCase().replace(/[^a-z0-9]+/g, '-') }
function sum(rows: Record<string, unknown>[], key: string) { return rows.reduce((total, row) => total + Number(row[key] ?? 0), 0) }
function decimal(value: unknown) { const n = Number(value); return Number.isFinite(n) ? new Intl.NumberFormat('id-ID', { maximumFractionDigits: 1 }).format(n) : '—' }
function shortText(value: string, max: number) { return value.length > max ? `${value.slice(0, max - 1)}…` : value }
function countBy(values: string[]) {
  return values.reduce<Record<string, number>>((result, value) => {
    result[value || 'unknown'] = (result[value || 'unknown'] ?? 0) + 1
    return result
  }, {})
}
function topEntries(values: Record<string, number>, limit: number) {
  return Object.entries(values).sort((a, b) => b[1] - a[1]).slice(0, limit)
}
function countVisibleTagAssociatedReadiness(graph: GraphSlice) {
  const equipmentByRu = new Map<string, Set<string>>()
  graph.nodes.filter((node) => node.kind === 'equipment').forEach((node) => {
    const ru = ruKey(node.refinery_unit ?? node.properties.refinery_unit ?? node.properties.ru)
    if (!ru) return
    const tokens = [
      node.label,
      node.subtitle,
      node.equipment_code_normalized,
      node.properties.business_key,
      node.properties.equipment_code_normalized,
      node.properties.equipment_id,
      node.properties.tag,
      node.properties.tag_no,
      node.properties.tag_number,
    ].map(compactToken).filter((token) => token.length >= 4)
    if (!equipmentByRu.has(ru)) equipmentByRu.set(ru, new Set())
    const set = equipmentByRu.get(ru)!
    tokens.forEach((token) => {
      set.add(token)
      for (let size = 4; size <= token.length; size += 1) set.add(token.slice(0, size))
    })
  })
  return graph.nodes.filter((node) => {
    if (node.kind !== 'readiness_record') return false
    const ru = ruKey(node.refinery_unit ?? node.properties.refinery_unit ?? node.properties.ru)
    const equipmentTokens = equipmentByRu.get(ru)
    if (!equipmentTokens?.size) return false
    const readinessTokens = [
      node.label,
      node.properties.derived_readiness_tag_compact,
      node.properties.equipment_or_tag,
      node.properties.tag_no,
      node.properties.tag_number,
      node.properties.process_equipment,
      node.properties.equipment,
    ].map(compactToken).filter((token) => token.length >= 4)
    return readinessTokens.some((token) => equipmentTokens.has(token))
  }).length
}
function compactToken(value: unknown) {
  const text = String(value ?? '').toUpperCase()
  const tail = text.includes('|') ? text.split('|').pop() ?? text : text
  return tail.replace(/[^A-Z0-9]/g, '')
}
function ruKey(value: unknown) {
  const match = String(value ?? '').toUpperCase().match(/\bRU\s+([IVX]+)\b/)
  return match ? `RU ${match[1]}` : ''
}
function domainGroup(node: GraphNode) { return node.domain || node.kind.split('_')[0] || 'related' }
function schemaDomain(row: Record<string, unknown>) {
  const raw = String(row.domain ?? row.relationship_type ?? '').toLowerCase()
  if (raw.includes('rkap') || raw.includes('cost_program')) return 'cost_program'
  if (raw.includes('readiness')) return 'readiness'
  if (raw.includes('reliability') || raw.includes('observed_in_period')) return 'reliability'
  if (raw.includes('maintenance') || raw.includes('notification')) return 'maintenance'
  if (raw.includes('issue') || raw.includes('rcps') || raw.includes('recommendation')) return 'issue'
  return raw || 'other'
}
function buildDepthDomainSummary(schema: Record<string, unknown>[], ontology: Record<string, unknown>[]): DepthDomainSummary[] {
  const summary = new Map<string, DepthDomainSummary>()
  const ensure = (key: string) => {
    if (!depthDomainLabels[key]) return null
    if (!summary.has(key)) summary.set(key, { key, label: depthDomainLabels[key], relationshipCount: 0, pathCount: 0, schemaCount: 0 })
    return summary.get(key)!
  }
  schema.forEach((row) => {
    const item = ensure(schemaDomain(row))
    if (!item) return
    item.relationshipCount += Number(row.relationship_count ?? 0)
    item.schemaCount += 1
  })
  ontology.forEach((row) => {
    const item = ensure(schemaDomain({ relationship_type: row.relationship_path }))
    if (item) item.pathCount += 1
  })
  return [...summary.values()].sort((a, b) => {
    const ai = depthDomainOrder.indexOf(a.key)
    const bi = depthDomainOrder.indexOf(b.key)
    return (ai === -1 ? 99 : ai) - (bi === -1 ? 99 : bi)
  })
}
function deriveSchemaPathFallback(schema: Record<string, unknown>[], ontology: Record<string, unknown>[], deepestPaths: Record<string, unknown>[]): Record<string, unknown>[] {
  const existing = deepestPaths.map((row) => `${row.path_pattern ?? ''} ${row.label_path ?? ''}`.toLowerCase()).join(' ')
  const wanted = [
    { relationship: 'EQUIPMENT_HAS_READINESS_RECORD', pattern: 'RU → Equipment → Readiness Record' },
    { relationship: 'EQUIPMENT_HAS_RKAP_PROGRAM', pattern: 'RU → Equipment → RKAP Program' },
    { relationship: 'REFINERY_UNIT_HAS_READINESS_RECORD', pattern: 'RU → Readiness Record' },
    { relationship: 'REFINERY_UNIT_HAS_RKAP_PROGRAM', pattern: 'RU → RKAP Program' },
  ]
  return wanted.flatMap((target) => {
    if (existing.includes(target.relationship.toLowerCase()) || existing.includes(target.pattern.toLowerCase())) return []
    const schemaRow = schema.find((row) => String(row.relationship_type) === target.relationship)
    if (!schemaRow) return []
    const ontologyRow = ontology.find((row) => String(row.relationship_path ?? '').includes(target.relationship))
    return [{
      path_id: `schema-${target.relationship}`,
      path_pattern: target.pattern,
      path_depth: ontologyRow?.depth ?? (target.relationship.startsWith('REFINERY_UNIT') ? 1 : 2),
      label_path: ontologyRow?.node_path ?? `${human(String(schemaRow.source_node_type))} → ${human(String(schemaRow.target_node_type))}`,
      analysis_scope: `${format(Number(schemaRow.relationship_count ?? 0))} verified relationships`,
      is_schema_fallback: true,
    }]
  })
}
function recordDate(node: GraphNode) {
  const values = node.properties
  return String(values.reference_date ?? values.status_date ?? values.plan_date ?? values.actual_date ?? values.month_update ?? 'No date')
}
