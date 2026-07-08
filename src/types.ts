export interface SourceReference {
  workbook: string
  sheet: string
  row?: number | null
  record_id?: string | null
}

export interface GraphNode {
  id: string
  kind: string
  label: string
  subtitle?: string
  domain?: string
  refinery_unit?: string
  equipment_code_normalized?: string
  properties: Record<string, unknown>
  source: SourceReference
}

export type EquipmentRelated = GraphNode & {
  relationship_type: string
  is_candidate?: boolean
  confidence?: number | null
  matched_token?: string
}

export interface Equipment360 {
  equipment: GraphNode
  related: EquipmentRelated[]
}

export interface GraphEdge {
  id: string
  source: string
  target: string
  type: string
  domain?: string
  confidence?: number | null
  match_method?: string
  is_candidate?: boolean
  properties?: Record<string, unknown>
  source_ref: SourceReference
  source_node?: GraphNode | null
  target_node?: GraphNode | null
}

export type GraphEdgeDetail = GraphEdge

export interface FolderFile {
  name: string
  path: string | null
  size: number
  modified_at: number | null
  workbook_type: string
  file_type?: string
  required: boolean
  status: 'Missing' | 'Copying' | 'Ready' | 'Already imported' | 'Changed' | 'Invalid' | 'Optional'
  stable: boolean
  sheets: string[]
  warnings: string[]
  row_count?: number | null
  columns?: string[]
}

export interface FolderScan {
  folder: string
  exists: boolean
  readable: boolean
  package_type?: string
  ready?: boolean
  scan_interval_seconds: number
  stability_seconds: number
  files: FolderFile[]
}

export interface DatasetSummary {
  id: string
  name: string
  created_at: string
  updated_at: string
  path: string
  mode: 'graph_contract' | 'exact_match_fallback' | 'etl_csv_graph'
  node_count: number
  edge_count: number
  issue_count: number
  workbooks: string[]
}

export interface LoadSummaryRow {
  workbook: string
  sheet_name: string
  row_count: number
  node_count: number
  edge_count: number
  issue_count: number
  status: string
}

export interface ImportJob {
  id: string
  name: string
  status: 'queued' | 'running' | 'completed' | 'failed' | 'cancelled'
  phase: string
  progress: number
  message: string
  dataset_id?: string
  error?: string
  warnings?: string[]
  started_at: number
  finished_at?: number
}

export interface GraphSlice {
  nodes: GraphNode[]
  edges: GraphEdge[]
  truncated: boolean
  next_frontier_count?: number
  high_degree_warning?: string
  degree?: NodeDegree
  paths?: DirectedPath[]
  has_deep_descendants?: boolean
  max_depth_found?: number
}

export interface DirectedPath {
  depth: number
  node_id_path: string[]
  label_path: string[]
  relationship_path: string[]
}

export interface NodeDegree {
  node_id: string
  total_edges: number
  candidate_edges: number
  high_degree: boolean
  by_relationship_type: Array<{ relationship_type: string; count: number }>
}

export interface DatasetStats {
  nodes: number
  verified_edges: number
  candidate_edges: number
  issues: number
  node_types: Array<{ node_type: string; count: number }>
  edge_types: Array<{ relationship_type: string; is_candidate: boolean; count: number }>
}

export interface QueryMetadata {
  node_types: Array<{ type: string; count: number; fields: string[] }>
  edge_types: Array<{ type: string; is_candidate: boolean; count: number; fields: string[] }>
  core_node_fields: string[]
  core_edge_fields: string[]
}

export interface ReadinessContext {
  node_id: string
  node_type: string
  label: string
  refinery_unit: string
  direct_count: number
  tag_match_count: number
  ru_level_count: number
  semantic_status: 'Direct linked' | 'Tag matched' | 'RU only' | 'No readiness' | string
  tag_match_samples: PromptEvidenceItem[]
  domain_evidence?: Record<string, PromptEvidenceItem[]>
  reliability_engineering?: ReliabilityEngineeringSignals | null
}

export interface ReliabilityEngineeringSignals {
  // (a) keandalan
  observations?: number
  avg_mtbf?: number | null
  avg_mttr?: number | null
  max_running_hours?: number | null
  abnormal_status_count?: number
  function_status?: string | null
  issue_count?: number
  // (b) work-management
  total_orders?: number
  open_orders?: number
  closed_orders?: number
  backlog_age_median?: number | null
  backlog_age_p90?: number | null
  material_blocked_count?: number
  priority_high_count?: number
  planned_cost?: number | null
  actual_cost?: number | null
  // (c) business case RKAP
  rkap_program_count?: number
  rkap_exact_count?: number
  rkap_candidate_count?: number
  rkap_total_cost?: number | null
  rkap_top_risk_count?: number
  rkap_delayed_count?: number
  rkap_high_value_count?: number
  // (d) inspeksi (tag-match exact-boundary, indikatif)
  inspection_match_count?: number
  inspection_findings?: string[]
  // (readiness, hasil tag-match exact-boundary)
  readiness_direct?: number
  readiness_tag_match?: number
  readiness_ru_level?: number
  readiness_association?: 'direct' | 'tag' | 'ru' | 'none' | string
  readiness_tag_samples?: string[]
  // (e) kritikalitas + keyakinan
  criticality?: string | null
  confidence_note?: string | null
}

export interface PromptEvidenceItem {
  node_id: string
  node_type: string
  label: string
  domain: string
  association_type: 'selected_node' | 'direct_verified' | 'candidate_relationship' | 'tag_secondary' | 'ru_context' | string
  relationship_type?: string | null
  confidence?: number | null
  match_method?: string | null
  is_candidate?: boolean
  matched_token?: string
  equipment_or_tag?: string | null
  source: SourceReference
  properties: Record<string, unknown>
}

export interface ReviewIssue {
  issue_type: string
  identifier?: string
  message: string
  source_file?: string
  source_sheet?: string
  source_row?: number
  details_json?: string
}

export interface RuSummary {
  refinery_units: Record<string, unknown>[]
  equipment_summary: Record<string, unknown>[]
  data_coverage: Record<string, unknown>[]
  relationship_quality: Record<string, unknown>[]
  computing?: boolean
}

export interface SchemaBundle {
  graph_schema: Record<string, unknown>[]
  ontology_depth: Record<string, unknown>[]
  deepest_paths: Record<string, unknown>[]
}

export interface ReliabilityInsight {
  kpis: Record<string, number | string | null>
  cross_domain_kpis: Record<string, number | string | null>
  ru_ranking: Record<string, unknown>[]
  ru_reliability_portfolio: Record<string, unknown>[]
  mtbf_mttr_by_ru: Record<string, unknown>[]
  status_distribution: Record<string, unknown>[]
  high_risk_equipment: Record<string, unknown>[]
  equipment_action_queue: Record<string, unknown>[]
  coverage_alerts: Record<string, unknown>[]
  relationship_quality_alerts: Record<string, unknown>[]
  data_quality_backlog: Record<string, unknown>[]
  reliability_trend: Record<string, unknown>[]
  computing?: boolean
}
