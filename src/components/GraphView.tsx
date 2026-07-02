import { useEffect, useMemo, useRef, useState } from 'react'
import ForceGraph2D, {
  type ForceGraphMethods,
  type LinkObject,
  type NodeObject,
} from 'react-force-graph-2d'
import type { GraphEdge, GraphNode, GraphSlice } from '../types'

const colors: Record<string, string> = {
  equipment: '#18936f',
  functional_location: '#2f6bdb',
  plant: '#6d49c9',
  plant_area: '#6d49c9',
  refinery_unit: '#7c4fd6',
  equipment_group: '#64748b',
  maintenance_order: '#c87916',
  maintenance_notification: '#d98438',
  reliability_observation: '#1f8aa6',
  time_period: '#2d77b3',
  equipment_issue: '#d23f47',
  operational_issue: '#b53a40',
  rcps: '#a849b8',
  rcps_recommendation: '#964ba0',
  status: '#3a9a5e',
  inspection: '#cf6a35',
  issue: '#d23f47',
  recommendation: '#a849b8',
  readiness_record: '#1f9173',
  rkap_program: '#b58a2a',
  availability: '#2d77b3',
  metering: '#2a7e92',
  utility: '#6f9a3d',
}

const domainColors: Record<string, string> = {
  asset: '#475569',
  reliability: '#1f8aa6',
  maintenance: '#c87916',
  readiness: '#1f9173',
  cost_program: '#b58a2a',
  issue: '#d23f47',
}

interface Props {
  graph: GraphSlice
  rootId: string
  selectedId?: string
  selectedEdgeId?: string
  onSelect: (node: GraphNode) => void
  onSelectEdge: (edge: GraphEdge) => void
}

type ForceNode = GraphNode & {
  val: number
  fx?: number
  fy?: number
  x?: number
  y?: number
}

type ForceLink = GraphEdge & {
  source: string | ForceNode
  target: string | ForceNode
}

export function GraphView({ graph, rootId, selectedId, selectedEdgeId, onSelect, onSelectEdge }: Props) {
  const containerRef = useRef<HTMLDivElement>(null)
  const graphRef = useRef<ForceGraphMethods<ForceNode, ForceLink> | undefined>(undefined)
  const zoomFrame = useRef<number | undefined>(undefined)
  const [size, setSize] = useState({ width: 900, height: 620 })
  const [zoom, setZoom] = useState(1)
  const [showLabels, setShowLabels] = useState(false)
  const [legendOpen, setLegendOpen] = useState(true)
  const [hoveredId, setHoveredId] = useState('')
  const [pinnedCount, setPinnedCount] = useState(0)

  const deepPathNodes = useMemo(() => new Set((graph.paths ?? []).flatMap((path) => path.node_id_path)), [graph.paths])
  const neighborIds = useMemo(() => {
    if (!selectedId) return new Set<string>()
    const ids = new Set<string>([selectedId])
    graph.edges.forEach((edge) => {
      if (edge.source === selectedId) ids.add(edge.target)
      if (edge.target === selectedId) ids.add(edge.source)
    })
    return ids
  }, [graph.edges, selectedId])

  const graphData = useMemo<{ nodes: ForceNode[]; links: ForceLink[] }>(() => ({
    nodes: graph.nodes.map((node): ForceNode => ({
      ...node,
      val: node.id === rootId ? 8 : node.kind === 'equipment' ? 5.5 : 3.8,
    })),
    links: graph.edges.map((edge): ForceLink => ({ ...edge })),
  }), [graph.edges, graph.nodes, rootId])

  useEffect(() => {
    const element = containerRef.current
    if (!element) return
    const resize = () => {
      const rect = element.getBoundingClientRect()
      setSize({
        width: Math.max(320, Math.floor(rect.width)),
        height: Math.max(360, Math.floor(rect.height)),
      })
    }
    resize()
    const observer = new ResizeObserver(resize)
    observer.observe(element)
    return () => observer.disconnect()
  }, [])

  useEffect(() => {
    setPinnedCount(0)
    window.setTimeout(() => fitGraph(450, 90), 120)
  }, [graphData])

  useEffect(() => () => {
    if (zoomFrame.current != null) window.cancelAnimationFrame(zoomFrame.current)
  }, [])

  useEffect(() => {
    const fg = graphRef.current
    if (!fg) return
    fg.d3Force('charge')?.strength?.(graph.nodes.length > 160 ? -70 : -145)
    fg.d3Force('link')?.distance?.((link: ForceLink) => {
      const source = resolveNode(link.source)
      const target = resolveNode(link.target)
      return source?.id === rootId || target?.id === rootId ? 86 : 64
    })
    fg.d3Force('collide', collisionForce(22))
    fg.d3ReheatSimulation()
  }, [graph.nodes.length, graphData, rootId])

  const fitGraph = (duration = 500, padding = 70) => {
    graphRef.current?.zoomToFit(duration, padding)
    window.setTimeout(() => {
      const currentZoom = graphRef.current?.zoom()
      if (currentZoom && currentZoom > 1.45) graphRef.current?.zoom(1.45, 220)
    }, duration + 20)
  }
  const fit = () => fitGraph(500, 70)
  const resetView = () => {
    graphRef.current?.centerAt(0, 0, 450)
    graphRef.current?.zoom(1, 450)
  }
  const releasePinned = () => {
    graphData.nodes.forEach((node) => {
      node.fx = undefined
      node.fy = undefined
    })
    setPinnedCount(0)
    graphRef.current?.d3ReheatSimulation()
  }

  return (
    <div className="graph-canvas" ref={containerRef} aria-label="Visualisasi knowledge graph interaktif">
      <div className="graph-tools">
        <button onClick={() => graphRef.current?.zoom(Math.max(.25, zoom - .18), 180)} title="Zoom out">−</button>
        <span>{Math.round(zoom * 100)}%</span>
        <button onClick={() => graphRef.current?.zoom(Math.min(3, zoom + .18), 180)} title="Zoom in">+</button>
        <button onClick={fit}>Fit</button>
        <button onClick={resetView}>Reset</button>
        <button className={showLabels ? 'active' : ''} onClick={() => setShowLabels((value) => !value)}>{showLabels ? 'Labels on' : 'Labels'}</button>
        <button disabled={!pinnedCount} onClick={releasePinned}>Release pins{pinnedCount ? ` ${pinnedCount}` : ''}</button>
      </div>

      <ForceGraph2D<ForceNode, ForceLink>
        ref={graphRef}
        graphData={graphData}
        width={size.width}
        height={size.height}
        backgroundColor="#ffffff"
        nodeId="id"
        nodeVal="val"
        minZoom={.25}
        maxZoom={3}
        cooldownTicks={graph.nodes.length > 250 ? 120 : 80}
        d3VelocityDecay={.32}
        enableNodeDrag
        linkSource="source"
        linkTarget="target"
        linkColor={(link) => link.id === selectedEdgeId ? '#1d4ed8' : link.is_candidate ? '#7c3aed' : domainColors[link.domain ?? ''] ?? '#475569'}
        linkWidth={(link) => link.id === selectedEdgeId ? 3.4 : selectedTouches(link, selectedId) ? 2.6 : 1.45}
        linkLineDash={(link) => link.is_candidate ? [7, 5] : null}
        linkDirectionalArrowLength={5.5}
        linkDirectionalArrowRelPos={1}
        linkDirectionalArrowColor={(link) => link.id === selectedEdgeId ? '#1d4ed8' : link.is_candidate ? '#7c3aed' : domainColors[link.domain ?? ''] ?? '#475569'}
        linkLabel={(link) => `${human(link.type)}${link.confidence != null ? ` · confidence ${link.confidence}` : ''}`}
        linkCanvasObjectMode={() => showLabels ? 'after' : undefined}
        linkCanvasObject={(link, ctx, globalScale) => {
          drawRelationship(link, ctx, globalScale, showLabels, selectedId, selectedEdgeId)
        }}
        nodeLabel={(node) => `${node.label} · ${human(node.kind)}`}
        nodeColor={(node) => colors[node.kind] ?? '#64748b'}
        nodeCanvasObject={(node, ctx, globalScale) => {
          drawNode(node, ctx, globalScale, {
            color: colors[node.kind] ?? '#64748b',
            selected: node.id === selectedId,
            root: node.id === rootId,
            hovered: node.id === hoveredId,
            deep: deepPathNodes.has(node.id),
            related: neighborIds.has(node.id),
            showLabel: showLabels || graph.nodes.length <= 55 || node.id === selectedId || node.id === hoveredId || node.id === rootId,
          })
        }}
        nodePointerAreaPaint={(node, color, ctx) => {
          ctx.fillStyle = color
          ctx.beginPath()
          ctx.arc(node.x ?? 0, node.y ?? 0, nodeRadius(node) + 8, 0, 2 * Math.PI)
          ctx.fill()
        }}
        onNodeClick={(node) => onSelect(node)}
        onLinkClick={(link) => onSelectEdge(link)}
        onNodeHover={(node) => setHoveredId(node?.id ?? '')}
        onNodeDragEnd={(node) => {
          node.fx = node.x
          node.fy = node.y
          setPinnedCount(graphData.nodes.filter((item) => item.fx != null || item.fy != null).length)
        }}
        onZoom={(state) => {
          if (zoomFrame.current != null) window.cancelAnimationFrame(zoomFrame.current)
          zoomFrame.current = window.requestAnimationFrame(() => setZoom(state.k))
        }}
        showPointerCursor={(item) => Boolean(item)}
      />

      <div className="graph-legend">
        <button onClick={() => setLegendOpen((value) => !value)}>{legendOpen ? 'Legend' : 'Legend +'}</button>
        {legendOpen && <>
          <span><i className="legend-line verified" />Verified edge</span>
          <span><i className="legend-line candidate" />Candidate edge</span>
          <span><i className="legend-line selected-edge" />Selected edge</span>
          <span><i className="legend-node" />Selected node</span>
          <span><i className="legend-node deep" />Deep path</span>
        </>}
      </div>
      <div className="graph-selection-hint">Drag node untuk pin posisi · scroll/pinch untuk zoom · drag background untuk pan</div>
      {graph.truncated && <div className="graph-warning">Tampilan dibatasi {graph.nodes.length} node. Persempit filter atau load more bertahap.</div>}
    </div>
  )
}

function drawNode(
  node: NodeObject<ForceNode>,
  ctx: CanvasRenderingContext2D,
  globalScale: number,
  options: { color: string; selected: boolean; root: boolean; hovered: boolean; deep: boolean; related: boolean; showLabel: boolean },
) {
  const x = node.x ?? 0
  const y = node.y ?? 0
  const radius = nodeRadius(node)
  const faded = !options.related && !options.selected && !options.hovered
  ctx.save()
  ctx.globalAlpha = faded ? .55 : 1
  ctx.beginPath()
  ctx.arc(x, y, radius + (options.root ? 8 : 6), 0, 2 * Math.PI)
  ctx.fillStyle = `${options.color}1f`
  ctx.fill()
  if (options.selected || options.hovered || options.deep) {
    ctx.lineWidth = (options.selected ? 3.4 : 2.4) / globalScale
    ctx.strokeStyle = options.deep && !options.selected ? '#d97706' : '#2563eb'
    ctx.stroke()
  }
  ctx.beginPath()
  ctx.arc(x, y, radius, 0, 2 * Math.PI)
  ctx.fillStyle = options.color
  ctx.fill()
  ctx.beginPath()
  ctx.arc(x, y, Math.max(5, radius - 5), 0, 2 * Math.PI)
  ctx.fillStyle = '#ffffff'
  ctx.globalAlpha = faded ? .82 : .96
  ctx.fill()
  ctx.globalAlpha = 1
  ctx.fillStyle = options.color
  ctx.font = `${Math.max(7, radius * .72)}px Manrope, system-ui, sans-serif`
  ctx.textAlign = 'center'
  ctx.textBaseline = 'middle'
  ctx.fillText(initials(node.label), x, y + .4)

  if (options.showLabel) {
    drawLabel(ctx, short(node.label, 28), x, y + radius + 12, globalScale, options.selected || options.hovered)
  }
  ctx.restore()
}

function drawRelationship(
  link: LinkObject<ForceNode, ForceLink>,
  ctx: CanvasRenderingContext2D,
  globalScale: number,
  showLabels: boolean,
  selectedId?: string,
  selectedEdgeId?: string,
) {
  if (!showLabels) return
  const source = resolveNode(link.source)
  const target = resolveNode(link.target)
  if (!source || !target || source.x == null || source.y == null || target.x == null || target.y == null) return
  const x = (source.x + target.x) / 2
  const y = (source.y + target.y) / 2
  drawLabel(ctx, human(link.type), x, y - 6, globalScale, selectedTouches(link, selectedId) || link.id === selectedEdgeId)
}

function drawLabel(ctx: CanvasRenderingContext2D, text: string, x: number, y: number, globalScale: number, strong = false) {
  const fontSize = Math.max(8, 11 / globalScale)
  ctx.font = `${strong ? 800 : 700} ${fontSize}px Manrope, system-ui, sans-serif`
  const width = ctx.measureText(text).width + 10
  const height = fontSize + 6
  ctx.fillStyle = strong ? 'rgba(239,246,255,.96)' : 'rgba(255,255,255,.88)'
  roundRect(ctx, x - width / 2, y - height / 2, width, height, 4 / globalScale)
  ctx.fill()
  ctx.fillStyle = strong ? '#1d4ed8' : '#334155'
  ctx.textAlign = 'center'
  ctx.textBaseline = 'middle'
  ctx.fillText(text, x, y)
}

function roundRect(ctx: CanvasRenderingContext2D, x: number, y: number, width: number, height: number, radius: number) {
  ctx.beginPath()
  ctx.moveTo(x + radius, y)
  ctx.arcTo(x + width, y, x + width, y + height, radius)
  ctx.arcTo(x + width, y + height, x, y + height, radius)
  ctx.arcTo(x, y + height, x, y, radius)
  ctx.arcTo(x, y, x + width, y, radius)
  ctx.closePath()
}

function collisionForce(radius: number) {
  let nodes: Array<NodeObject<ForceNode>> = []
  const force = () => {
    for (let i = 0; i < nodes.length; i += 1) {
      for (let j = i + 1; j < nodes.length; j += 1) {
        const a = nodes[i]
        const b = nodes[j]
        const dx = (b.x ?? 0) - (a.x ?? 0)
        const dy = (b.y ?? 0) - (a.y ?? 0)
        const distance = Math.hypot(dx, dy) || 1
        const min = radius + nodeRadius(a) + nodeRadius(b)
        if (distance < min) {
          const push = (min - distance) / distance * .18
          const x = dx * push
          const y = dy * push
          a.vx = (a.vx ?? 0) - x
          a.vy = (a.vy ?? 0) - y
          b.vx = (b.vx ?? 0) + x
          b.vy = (b.vy ?? 0) + y
        }
      }
    }
  }
  force.initialize = (items: Array<NodeObject<ForceNode>>) => {
    nodes = items
  }
  return force
}

function selectedTouches(link: LinkObject<ForceNode, ForceLink>, selectedId?: string) {
  if (!selectedId) return false
  return resolveNode(link.source)?.id === selectedId || resolveNode(link.target)?.id === selectedId
}

function resolveNode(value: string | number | NodeObject<ForceNode> | undefined) {
  return typeof value === 'object' ? value : undefined
}

function nodeRadius(node: NodeObject<ForceNode>) {
  return node.id === undefined ? 11 : Math.max(10, Math.min(24, (node.val ?? 4) * 2.4))
}

function initials(value: string) {
  const parts = value.replace(/[_-]/g, ' ').split(/\s+/).filter(Boolean)
  const text = parts.length > 1 ? `${parts[0][0]}${parts[1][0]}` : value.slice(0, 2)
  return text.toUpperCase()
}

function short(value: string, max: number) {
  return value.length > max ? `${value.slice(0, max - 1)}...` : value
}

function human(value: string) {
  return value.replaceAll('_', ' ').toLowerCase()
}
