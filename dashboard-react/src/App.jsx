import { useEffect, useMemo, useState } from 'react'
import {
  ReactFlow,
  Background,
  Controls,
  MiniMap,
  Handle,
  Position,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import './App.css'

const API = 'http://localhost:8000'

function StreamNode({ data }) {
  return (
    <div className={`stream-node ${data.type || ''}`}>
      <Handle type="target" position={Position.Left} />
      <div className="node-title">{data.label}</div>
      <div className="node-type">{data.type}</div>
      <Handle type="source" position={Position.Right} />
    </div>
  )
}

const nodeTypes = {
  stream: StreamNode,
}

function App() {
  const [topology, setTopology] = useState(null)
  const [status, setStatus] = useState(null)
  const [apiStatus, setApiStatus] = useState('CONNECTING')

  useEffect(() => {
    let active = true

    const load = async () => {
      try {
        const [topologyResponse, statusResponse] = await Promise.all([
          fetch(`${API}/api/topology`),
          fetch(`${API}/api/status`),
        ])

        if (!topologyResponse.ok || !statusResponse.ok) {
          throw new Error('API request failed')
        }

        const topologyData = await topologyResponse.json()
        const statusData = await statusResponse.json()

        if (active) {
          setTopology(topologyData)
          setStatus(statusData)
          setApiStatus('CONNECTED')
        }
      } catch {
        if (active) {
          setApiStatus('DISCONNECTED')
        }
      }
    }

    load()
    const timer = setInterval(load, 2000)

    return () => {
      active = false
      clearInterval(timer)
    }
  }, [])

  const nodes = useMemo(() => {
    if (!topology?.nodes) return []

    const positions = [
      [0, 120],
      [0, 300],
      [260, 210],
      [520, 210],
      [780, 210],
      [1040, 210],
      [1300, 210],
      [1560, 210],
      [1820, 210],
      [2080, 210],
    ]

    return topology.nodes.map((node, index) => ({
      id: node.id,
      type: 'stream',
      position: {
        x: positions[index]?.[0] ?? index * 260,
        y: positions[index]?.[1] ?? 210,
      },
      data: {
        label: node.label,
        type: node.type,
      },
    }))
  }, [topology])

  const edges = useMemo(() => {
    if (!topology?.edges) return []

    return topology.edges.map(([source, target], index) => ({
      id: `edge-${index}`,
      source,
      target,
      animated: true,
    }))
  }, [topology])

  return (
    <div className="app">
      <header className="header">
        <div>
          <div className="eyebrow">REAL-TIME STREAM PROCESSING</div>
          <h1>StreamForge</h1>
          <p>Distributed Python Event Processor</p>
        </div>

        <div className={`api-status ${apiStatus.toLowerCase()}`}>
          <span className="status-dot" />
          API {apiStatus}
        </div>
      </header>

      <section className="metrics">
        <div className="metric-card">
          <span>Worker</span>
          <strong>{status?.worker_id || '—'}</strong>
          <small>Active processor</small>
        </div>

        <div className="metric-card">
          <span>Worker Status</span>
          <strong>{status?.worker_status || 'WAITING'}</strong>
          <small>Runtime state</small>
        </div>

        <div className="metric-card highlight">
          <span>Events / Sec</span>
          <strong>{status?.events_per_second ?? 0}</strong>
          <small>Live throughput</small>
        </div>

        <div className="metric-card">
          <span>Processing Lag</span>
          <strong>
            {Number(status?.processing_lag ?? 0).toFixed(2)}s
          </strong>
          <small>Event-time lag</small>
        </div>

        <div className="metric-card">
          <span>Partitions</span>
          <strong>{status?.assigned_partitions?.length ?? 0}</strong>
          <small>
            {status?.assigned_partitions?.join(', ') || 'None'}
          </small>
        </div>
      </section>

      <section className="analytics-grid">
        <div className="analytics-card">
          <span>Latest Temperature</span>
          <strong>
            {status?.temperature != null
              ? `${Number(status.temperature).toFixed(2)}°C`
              : '—'}
          </strong>
          <small>Latest telemetry reading</small>
        </div>

        <div className="analytics-card">
          <span>Rolling Average</span>
          <strong>
            {status?.rolling_average != null
              ? `${Number(status.rolling_average).toFixed(2)}°C`
              : '—'}
          </strong>
          <small>5-minute event window</small>
        </div>

        <div className="analytics-card">
          <span>Processing Latency</span>
          <strong>
            {status?.processing_latency != null
              ? `${Number(status.processing_latency).toFixed(3)}s`
              : '—'}
          </strong>
          <small>Processor execution time</small>
        </div>

        <div className="analytics-card">
          <span>Last Updated</span>
          <strong className="timestamp">
            {status?.updated_at
              ? new Date(status.updated_at).toLocaleTimeString()
              : '—'}
          </strong>
          <small>Live API refresh</small>
        </div>
      </section>

      <section className="topology-card">
        <div className="section-heading">
          <div>
            <div className="section-label">PIPELINE VISUALIZATION</div>
            <h2>Live Processing Topology</h2>
            <p>
              Kafka → Processor → Window → State → Metrics → API → Dashboard
            </p>
          </div>

          <div className="partition-info">
            <span>Active partitions</span>
            <strong>
              {status?.assigned_partitions?.join(', ') || 'None'}
            </strong>
          </div>
        </div>

        <div className="flow-container">
          {topology ? (
            <ReactFlow
              nodes={nodes}
              edges={edges}
              nodeTypes={nodeTypes}
              fitView
              attributionPosition="bottom-left"
            >
              <Background />
              <Controls />
              <MiniMap />
            </ReactFlow>
          ) : (
            <div className="loading">
              Waiting for topology data from FastAPI...
            </div>
          )}
        </div>
      </section>

      <section className="system-panel">
        <div>
          <div className="section-label">SYSTEM HEALTH</div>
          <h2>StreamForge Runtime</h2>
        </div>

        <div className="health-items">
          <div>
            <span className="health-dot" />
            <div>
              <strong>Kafka Stream</strong>
              <small>Telemetry ingestion active</small>
            </div>
          </div>

          <div>
            <span className="health-dot" />
            <div>
              <strong>State Store</strong>
              <small>RocksDB persistence enabled</small>
            </div>
          </div>

          <div>
            <span className="health-dot" />
            <div>
              <strong>FastAPI</strong>
              <small>Metrics and status API online</small>
            </div>
          </div>

          <div>
            <span className="health-dot" />
            <div>
              <strong>Dashboard</strong>
              <small>Live monitoring connected</small>
            </div>
          </div>
        </div>
      </section>

      <footer>
        StreamForge • Live data from FastAPI • No simulated telemetry
      </footer>
    </div>
  )
}

export default App