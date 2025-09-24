import axios from 'axios'

const api = axios.create({
  baseURL: '/api',
})

export type ConnectPayload = {
  db_type: 'postgresql' | 'mysql' | 'sqlserver'
  host?: string
  port?: number
  database?: string
  username?: string
  password?: string
  server?: string
  trusted_connection?: boolean
}

export async function apiConnect(payload: ConnectPayload) {
  const { data } = await api.post('/connect', payload)
  return data as { status: string; db_type: string }
}

export async function apiOverview() {
  const { data } = await api.get('/overview')
  return data as {
    db_type: string
    tables_count: number
    views_count: number
    schemas_count: number
    database_size?: string | null
  }
}

export async function apiTables() {
  const { data } = await api.get('/tables')
  return data as { schema: string; table: string }[]
}

export async function apiViews() {
  const { data } = await api.get('/views')
  return data as { schema: string; view: string }[]
}

export async function apiTableDetails(schema: string, table: string) {
  const { data } = await api.post('/table/details', { schema, table })
  return data as {
    schema: string
    table: string
    row_count: number
    columns: { name: string; type: string; nullable: string; default: string | null }[]
    estimated_size?: string | null
  }
}

export async function apiTableIndexes(schema: string, table: string) {
  const { data } = await api.post('/table/indexes', { schema, table })
  return data as { schema: string; table: string; indexes: any[] }
}

