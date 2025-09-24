import { useEffect, useMemo, useState } from 'react'
import { Alert, Box, Card, CardContent, CircularProgress, Dialog, DialogContent, DialogTitle, IconButton, InputAdornment, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, TextField, Typography } from '@mui/material'
import SearchIcon from '@mui/icons-material/Search'
import InfoIcon from '@mui/icons-material/Info'
import { apiTableDetails, apiTableIndexes, apiTables } from '../lib/api'

type TableRowItem = { schema: string; table: string }

export default function TablesPage() {
  const [tables, setTables] = useState<TableRowItem[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [query, setQuery] = useState('')
  const [selected, setSelected] = useState<TableRowItem | null>(null)
  const [details, setDetails] = useState<any | null>(null)
  const [indexes, setIndexes] = useState<any[] | null>(null)

  useEffect(() => {
    (async () => {
      try {
        setLoading(true)
        setError(null)
        const t = await apiTables()
        setTables(t)
      } catch (err: any) {
        setError(err?.response?.data?.detail ?? err.message)
      } finally {
        setLoading(false)
      }
    })()
  }, [])

  const filtered = useMemo(() => {
    const q = query.toLowerCase()
    return tables.filter((t) => `${t.schema}.${t.table}`.toLowerCase().includes(q))
  }, [tables, query])

  const openDetails = async (row: TableRowItem) => {
    setSelected(row)
    setDetails(null)
    setIndexes(null)
    try {
      const [d, i] = await Promise.all([
        apiTableDetails(row.schema, row.table),
        apiTableIndexes(row.schema, row.table),
      ])
      setDetails(d)
      setIndexes(i.indexes)
    } catch (err: any) {
      setError(err?.response?.data?.detail ?? err.message)
    }
  }

  return (
    <Box sx={{ display: 'grid', gap: 2 }}>
      <TextField
        placeholder="Search tables..."
        value={query}
        onChange={(e) => setQuery(e.target.value)}
        InputProps={{ startAdornment: (<InputAdornment position="start"><SearchIcon/></InputAdornment>) }}
      />

      {loading && <CircularProgress />}
      {error && <Alert severity="error">{error}</Alert>}

      {!loading && !error && (
        <Card>
          <CardContent>
            <Typography variant="h6" gutterBottom>Tables ({filtered.length})</Typography>
            <TableContainer>
              <Table size="small">
                <TableHead>
                  <TableRow>
                    <TableCell>Schema</TableCell>
                    <TableCell>Table</TableCell>
                    <TableCell align="right">Actions</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {filtered.map((row) => (
                    <TableRow key={`${row.schema}.${row.table}`} hover>
                      <TableCell>{row.schema}</TableCell>
                      <TableCell>{row.table}</TableCell>
                      <TableCell align="right">
                        <IconButton onClick={() => openDetails(row)} aria-label="details"><InfoIcon/></IconButton>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          </CardContent>
        </Card>
      )}

      <Dialog open={!!selected} onClose={() => setSelected(null)} maxWidth="md" fullWidth>
        <DialogTitle>{selected ? `${selected.schema}.${selected.table}` : ''}</DialogTitle>
        <DialogContent dividers>
          {!details ? (
            <CircularProgress />
          ) : (
            <Box sx={{ display: 'grid', gap: 2 }}>
              <Typography variant="subtitle1">Rows: {details.row_count?.toLocaleString?.() ?? details.row_count}</Typography>
              <Typography variant="subtitle1">Estimated Size: {details.estimated_size ?? 'N/A'}</Typography>
              <Typography variant="h6">Columns</Typography>
              <Table size="small">
                <TableHead>
                  <TableRow>
                    <TableCell>Name</TableCell>
                    <TableCell>Type</TableCell>
                    <TableCell>Nullable</TableCell>
                    <TableCell>Default</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {details.columns?.map((c: any) => (
                    <TableRow key={c.name}>
                      <TableCell>{c.name}</TableCell>
                      <TableCell>{c.type}</TableCell>
                      <TableCell>{c.nullable}</TableCell>
                      <TableCell>{String(c.default ?? '')}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>

              <Typography variant="h6" sx={{ mt: 2 }}>Indexes</Typography>
              {!indexes ? <CircularProgress /> : (
                <Table size="small">
                  <TableHead>
                    <TableRow>
                      <TableCell>Index</TableCell>
                      <TableCell>Columns</TableCell>
                      <TableCell>Unique</TableCell>
                      <TableCell>Primary</TableCell>
                      <TableCell>Type</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {indexes.map((ix: any, idx: number) => (
                      <TableRow key={idx}>
                        <TableCell>{ix.index_name ?? ix.index}</TableCell>
                        <TableCell>{ix.columns ?? ix.column}</TableCell>
                        <TableCell>{String(ix.is_unique ?? false)}</TableCell>
                        <TableCell>{String(ix.is_primary ?? false)}</TableCell>
                        <TableCell>{ix.index_type}</TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              )}
            </Box>
          )}
        </DialogContent>
      </Dialog>
    </Box>
  )
}

