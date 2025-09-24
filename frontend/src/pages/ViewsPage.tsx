import { useEffect, useMemo, useState } from 'react'
import { Alert, Box, Card, CardContent, CircularProgress, InputAdornment, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, TextField, Typography } from '@mui/material'
import SearchIcon from '@mui/icons-material/Search'
import { apiViews } from '../lib/api'

type ViewRow = { schema: string; view: string }

export default function ViewsPage() {
  const [views, setViews] = useState<ViewRow[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [query, setQuery] = useState('')

  useEffect(() => {
    (async () => {
      try {
        setLoading(true)
        setError(null)
        const v = await apiViews()
        setViews(v)
      } catch (err: any) {
        setError(err?.response?.data?.detail ?? err.message)
      } finally {
        setLoading(false)
      }
    })()
  }, [])

  const filtered = useMemo(() => {
    const q = query.toLowerCase()
    return views.filter((v) => `${v.schema}.${v.view}`.toLowerCase().includes(q))
  }, [views, query])

  return (
    <Box sx={{ display: 'grid', gap: 2 }}>
      <TextField
        placeholder="Search views..."
        value={query}
        onChange={(e) => setQuery(e.target.value)}
        InputProps={{ startAdornment: (<InputAdornment position="start"><SearchIcon/></InputAdornment>) }}
      />

      {loading && <CircularProgress />}
      {error && <Alert severity="error">{error}</Alert>}

      {!loading && !error && (
        <Card>
          <CardContent>
            <Typography variant="h6" gutterBottom>Views ({filtered.length})</Typography>
            <TableContainer>
              <Table size="small">
                <TableHead>
                  <TableRow>
                    <TableCell>Schema</TableCell>
                    <TableCell>View</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {filtered.map((row) => (
                    <TableRow key={`${row.schema}.${row.view}`} hover>
                      <TableCell>{row.schema}</TableCell>
                      <TableCell>{row.view}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          </CardContent>
        </Card>
      )}
    </Box>
  )
}

