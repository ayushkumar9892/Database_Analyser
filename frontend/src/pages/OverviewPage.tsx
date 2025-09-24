import { useEffect, useState } from 'react'
import { Alert, Card, CardContent, CircularProgress, Grid, Typography } from '@mui/material'
import { apiOverview } from '../lib/api'

export default function OverviewPage() {
  const [data, setData] = useState<null | Awaited<ReturnType<typeof apiOverview>>>(null)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    (async () => {
      try {
        setLoading(true)
        const res = await apiOverview()
        setData(res)
      } catch (err: any) {
        setError(err?.response?.data?.detail ?? err.message)
      } finally {
        setLoading(false)
      }
    })()
  }, [])

  if (loading) return <CircularProgress />
  if (error) return <Alert severity="error">{error}</Alert>
  if (!data) return null

  const items = [
    { label: 'Database', value: data.db_type?.toUpperCase() ?? '—' },
    { label: 'Tables', value: data.tables_count },
    { label: 'Views', value: data.views_count },
    { label: 'Schemas', value: data.schemas_count },
    { label: 'Size', value: data.database_size ?? 'N/A' },
  ]

  return (
    <Grid container spacing={3}>
      {items.map((it) => (
        <Grid item xs={12} sm={6} md={4} key={it.label}>
          <Card>
            <CardContent>
              <Typography variant="overline" color="text.secondary">{it.label}</Typography>
              <Typography variant="h4">{it.value}</Typography>
            </CardContent>
          </Card>
        </Grid>
      ))}
    </Grid>
  )
}

