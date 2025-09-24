import { useState } from 'react'
import { Alert, Box, Button, Card, CardContent, Grid, MenuItem, TextField, Typography } from '@mui/material'
import { apiConnect, type ConnectPayload } from '../lib/api'

export default function ConnectPage() {
  const [dbType, setDbType] = useState<ConnectPayload['db_type']>('postgresql')
  const [form, setForm] = useState<ConnectPayload>({ db_type: 'postgresql' })
  const [loading, setLoading] = useState(false)
  const [message, setMessage] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)

  const handleChange = (key: keyof ConnectPayload) => (e: React.ChangeEvent<HTMLInputElement>) => {
    const value = e.target.type === 'number' ? Number(e.target.value) : e.target.value
    setForm((prev) => ({ ...prev, [key]: value }))
  }

  const handleDbTypeChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const next = e.target.value as ConnectPayload['db_type']
    setDbType(next)
    setForm({ db_type: next })
  }

  const onSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    setLoading(true)
    setMessage(null)
    setError(null)
    try {
      const res = await apiConnect(form)
      setMessage(`Connected to ${res.db_type.toUpperCase()} successfully`)
    } catch (err: any) {
      setError(err?.response?.data?.detail ?? err.message)
    } finally {
      setLoading(false)
    }
  }

  return (
    <Grid container spacing={3}>
      <Grid item xs={12} md={8} lg={6}>
        <Card>
          <CardContent>
            <Typography variant="h5" gutterBottom>Connect to Database</Typography>
            <Box component="form" onSubmit={onSubmit} sx={{ mt: 2, display: 'grid', gap: 2 }}>
              <TextField select label="Database Type" value={dbType} onChange={handleDbTypeChange} fullWidth>
                <MenuItem value="postgresql">PostgreSQL</MenuItem>
                <MenuItem value="mysql">MySQL</MenuItem>
                <MenuItem value="sqlserver">SQL Server</MenuItem>
              </TextField>

              {dbType === 'sqlserver' ? (
                <>
                  <TextField label="Server" value={form.server ?? ''} onChange={handleChange('server')} fullWidth />
                  <TextField label="Database" value={form.database ?? ''} onChange={handleChange('database')} fullWidth />
                  <TextField label="Username" value={form.username ?? ''} onChange={handleChange('username')} fullWidth />
                  <TextField label="Password" type="password" value={form.password ?? ''} onChange={handleChange('password')} fullWidth />
                </>
              ) : (
                <>
                  <TextField label="Host" value={form.host ?? ''} onChange={handleChange('host')} fullWidth />
                  <TextField label="Port" type="number" value={form.port ?? ''} onChange={handleChange('port')} fullWidth />
                  <TextField label="Database" value={form.database ?? ''} onChange={handleChange('database')} fullWidth />
                  <TextField label="Username" value={form.username ?? ''} onChange={handleChange('username')} fullWidth />
                  <TextField label="Password" type="password" value={form.password ?? ''} onChange={handleChange('password')} fullWidth />
                </>
              )}

              <Box sx={{ display: 'flex', gap: 2, mt: 1 }}>
                <Button type="submit" disabled={loading} variant="contained">{loading ? 'Connecting...' : 'Connect'}</Button>
                <Button type="button" variant="outlined" onClick={() => setForm({ db_type: dbType })}>Reset</Button>
              </Box>
            </Box>

            {message && <Alert sx={{ mt: 2 }} severity="success">{message}</Alert>}
            {error && <Alert sx={{ mt: 2 }} severity="error">{error}</Alert>}
          </CardContent>
        </Card>
      </Grid>
    </Grid>
  )
}

