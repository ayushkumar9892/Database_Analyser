import { AppBar, Box, Container, IconButton, Toolbar, Typography } from '@mui/material'
import StorageIcon from '@mui/icons-material/Storage'
import { Link, Route, Routes } from 'react-router-dom'
import ConnectPage from './pages/ConnectPage'
import OverviewPage from './pages/OverviewPage'
import TablesPage from './pages/TablesPage'
import ViewsPage from './pages/ViewsPage'

export default function App() {
  return (
    <Box sx={{ minHeight: '100vh', bgcolor: (t) => t.palette.background.default }}>
      <AppBar position="static" color="primary" enableColorOnDark>
        <Toolbar>
          <IconButton color="inherit" edge="start" sx={{ mr: 1 }} component={Link} to="/">
            <StorageIcon />
          </IconButton>
          <Typography variant="h6" sx={{ flexGrow: 1 }}>
            Database Analyzer
          </Typography>
          <Box sx={{ display: 'flex', gap: 2 }}>
            <Typography component={Link} to="/" color="inherit" sx={{ textDecoration: 'none' }}>Connect</Typography>
            <Typography component={Link} to="/overview" color="inherit" sx={{ textDecoration: 'none' }}>Overview</Typography>
            <Typography component={Link} to="/tables" color="inherit" sx={{ textDecoration: 'none' }}>Tables</Typography>
            <Typography component={Link} to="/views" color="inherit" sx={{ textDecoration: 'none' }}>Views</Typography>
          </Box>
        </Toolbar>
      </AppBar>

      <Container sx={{ py: 4 }}>
        <Routes>
          <Route path="/" element={<ConnectPage />} />
          <Route path="/overview" element={<OverviewPage />} />
          <Route path="/tables" element={<TablesPage />} />
          <Route path="/views" element={<ViewsPage />} />
        </Routes>
      </Container>
    </Box>
  )
}

