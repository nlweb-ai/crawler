# Web UI - Static Files

Frontend web interface for the Schema.org Crawler.

## Files

### HTML Pages

#### `index.html` (Main Dashboard)
Primary admin interface with sections for:
- **Sites Management** - Add, view, and delete monitored sites
- **System Status** - Overall system health and statistics
- **Recent Activity** - Processing logs and events
- **Quick Actions** - Trigger manual processing

Features:
- Real-time status updates
- Add/delete sites
- View site details (files, IDs, last processed)
- Trigger manual processing
- Responsive design

#### `login.html` (Authentication)
OAuth login page with:
- GitHub OAuth button
- Microsoft OAuth button
- API key instructions
- Link to FAQ page

#### `faq.html` (FAQ Page)
Frequently asked questions including:
- What is a schema map?
- How to get an API key
- Authentication options
- Troubleshooting

#### `files.html` (File Browser)
Detailed file explorer showing:
- All monitored files across all sites
- File URLs and schema map associations
- Number of IDs per file
- Last read timestamps
- Active/inactive status
- Expandable ID list per file

#### `queue.html` (Queue Monitor)
Real-time queue monitoring:
- Pending jobs count
- Processing jobs count
- Failed jobs count
- Job details (type, site, file URL, timestamp)
- Queue type indicator (file/servicebus/storage)

#### `workers.html` (Worker Status)
Worker pod monitoring (Kubernetes):
- List of all worker pods
- Pod status (Running, Pending, Failed)
- Current job being processed
- Worker statistics (total processed, failed)
- Last job timestamp
- Pod IP and phase

### JavaScript

#### `app.js` (Main Application Logic)
Core frontend JavaScript providing:

**API Communication:**
```javascript
const API_BASE = window.location.origin + '/api';

async function apiRequest(url, options) {
    // Handles all API calls with error handling
    // Automatic JSON parsing
    // Alert display on errors
}
```

**Key Functions:**
- `loadSites()` - Fetch and display sites
- `addSite(url, interval)` - Add new site
- `deleteSite(url)` - Remove site
- `processSite(url)` - Trigger manual processing
- `loadStatus()` - Fetch system status
- `loadFiles()` - Fetch all files
- `loadQueue()` - Fetch queue status
- `loadWorkers()` - Fetch worker status

**Auto-refresh:**
- Status updates every 5 seconds
- Queue monitoring every 3 seconds
- Worker status every 10 seconds

## Usage

### Accessing the UI

**Via Web Browser:**
```
http://localhost:5001/          # Local development
http://<external-ip>/           # Kubernetes deployment
```

**Authentication Required:**
- OAuth login (GitHub or Microsoft)
- Or use API key in requests

### Navigation

```
/               → Main dashboard (index.html)
/login          → Login page
/faq            → FAQ page
/files.html     → File browser
/queue.html     → Queue monitor
/workers.html   → Worker status
```

### API Integration

All pages use the same API endpoints via `app.js`:

```javascript
// Example: Add a site
const data = await apiRequest('/sites', {
    method: 'POST',
    body: JSON.stringify({
        site_url: 'https://example.com',
        interval_hours: 24
    })
});

// Example: Get queue status
const queueStatus = await apiRequest('/queue/status');
console.log(`Pending jobs: ${queueStatus.pending_jobs}`);
```

## Styling

All HTML files use inline CSS with:
- Modern, clean design
- Responsive layout
- Card-based sections
- Color scheme:
  - Primary: `#2c3e50` (dark blue-gray)
  - Accent: `#3498db` (blue)
  - Success: `#27ae60` (green)
  - Danger: `#e74c3c` (red)
  - Warning: `#f39c12` (orange)

## Development

### Local Testing

1. Start the API server:
```bash
python3 code/core/api.py
```

2. Open browser:
```
http://localhost:5001/
```

3. Login via OAuth or use API key

### Modifying UI

Edit HTML/JS files and refresh browser. No build step required.

**Best Practices:**
- Keep inline CSS for simplicity (no build tools)
- Use vanilla JavaScript (no frameworks)
- Handle errors gracefully with user-friendly messages
- Show loading states during API calls
- Auto-refresh for real-time data

### Adding New Pages

1. Create `newpage.html` in this directory
2. Add route in `api.py`:
```python
@app.route('/newpage.html')
def newpage():
    return send_from_directory('static', 'newpage.html')
```
3. Add navigation link in `index.html`

### Adding New API Endpoints

1. Add endpoint in `api.py`:
```python
@app.route('/api/custom', methods=['GET'])
@auth.require_auth
def custom_endpoint():
    user_id = auth.get_current_user()
    # ... your logic
    return jsonify({'result': data})
```

2. Call from JavaScript:
```javascript
async function loadCustomData() {
    const data = await apiRequest('/custom');
    // Update UI with data
}
```

## Features by Page

### index.html
- ✅ Add/delete sites
- ✅ View site status
- ✅ Manual processing trigger
- ✅ System statistics
- ✅ Auto-refresh

### files.html
- ✅ Browse all files
- ✅ Filter by site
- ✅ View IDs per file
- ✅ Expandable details
- ✅ Active/inactive status

### queue.html
- ✅ Real-time queue monitoring
- ✅ Pending/processing/failed counts
- ✅ Job details
- ✅ Queue type indicator
- ✅ Auto-refresh every 3 seconds

### workers.html
- ✅ Worker pod list (Kubernetes)
- ✅ Current job per worker
- ✅ Worker statistics
- ✅ Health status
- ✅ Auto-refresh every 10 seconds

### login.html
- ✅ OAuth login (GitHub, Microsoft)
- ✅ API key instructions
- ✅ Link to FAQ

### faq.html
- ✅ Schema map explanation
- ✅ API key instructions
- ✅ Authentication guide
- ✅ Common questions

## Browser Compatibility

Tested on:
- Chrome/Edge (modern versions)
- Firefox (modern versions)
- Safari (modern versions)

Requires:
- JavaScript enabled
- Fetch API support
- ES6+ support (async/await)

## Security Considerations

### Authentication
- All API calls go through `@require_auth` decorator
- OAuth session cookies are httpOnly
- API keys transmitted in headers (not URL)

### CORS
- CORS enabled in `api.py` for cross-origin development
- Configure allowed origins for production

### XSS Prevention
- User input sanitized before display
- No `eval()` or `innerHTML` with user data
- Content-Type headers properly set

### CSRF
- Consider adding CSRF tokens for production
- Currently relying on OAuth session + same-origin

## Troubleshooting

### "Authentication required" error
- Ensure you're logged in via OAuth
- Or provide `X-API-Key` header
- Check browser console for details

### Auto-refresh not working
- Check browser console for JavaScript errors
- Verify API endpoints are accessible
- Check network tab for failed requests

### Styling issues
- Clear browser cache
- Check for CSS syntax errors
- Verify HTML structure

### API calls failing
- Verify API server is running (`python3 api.py`)
- Check API_BASE URL in `app.js`
- Look for CORS errors in console

## Future Enhancements

Potential improvements:
- [ ] Search/filter for sites and files
- [ ] Pagination for large datasets
- [ ] Export data to CSV/JSON
- [ ] Charts and graphs for statistics
- [ ] Dark mode toggle
- [ ] Keyboard shortcuts
- [ ] Bulk operations (delete multiple sites)
- [ ] Real-time WebSocket updates (instead of polling)
- [ ] User preferences (refresh intervals, page size)
- [ ] Advanced queue filtering
