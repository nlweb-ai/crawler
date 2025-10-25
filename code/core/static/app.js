// API base URL - adjust if running on different host/port
const API_BASE = window.location.origin + '/api';

// Utility functions
function showAlert(message, type = 'success') {
    const alertDiv = document.createElement('div');
    alertDiv.className = `alert alert-${type}`;
    alertDiv.textContent = message;

    const alertContainer = document.getElementById('alerts');
    alertContainer.innerHTML = '';
    alertContainer.appendChild(alertDiv);

    // Only auto-hide success messages, keep errors visible
    if (type === 'success') {
        setTimeout(() => alertDiv.remove(), 5000);
    }
}

async function apiRequest(url, options = {}) {
    try {
        const response = await fetch(`${API_BASE}${url}`, {
            ...options,
            headers: {
                'Content-Type': 'application/json',
                ...options.headers
            }
        });

        if (!response.ok) {
            const error = await response.json();
            throw new Error(error.error || 'Request failed');
        }

        return await response.json();
    } catch (error) {
        showAlert(error.message, 'error');
        throw error;
    }
}

// Load and display sites
async function loadSites() {
    try {
        const sites = await apiRequest('/sites');
        const tbody = document.getElementById('sitesBody');

        if (sites.length === 0) {
            tbody.innerHTML = '<tr><td colspan="6" style="text-align: center;">No sites configured</td></tr>';
            return;
        }

        tbody.innerHTML = sites.map(site => `
            <tr>
                <td>${site.site_url}</td>
                <td class="${site.is_active ? 'status-active' : 'status-inactive'}">
                    ${site.is_active ? 'Active' : 'Inactive'}
                </td>
                <td>${site.process_interval_hours}h</td>
                <td>${site.last_processed || 'Never'}</td>
                <td>
                    <button onclick="viewFiles('${encodeURIComponent(site.site_url)}')">View Files</button>
                </td>
                <td>
                    <button class="success" onclick="processSite('${encodeURIComponent(site.site_url)}')">Process</button>
                    <button class="danger" onclick="removeSite('${encodeURIComponent(site.site_url)}')">Remove</button>
                </td>
            </tr>
        `).join('');

        updateStats();
    } catch (error) {
        console.error('Failed to load sites:', error);
    }
}

// Load and display status
async function loadStatus() {
    try {
        const response = await apiRequest('/status');
        const sites = response.sites || [];
        const tbody = document.getElementById('statusBody');

        // Update master uptime info
        if (response.master_started_at) {
            const startTime = new Date(response.master_started_at);
            const uptimeSeconds = response.master_uptime_seconds;
            const hours = Math.floor(uptimeSeconds / 3600);
            const minutes = Math.floor((uptimeSeconds % 3600) / 60);

            document.getElementById('masterStartTime').textContent = startTime.toLocaleString();
            document.getElementById('masterUptime').textContent = `${hours}h ${minutes}m`;
        }

        if (sites.length === 0) {
            tbody.innerHTML = '<tr><td colspan="6" style="text-align: center;">No data available</td></tr>';
            return;
        }

        tbody.innerHTML = sites.map(item => `
            <tr>
                <td>${item.site_url}</td>
                <td class="${item.is_active ? 'status-active' : 'status-inactive'}">
                    ${item.is_active ? 'Yes' : 'No'}
                </td>
                <td>${item.total_files}</td>
                <td>${item.manual_files}</td>
                <td>${item.total_ids}</td>
                <td>${item.last_processed || 'Never'}</td>
            </tr>
        `).join('');

        // Update statistics
        const totalSites = sites.length;
        const totalFiles = sites.reduce((sum, s) => sum + s.total_files, 0);
        const totalIds = sites.reduce((sum, s) => sum + s.total_ids, 0);

        document.getElementById('totalSites').textContent = totalSites;
        document.getElementById('totalFiles').textContent = totalFiles;
        document.getElementById('totalIds').textContent = totalIds;
    } catch (error) {
        console.error('Failed to load status:', error);
    }
}

// Add a new site
document.getElementById('addSiteForm').addEventListener('submit', async (e) => {
    e.preventDefault();

    const siteUrl = document.getElementById('siteUrl').value;
    const intervalHours = parseInt(document.getElementById('intervalHours').value);

    try {
        await apiRequest('/sites', {
            method: 'POST',
            body: JSON.stringify({ site_url: siteUrl, interval_hours: intervalHours })
        });

        showAlert(`Site ${siteUrl} added successfully`);
        document.getElementById('addSiteForm').reset();
        loadSites();
        loadStatus();
    } catch (error) {
        console.error('Failed to add site:', error);
    }
});

// Add a manual schema file
document.getElementById('addFileForm').addEventListener('submit', async (e) => {
    e.preventDefault();

    const siteUrl = document.getElementById('fileSiteUrl').value;
    const fileUrl = document.getElementById('fileUrl').value;

    try {
        await apiRequest(`/sites/${encodeURIComponent(siteUrl)}/schema-files`, {
            method: 'POST',
            body: JSON.stringify({ file_url: fileUrl })
        });

        showAlert(`Schema file added successfully`);
        document.getElementById('addFileForm').reset();
        loadStatus();
    } catch (error) {
        console.error('Failed to add schema file:', error);
    }
});

// Remove a site
async function removeSite(siteUrl) {
    if (!confirm(`Are you sure you want to remove ${decodeURIComponent(siteUrl)}?`)) {
        return;
    }

    try {
        await apiRequest(`/sites/${siteUrl}`, { method: 'DELETE' });
        showAlert(`Site removed successfully`);
        loadSites();
        loadStatus();
    } catch (error) {
        console.error('Failed to remove site:', error);
    }
}

// Manually trigger site processing
async function processSite(siteUrl) {
    try {
        const result = await apiRequest(`/process/${siteUrl}`, { method: 'POST' });
        showAlert(result.message);
    } catch (error) {
        console.error('Failed to process site:', error);
    }
}

// View files for a site
async function viewFiles(siteUrl) {
    try {
        const files = await apiRequest(`/sites/${siteUrl}/files`);
        const modal = document.getElementById('filesModal');
        const modalSiteUrl = document.getElementById('modalSiteUrl');
        const filesList = document.getElementById('filesList');

        modalSiteUrl.textContent = decodeURIComponent(siteUrl);

        if (files.length === 0) {
            filesList.innerHTML = '<p>No files found for this site</p>';
        } else {
            filesList.innerHTML = files.map(file => `
                <div class="file-item">
                    <div>
                        <span class="file-url">${file.file_url}</span>
                        <span class="badge ${file.is_manual ? 'badge-manual' : 'badge-auto'}">
                            ${file.is_manual ? 'Manual' : 'Auto'}
                        </span>
                    </div>
                    <div>
                        <small>Items: ${file.number_of_items || 0}</small>
                        <button class="danger" onclick="removeFile('${encodeURIComponent(file.file_url)}')">Remove</button>
                    </div>
                </div>
            `).join('');
        }

        modal.style.display = 'block';
    } catch (error) {
        console.error('Failed to load files:', error);
    }
}

// Close files modal
function closeFilesModal() {
    document.getElementById('filesModal').style.display = 'none';
}

// Remove a schema file
async function removeFile(fileUrl) {
    if (!confirm(`Are you sure you want to remove this schema file?`)) {
        return;
    }

    try {
        await apiRequest(`/schema-files/${fileUrl}`, { method: 'DELETE' });
        showAlert(`Schema file removed successfully`);
        closeFilesModal();
        loadStatus();
    } catch (error) {
        console.error('Failed to remove schema file:', error);
    }
}

// Update statistics
async function updateStats() {
    loadStatus();
}

// Load and display queue status
async function loadQueueStatus() {
    try {
        const status = await apiRequest('/queue/status');
        const container = document.getElementById('queueStatus');

        if (!container) return;

        // Update queue summary
        const summaryHTML = `
            <div class="queue-summary">
                <div class="queue-stat">
                    <span class="stat-label">Queue Type:</span>
                    <span class="stat-value">${status.queue_type.toUpperCase()}</span>
                </div>
                <div class="queue-stat">
                    <span class="stat-label">Pending:</span>
                    <span class="stat-value ${status.pending_jobs > 0 ? 'pending' : ''}">${status.pending_jobs}</span>
                </div>
                <div class="queue-stat">
                    <span class="stat-label">Processing:</span>
                    <span class="stat-value ${status.processing_jobs > 0 ? 'processing' : ''}">${status.processing_jobs}</span>
                </div>
                <div class="queue-stat">
                    <span class="stat-label">Failed:</span>
                    <span class="stat-value ${status.failed_jobs > 0 ? 'failed' : ''}">${status.failed_jobs}</span>
                </div>
                <div class="queue-stat">
                    <span class="stat-label">Total:</span>
                    <span class="stat-value">${status.total_jobs}</span>
                </div>
            </div>
        `;

        // Display error if any
        let errorHTML = '';
        if (status.error) {
            errorHTML = `<div class="alert alert-error">${status.error}</div>`;
        }

        // Display jobs list
        let jobsHTML = '';
        if (status.jobs && status.jobs.length > 0) {
            jobsHTML = `
                <div class="jobs-list">
                    <h3>Active Jobs</h3>
                    <table>
                        <thead>
                            <tr>
                                <th>Status</th>
                                <th>Type</th>
                                <th>Site</th>
                                <th>File</th>
                                <th>Time</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${status.jobs.map(job => {
                                const timeInfo = job.processing_time
                                    ? `${job.processing_time}s`
                                    : job.queued_at
                                        ? new Date(job.queued_at).toLocaleTimeString()
                                        : '-';

                                const statusClass = job.status === 'processing' ? 'processing' : 'pending';
                                const fileDisplay = job.file_url
                                    ? job.file_url.split('/').pop()
                                    : job.type || 'N/A';

                                return `
                                    <tr>
                                        <td><span class="badge badge-${statusClass}">${job.status}</span></td>
                                        <td>${job.type || '-'}</td>
                                        <td>${job.site ? new URL(job.site).hostname : '-'}</td>
                                        <td title="${job.file_url || ''}">${fileDisplay}</td>
                                        <td>${timeInfo}</td>
                                    </tr>
                                `;
                            }).join('')}
                        </tbody>
                    </table>
                </div>
            `;
        } else if (status.total_jobs === 0) {
            jobsHTML = '<div class="no-jobs">No jobs in queue</div>';
        }

        container.innerHTML = errorHTML + summaryHTML + jobsHTML;

    } catch (error) {
        console.error('Failed to load queue status:', error);
    }
}

// Initial load
document.addEventListener('DOMContentLoaded', () => {
    loadSites();
    loadStatus();
    loadQueueStatus();

    // Refresh data periodically
    setInterval(() => {
        loadSites();
        loadStatus();
    }, 30000); // Every 30 seconds

    // Refresh queue status more frequently
    setInterval(() => {
        loadQueueStatus();
    }, 5000); // Every 5 seconds
});