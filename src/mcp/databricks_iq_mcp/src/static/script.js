// Databricks Tags MCP Server JavaScript

class DatabricksTagsManager {
    constructor() {
        this.baseUrl = '';
        this.currentResource = null;
        this.init();
    }

    async init() {
        this.setupEventListeners();
        await this.checkServerStatus();
    }

    setupEventListeners() {
        // Tab navigation
        document.querySelectorAll('.tab-button').forEach(button => {
            button.addEventListener('click', (e) => {
                this.switchTab(e.target.dataset.tab);
            });
        });

        // Modal close events
        document.querySelectorAll('.close').forEach(closeBtn => {
            closeBtn.addEventListener('click', () => {
                this.closeModal();
            });
        });

        // Click outside modal to close
        window.addEventListener('click', (e) => {
            if (e.target.classList.contains('modal')) {
                this.closeModal();
            }
        });

        // Form submissions
        this.setupFormHandlers();
    }

    setupFormHandlers() {
        // Search form
        const searchForm = document.getElementById('searchForm');
        if (searchForm) {
            searchForm.addEventListener('submit', (e) => {
                e.preventDefault();
                this.performSearch();
            });
        }

        // Tag editor form
        const tagForm = document.getElementById('tagForm');
        if (tagForm) {
            tagForm.addEventListener('submit', (e) => {
                e.preventDefault();
                this.saveTags();
            });
        }

        // Bulk operations form
        const bulkForm = document.getElementById('bulkForm');
        if (bulkForm) {
            bulkForm.addEventListener('submit', (e) => {
                e.preventDefault();
                this.performBulkOperation();
            });
        }

        // Compliance report form
        const complianceForm = document.getElementById('complianceForm');
        if (complianceForm) {
            complianceForm.addEventListener('submit', (e) => {
                e.preventDefault();
                this.generateComplianceReport();
            });
        }
    }

    switchTab(tabName) {
        // Hide all tab contents
        document.querySelectorAll('.tab-content').forEach(content => {
            content.classList.remove('active');
        });

        // Remove active class from all buttons
        document.querySelectorAll('.tab-button').forEach(button => {
            button.classList.remove('active');
        });

        // Show selected tab content
        const selectedContent = document.getElementById(tabName);
        if (selectedContent) {
            selectedContent.classList.add('active');
        }

        // Add active class to clicked button
        const selectedButton = document.querySelector(`[data-tab="${tabName}"]`);
        if (selectedButton) {
            selectedButton.classList.add('active');
        }

        // Load data for the selected tab
        this.loadTabData(tabName);
    }

    async loadTabData(tabName) {
        switch (tabName) {
            case 'overview':
                await this.loadOverview();
                break;
            case 'clusters':
                await this.loadClusters();
                break;
            case 'warehouses':
                await this.loadWarehouses();
                break;
            case 'jobs':
                await this.loadJobs();
                break;
            case 'pipelines':
                await this.loadPipelines();
                break;
            case 'experiments':
                await this.loadExperiments();
                break;
            case 'models':
                await this.loadModels();
                break;
            case 'catalogs':
                await this.loadCatalogs();
                break;
            case 'serving':
                await this.loadServingEndpoints();
                break;
            case 'budgets':
                await this.loadBudgetPolicies();
                break;
        }
    }

    async checkServerStatus() {
        const statusDot = document.querySelector('.status-dot');
        const statusText = document.querySelector('.status-text');
        
        try {
            const response = await fetch('/health');
            if (response.ok) {
                statusDot.classList.add('connected');
                statusText.textContent = 'Connected to Databricks';
            } else {
                throw new Error('Server not responding');
            }
        } catch (error) {
            statusDot.classList.add('error');
            statusText.textContent = 'Connection failed';
            console.error('Server status check failed:', error);
        }
    }

    async loadOverview() {
        try {
            this.showLoading('overview-metrics');
            
            const [clusters, warehouses, jobs, pipelines] = await Promise.all([
                this.apiCall('/clusters'),
                this.apiCall('/warehouses'),
                this.apiCall('/jobs'),
                this.apiCall('/pipelines')
            ]);

            this.updateMetrics({
                clusters: clusters.length,
                warehouses: warehouses.length,
                jobs: jobs.length,
                pipelines: pipelines.length
            });

            this.hideLoading('overview-metrics');
        } catch (error) {
            this.showError('Failed to load overview data');
            this.hideLoading('overview-metrics');
        }
    }

    async loadClusters() {
        try {
            this.showLoading('clusters-list');
            const clusters = await this.apiCall('/clusters');
            this.renderResourceList('clusters-list', clusters, 'cluster');
            this.hideLoading('clusters-list');
        } catch (error) {
            this.showError('Failed to load clusters');
            this.hideLoading('clusters-list');
        }
    }

    async loadWarehouses() {
        try {
            this.showLoading('warehouses-list');
            const warehouses = await this.apiCall('/warehouses');
            this.renderResourceList('warehouses-list', warehouses, 'warehouse');
            this.hideLoading('warehouses-list');
        } catch (error) {
            this.showError('Failed to load warehouses');
            this.hideLoading('warehouses-list');
        }
    }

    async loadJobs() {
        try {
            this.showLoading('jobs-list');
            const jobs = await this.apiCall('/jobs');
            this.renderResourceList('jobs-list', jobs, 'job');
            this.hideLoading('jobs-list');
        } catch (error) {
            this.showError('Failed to load jobs');
            this.hideLoading('jobs-list');
        }
    }

    async loadPipelines() {
        try {
            this.showLoading('pipelines-list');
            const pipelines = await this.apiCall('/pipelines');
            this.renderResourceList('pipelines-list', pipelines, 'pipeline');
            this.hideLoading('pipelines-list');
        } catch (error) {
            this.showError('Failed to load pipelines');
            this.hideLoading('pipelines-list');
        }
    }

    async loadExperiments() {
        try {
            this.showLoading('experiments-list');
            const experiments = await this.apiCall('/experiments');
            this.renderResourceList('experiments-list', experiments, 'experiment');
            this.hideLoading('experiments-list');
        } catch (error) {
            this.showError('Failed to load experiments');
            this.hideLoading('experiments-list');
        }
    }

    async loadModels() {
        try {
            this.showLoading('models-list');
            const models = await this.apiCall('/models');
            this.renderResourceList('models-list', models, 'model');
            this.hideLoading('models-list');
        } catch (error) {
            this.showError('Failed to load models');
            this.hideLoading('models-list');
        }
    }

    async loadCatalogs() {
        try {
            this.showLoading('catalogs-list');
            const catalogs = await this.apiCall('/catalogs');
            this.renderResourceList('catalogs-list', catalogs, 'catalog');
            this.hideLoading('catalogs-list');
        } catch (error) {
            this.showError('Failed to load catalogs');
            this.hideLoading('catalogs-list');
        }
    }

    async loadServingEndpoints() {
        try {
            this.showLoading('serving-list');
            const endpoints = await this.apiCall('/serving_endpoints');
            this.renderResourceList('serving-list', endpoints, 'serving_endpoint');
            this.hideLoading('serving-list');
        } catch (error) {
            this.showError('Failed to load serving endpoints');
            this.hideLoading('serving-list');
        }
    }

    async loadBudgetPolicies() {
        try {
            this.showLoading('budget-policies-list');
            const policies = await this.apiCall('/budget_policies');
            this.renderBudgetPoliciesList('budget-policies-list', policies);
            this.hideLoading('budget-policies-list');
        } catch (error) {
            this.showError('Failed to load budget policies');
            this.hideLoading('budget-policies-list');
        }
    }

    renderResourceList(containerId, resources, resourceType) {
        const container = document.getElementById(containerId);
        if (!container) return;

        if (resources.length === 0) {
            container.innerHTML = '<p class="text-center">No resources found</p>';
            return;
        }

        container.innerHTML = resources.map(resource => `
            <div class="resource-item">
                <div class="resource-header">
                    <div>
                        <div class="resource-name">${resource.name || resource.display_name || 'Unnamed'}</div>
                        <div class="resource-id">${resource.id || resource.cluster_id || resource.job_id || resource.pipeline_id}</div>
                    </div>
                    <button class="btn btn-primary" onclick="tagsManager.editTags('${resourceType}', '${resource.id || resource.cluster_id || resource.job_id || resource.pipeline_id}', '${resource.name || resource.display_name || 'Unnamed'}')">
                        Edit Tags
                    </button>
                </div>
                <div class="tags-container">
                    ${this.renderTags(resource.tags || {})}
                </div>
            </div>
        `).join('');
    }

    renderBudgetPoliciesList(containerId, policies) {
        const container = document.getElementById(containerId);
        if (!container) return;

        if (policies.length === 0) {
            container.innerHTML = '<p class="text-center">No budget policies found</p>';
            return;
        }

        container.innerHTML = policies.map(policy => `
            <div class="resource-item">
                <div class="resource-header">
                    <div>
                        <div class="resource-name">${policy.policy_name}</div>
                        <div class="resource-id">ID: ${policy.policy_id}</div>
                        <div class="resource-meta">Created: ${policy.created_time || 'Unknown'}</div>
                    </div>
                    <div class="resource-actions">
                        <button class="btn btn-primary" onclick="tagsManager.editBudgetPolicy('${policy.policy_id}', '${policy.policy_name}')">
                            Edit Policy
                        </button>
                        <button class="btn btn-secondary" onclick="tagsManager.applyBudgetPolicy('${policy.policy_id}', '${policy.policy_name}')">
                            Apply to Resources
                        </button>
                    </div>
                </div>
                <div class="tags-container">
                    ${this.renderTags(policy.custom_tags || {})}
                </div>
            </div>
        `).join('');
    }

    renderTags(tags) {
        if (!tags || Object.keys(tags).length === 0) {
            return '<span class="text-secondary">No tags</span>';
        }

        return Object.entries(tags).map(([key, value]) => 
            `<span class="tag"><span class="tag-key">${key}:</span> ${value}</span>`
        ).join('');
    }

    updateMetrics(metrics) {
        Object.entries(metrics).forEach(([key, value]) => {
            const element = document.getElementById(`${key}-count`);
            if (element) {
                element.textContent = value;
            }
        });
    }

    editTags(resourceType, resourceId, resourceName) {
        this.currentResource = { type: resourceType, id: resourceId, name: resourceName };
        
        // Update modal title
        document.getElementById('modalResourceName').textContent = resourceName;
        document.getElementById('modalResourceId').textContent = resourceId;
        
        // Load current tags
        this.loadCurrentTags(resourceType, resourceId);
        
        // Show modal
        document.getElementById('tagModal').style.display = 'block';
    }

    async loadCurrentTags(resourceType, resourceId) {
        try {
            const tags = await this.apiCall(`/${resourceType}s/${resourceId}/tags`);
            this.renderTagEditor(tags);
        } catch (error) {
            console.error('Failed to load current tags:', error);
            this.renderTagEditor({});
        }
    }

    renderTagEditor(tags) {
        const container = document.getElementById('tagEditor');
        container.innerHTML = '';

        // Render existing tags
        Object.entries(tags).forEach(([key, value]) => {
            this.addTagEditorItem(key, value);
        });

        // Add empty item for new tag
        this.addTagEditorItem('', '');
    }

    addTagEditorItem(key = '', value = '') {
        const container = document.getElementById('tagEditor');
        const item = document.createElement('div');
        item.className = 'tag-editor-item';
        item.innerHTML = `
            <input type="text" placeholder="Tag key" value="${key}" class="tag-key-input">
            <input type="text" placeholder="Tag value" value="${value}" class="tag-value-input">
            <button type="button" class="btn btn-danger" onclick="this.parentElement.remove()">Remove</button>
        `;
        container.appendChild(item);
    }

    addNewTag() {
        this.addTagEditorItem('', '');
    }

    async saveTags() {
        if (!this.currentResource) return;

        const tagItems = document.querySelectorAll('.tag-editor-item');
        const tags = {};

        tagItems.forEach(item => {
            const key = item.querySelector('.tag-key-input').value.trim();
            const value = item.querySelector('.tag-value-input').value.trim();
            
            if (key && value) {
                tags[key] = value;
            }
        });

        try {
            this.showLoading('save-tags-btn');
            
            await this.apiCall(`/${this.currentResource.type}s/${this.currentResource.id}/tags`, {
                method: 'PUT',
                body: JSON.stringify({ tags })
            });

            this.showSuccess('Tags updated successfully');
            this.closeModal();
            
            // Refresh the current tab
            const activeTab = document.querySelector('.tab-button.active');
            if (activeTab) {
                this.loadTabData(activeTab.dataset.tab);
            }

        } catch (error) {
            this.showError('Failed to update tags');
        } finally {
            this.hideLoading('save-tags-btn');
        }
    }

    async performSearch() {
        const query = document.getElementById('searchQuery').value.trim();
        const searchType = document.getElementById('searchType').value;

        if (!query) {
            this.showError('Please enter a search query');
            return;
        }

        try {
            this.showLoading('search-results');
            
            const results = await this.apiCall('/search', {
                method: 'POST',
                body: JSON.stringify({ query, type: searchType })
            });

            this.renderSearchResults(results);
            this.hideLoading('search-results');

        } catch (error) {
            this.showError('Search failed');
            this.hideLoading('search-results');
        }
    }

    renderSearchResults(results) {
        const container = document.getElementById('search-results');
        
        if (results.length === 0) {
            container.innerHTML = '<p class="text-center">No results found</p>';
            return;
        }

        container.innerHTML = results.map(result => `
            <div class="search-result">
                <div class="search-result-header">
                    <div>
                        <strong>${result.name}</strong>
                        <span class="search-result-type">${result.type}</span>
                    </div>
                    <button class="btn btn-primary" onclick="tagsManager.editTags('${result.type}', '${result.id}', '${result.name}')">
                        Edit Tags
                    </button>
                </div>
                <div class="resource-id">${result.id}</div>
                <div class="tags-container">
                    ${this.renderTags(result.tags)}
                </div>
            </div>
        `).join('');
    }

    async performBulkOperation() {
        const operation = document.getElementById('bulkOperation').value;
        const resourceType = document.getElementById('bulkResourceType').value;
        const tagKey = document.getElementById('bulkTagKey').value.trim();
        const tagValue = document.getElementById('bulkTagValue').value.trim();

        if (!tagKey) {
            this.showError('Please enter a tag key');
            return;
        }

        if (operation === 'add' && !tagValue) {
            this.showError('Please enter a tag value');
            return;
        }

        try {
            this.showLoading('bulk-results');
            
            const result = await this.apiCall('/bulk-operation', {
                method: 'POST',
                body: JSON.stringify({
                    operation,
                    resource_type: resourceType,
                    tag_key: tagKey,
                    tag_value: tagValue
                })
            });

            this.displayResults(result);
            this.hideLoading('bulk-results');

        } catch (error) {
            this.showError('Bulk operation failed');
            this.hideLoading('bulk-results');
        }
    }

    async generateComplianceReport() {
        const requiredTags = document.getElementById('requiredTags').value
            .split(',')
            .map(tag => tag.trim())
            .filter(tag => tag);

        if (requiredTags.length === 0) {
            this.showError('Please enter at least one required tag');
            return;
        }

        try {
            this.showLoading('compliance-results');
            
            const report = await this.apiCall('/compliance-report', {
                method: 'POST',
                body: JSON.stringify({ required_tags: requiredTags })
            });

            this.renderComplianceReport(report);
            this.hideLoading('compliance-results');

        } catch (error) {
            this.showError('Failed to generate compliance report');
            this.hideLoading('compliance-results');
        }
    }

    renderComplianceReport(report) {
        const container = document.getElementById('compliance-results');
        
        container.innerHTML = `
            <div class="compliance-summary">
                <div class="compliance-metric success">
                    <div class="value">${report.compliant}</div>
                    <div class="label">Compliant</div>
                </div>
                <div class="compliance-metric danger">
                    <div class="value">${report.non_compliant}</div>
                    <div class="label">Non-Compliant</div>
                </div>
                <div class="compliance-metric">
                    <div class="value">${report.total}</div>
                    <div class="label">Total Resources</div>
                </div>
                <div class="compliance-metric">
                    <div class="value">${report.compliance_rate}%</div>
                    <div class="label">Compliance Rate</div>
                </div>
            </div>
            
            <h4>Non-Compliant Resources:</h4>
            <div class="resource-list">
                ${report.details.map(item => `
                    <div class="resource-item">
                        <div class="resource-header">
                            <div>
                                <div class="resource-name">${item.name}</div>
                                <div class="resource-id">${item.id}</div>
                            </div>
                            <span class="search-result-type">${item.type}</span>
                        </div>
                        <div>Missing tags: ${item.missing_tags.join(', ')}</div>
                    </div>
                `).join('')}
            </div>
        `;
    }

    displayResults(result) {
        const container = document.getElementById('resultsContent');
        container.textContent = JSON.stringify(result, null, 2);
        document.querySelector('.results-container').style.display = 'block';
    }

    closeModal() {
        document.getElementById('tagModal').style.display = 'none';
        this.currentResource = null;
    }

    async apiCall(endpoint, options = {}) {
        const url = `${this.baseUrl}${endpoint}`;
        const defaultOptions = {
            headers: {
                'Content-Type': 'application/json',
            }
        };

        const response = await fetch(url, { ...defaultOptions, ...options });
        
        if (!response.ok) {
            throw new Error(`API call failed: ${response.statusText}`);
        }

        return response.json();
    }

    showLoading(elementId) {
        const element = document.getElementById(elementId);
        if (element) {
            element.innerHTML = '<div class="spinner"></div><div class="loading">Loading...</div>';
        }
    }

    hideLoading(elementId) {
        // This will be handled by the calling function that updates the content
    }

    showSuccess(message) {
        this.showMessage(message, 'success');
    }

    showError(message) {
        this.showMessage(message, 'error');
    }

    showMessage(message, type) {
        // Remove existing messages
        document.querySelectorAll('.message').forEach(msg => msg.remove());

        // Create new message
        const messageEl = document.createElement('div');
        messageEl.className = `message ${type}`;
        messageEl.textContent = message;

        // Insert at the top of the container
        const container = document.querySelector('.container');
        container.insertBefore(messageEl, container.firstChild);

        // Auto-remove after 5 seconds
        setTimeout(() => {
            messageEl.remove();
        }, 5000);
    }
}

// Initialize the application when the DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    window.tagsManager = new DatabricksTagsManager();
});

// Utility functions for inline event handlers
function addNewTag() {
    window.tagsManager.addNewTag();
}

function refreshCurrentTab() {
    const activeTab = document.querySelector('.tab-button.active');
    if (activeTab && window.tagsManager) {
        window.tagsManager.loadTabData(activeTab.dataset.tab);
    }
}

// Global wrapper functions for new resource types
function loadExperiments() {
    if (window.tagsManager) {
        window.tagsManager.loadExperiments();
    }
}

function loadModels() {
    if (window.tagsManager) {
        window.tagsManager.loadModels();
    }
}

function loadCatalogs() {
    if (window.tagsManager) {
        window.tagsManager.loadCatalogs();
    }
}

function loadServingEndpoints() {
    if (window.tagsManager) {
        window.tagsManager.loadServingEndpoints();
    }
}

function loadBudgetPolicies() {
    if (window.tagsManager) {
        window.tagsManager.loadBudgetPolicies();
    }
}

function showCreateBudgetPolicy() {
    if (window.tagsManager) {
        window.tagsManager.showCreateBudgetPolicy();
    }
}

function showApplyBudgetPolicy() {
    if (window.tagsManager) {
        window.tagsManager.showApplyBudgetPolicy();
    }
}
