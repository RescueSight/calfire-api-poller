const { app } = require('@azure/functions');
const { CosmosClient } = require('@azure/cosmos');

// Initialize Cosmos DB client
const cosmosClient = new CosmosClient({
    endpoint: process.env.COSMOS_DB_ENDPOINT,
    key: process.env.COSMOS_DB_KEY,
});

const database = cosmosClient.database(process.env.COSMOS_DB_DATABASE || 'CalFireDB');
const container = database.container(process.env.COSMOS_DB_CONTAINER || 'Alerts');

// CalFire API configuration
const CALFIRE_API_URL = 'https://incidents.fire.ca.gov/umbraco/api/IncidentApi/List?inactive=true';
const MAX_RETRIES = 3;
const RETRY_DELAY = 5000; // 5 seconds

// Timer trigger function - runs every 10 minutes
app.timer('CalFireApiPoller', {
    schedule: '0 */10 * * * *', // Every 10 minutes
    handler: async (myTimer, context) => {
        context.log('CalFire API Poller started at:', new Date().toISOString());
        
        try {
            // Fetch data from CalFire API with retry logic
            const incidents = await fetchCalFireData(context);
            
            if (!incidents || incidents.length === 0) {
                context.log('No incidents received from API');
                return;
            }

            context.log(`Received ${incidents.length} incidents from CalFire API`);

            // Process and save incidents to Cosmos DB
            const results = await processAndSaveIncidents(incidents, context);
            
            context.log(`Processing complete: ${results.new} new, ${results.updated} updated, ${results.errors} errors`);
            
        } catch (error) {
            context.log.error('Fatal error in CalFire API Poller:', error);
            throw error; // This will trigger Azure Monitor alerts
        }
    }
});

/**
 * Fetch data from CalFire API with retry logic
 */
async function fetchCalFireData(context) {
    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
        try {
            context.log(`Fetching CalFire data (attempt ${attempt}/${MAX_RETRIES})`);
            
            const response = await fetch(CALFIRE_API_URL, {
                method: 'GET',
                headers: {
                    'User-Agent': 'CalFireMonitor/1.0',
                    'Accept': 'application/json'
                },
                timeout: 30000 // 30 second timeout
            });

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }

            const data = await response.json();
            context.log(`Received response from CalFire API: ${JSON.stringify(data)}`);

            // Validate response structure
            if (!Array.isArray(data)) {
                throw new Error('Invalid response format: expected array');
            }

            context.log(`Received ${data.length} incidents from CalFire API`);
            if (data.length === 0) {
                context.log.warn('No incidents found in response');
            }
            

            return data;

        } catch (error) {
            context.log.error(`Attempt ${attempt} failed:`, error.message);
            
            if (attempt === MAX_RETRIES) {
                throw new Error(`Failed to fetch CalFire data after ${MAX_RETRIES} attempts: ${error.message}`);
            }
            
            // Wait before retry
            await new Promise(resolve => setTimeout(resolve, RETRY_DELAY * attempt));
        }
    }
}

/**
 * Process incidents and save to Cosmos DB
 */
async function processAndSaveIncidents(incidents, context) {
    const results = { new: 0, updated: 0, errors: 0 };
    const batchSize = 25; // Process in batches to avoid overwhelming Cosmos DB
    
    for (let i = 0; i < incidents.length; i += batchSize) {
        const batch = incidents.slice(i, i + batchSize);
        const batchPromises = batch.map(incident => processIncident(incident, context));
        
        const batchResults = await Promise.allSettled(batchPromises);
        
        batchResults.forEach((result, index) => {
            if (result.status === 'fulfilled') {
                if (result.value === 'new') results.new++;
                else if (result.value === 'updated') results.updated++;
            } else {
                results.errors++;
                context.log.error(`Error processing incident ${batch[index]?.UniqueId}:`, result.reason);
            }
        });
        
        // Small delay between batches
        if (i + batchSize < incidents.length) {
            await new Promise(resolve => setTimeout(resolve, 100));
        }
    }
    
    return results;
}

/**
 * Process individual incident
 */
async function processIncident(incident, context) {
    try {
        // Validate required fields
        if (!incident.UniqueId) {
            throw new Error('Missing UniqueId');
        }

        // Enhance incident data
        const processedIncident = {
            id: incident.UniqueId,
            ...incident,
            // Add processing metadata
            processedAt: new Date().toISOString(),
            coordinates: {
                type: 'Point',
                coordinates: [incident.Longitude, incident.Latitude]
            },
            // Normalize date fields
            startedDate: parseDate(incident.Started),
            updatedDate: parseDate(incident.Updated),
            extinguishedDate: parseDate(incident.ExtinguishedDate),
            // Add computed fields
            isRecent: isRecentIncident(incident.Started),
            severityLevel: calculateSeverityLevel(incident),
            // Add search-friendly fields
            searchText: `${incident.Name} ${incident.Location} ${incident.County}`.toLowerCase()
        };

        // Check if incident already exists
        try {
            const { resource: existingIncident } = await container.item(incident.UniqueId, incident.UniqueId).read();
            
            // Compare Updated timestamps to see if we need to update
            if (existingIncident && existingIncident.Updated !== incident.Updated) {
                // Update existing incident
                processedIncident.firstSeenAt = existingIncident.firstSeenAt; // Preserve original timestamp
                processedIncident.updateCount = (existingIncident.updateCount || 0) + 1;
                
                await container.item(incident.UniqueId, incident.UniqueId).replace(processedIncident);
                context.log(`Updated incident: ${incident.Name} (${incident.UniqueId})`);
                return 'updated';
            }
            
            return 'existing'; // No update needed
            
        } catch (error) {
            if (error.code === 404) {
                // New incident
                processedIncident.firstSeenAt = new Date().toISOString();
                processedIncident.updateCount = 0;
                
                await container.items.create(processedIncident);
                context.log(`New incident saved: ${incident.Name} (${incident.UniqueId})`);
                return 'new';
            }
            throw error;
        }

    } catch (error) {
        context.log.error(`Error processing incident ${incident?.UniqueId || 'unknown'}:`, error);
        throw error;
    }
}

/**
 * Helper function to parse dates safely
 */
function parseDate(dateString) {
    if (!dateString || dateString.trim() === '') return null;
    
    try {
        const date = new Date(dateString);
        return isNaN(date.getTime()) ? null : date.toISOString();
    } catch {
        return null;
    }
}

/**
 * Check if incident is recent (within last 7 days)
 */
function isRecentIncident(startedDate) {
    if (!startedDate) return false;
    
    try {
        const started = new Date(startedDate);
        const sevenDaysAgo = new Date();
        sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 7);
        
        return started >= sevenDaysAgo;
    } catch {
        return false;
    }
}

/**
 * Calculate severity level based on acres burned and containment
 */
function calculateSeverityLevel(incident) {
    const acres = incident.AcresBurned || 0;
    const contained = incident.PercentContained || 0;
    
    if (acres === 0) return 'minimal';
    
    if (acres >= 10000) return 'extreme';
    if (acres >= 5000) return 'high';
    if (acres >= 1000) return 'moderate';
    if (acres >= 100) return 'low';
    
    return 'minimal';
}

// HTTP trigger for manual execution and testing
app.http('ManualCalFireSync', {
    methods: ['POST'],
    authLevel: 'function',
    handler: async (request, context) => {
        context.log('Manual CalFire sync triggered');
        
        try {
            const incidents = await fetchCalFireData(context);
            const results = await processAndSaveIncidents(incidents, context);
            
            return {
                status: 200,
                jsonBody: {
                    success: true,
                    message: 'Sync completed successfully',
                    results: results,
                    timestamp: new Date().toISOString()
                }
            };
        } catch (error) {
            context.log.error('Manual sync failed:', error);
            
            return {
                status: 500,
                jsonBody: {
                    success: false,
                    error: error.message,
                    timestamp: new Date().toISOString()
                }
            };
        }
    }
});

// Health check endpoint
app.http('HealthCheck', {
    methods: ['GET'],
    authLevel: 'anonymous',
    route: 'health',
    handler: async (request, context) => {
        try {
            // Test Cosmos DB connection
            await container.read();
            
            return {
                status: 200,
                jsonBody: {
                    status: 'healthy',
                    timestamp: new Date().toISOString(),
                    services: {
                        cosmosDb: 'connected'
                    }
                }
            };
        } catch (error) {
            return {
                status: 503,
                jsonBody: {
                    status: 'unhealthy',
                    error: error.message,
                    timestamp: new Date().toISOString()
                }
            };
        }
    }
});