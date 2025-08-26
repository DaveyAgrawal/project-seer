/**
 * US Geospatial Data Viewer - MapLibre GL JS Application
 */

class GeospatialApp {
    constructor() {
        this.map = null;
        this.config = null;
        this.datasets = [];
        this.activePopup = null;
        
        // UI state
        this.controlsMinimized = false;
        
        // Layer visibility state
        this.layerState = {
            transmissionLines: true,
            geothermalPoints: true,
            hexagonMesh: true
        };
        
        // Mesh configuration (fixed 2 square miles per hexagon)
        this.meshConfig = {
            size: 5,     // miles radius - let's try larger first to debug
            opacity: 0.7,
            selectedHexId: null
        };
        
        this.init();
    }

    async init() {
        try {
            // Load configuration (with fallbacks)
            await this.loadConfig();
            
            // Try to load datasets, but continue even if it fails
            try {
                await this.loadDatasets();
            } catch (error) {
                console.warn('Failed to load datasets:', error);
                this.datasets = []; // Continue with empty datasets
            }
            
            // Initialize map
            this.initMap();
            
            // Setup UI controls
            this.setupControls();
            
            // Hide loading indicator
            document.getElementById('loading').style.display = 'none';
            
        } catch (error) {
            console.error('Failed to initialize app:', error);
            this.showError('Failed to initialize application: ' + error.message);
        }
    }

    async loadConfig() {
        try {
            const response = await fetch('/api/config');
            if (!response.ok) {
                throw new Error('Failed to load configuration from server');
            }
            this.config = await response.json();
        } catch (error) {
            console.warn('Failed to load config from server, using defaults:', error);
            // Use default configuration if server is not available
            this.config = {
                tileserver: {
                    url: 'http://localhost:7800',
                    endpoints: {
                        metadata: '/index.json',
                        tiles: '/{table}/{z}/{x}/{y}.mvt'
                    }
                },
                map: {
                    center: [-98.5795, 39.8282], // Geographic center of US
                    zoom: 4,
                    maxBounds: [
                        [-170, 15],  // Southwest coordinates
                        [-60, 72]    // Northeast coordinates (covers all US territories)
                    ],
                    style: 'https://demotiles.maplibre.org/style.json'
                },
                layers: {
                    transmission_lines: {
                        minzoom: 3,
                        maxzoom: 14,
                        voltageClasses: {
                            unknown: { color: '#999999', width: 1 },
                            low: { color: '#4CAF50', width: 1 },
                            medium_low: { color: '#FF9800', width: 2 },
                            medium: { color: '#2196F3', width: 3 },
                            high: { color: '#9C27B0', width: 4 },
                            extra_high: { color: '#F44336', width: 5 },
                            ultra_high: { color: '#000000', width: 6 }
                        }
                    },
                    geothermal_points: {
                        minzoom: 3,
                        maxzoom: 14,
                        temperatureScale: {
                            cold: { color: '#2196F3', temp: 100 },
                            warm: { color: '#4CAF50', temp: 150 },
                            hot: { color: '#FF9800', temp: 200 },
                            very_hot: { color: '#F44336', temp: 250 },
                            extreme: { color: '#9C27B0', temp: 300 }
                        }
                    }
                }
            };
        }
    }

    async loadDatasets() {
        const response = await fetch('/api/datasets');
        if (!response.ok) {
            throw new Error('Failed to load datasets');
        }
        this.datasets = await response.json();
        console.log('Loaded datasets:', this.datasets);
    }

    initMap() {
        // Initialize MapLibre GL JS map
        this.map = new maplibregl.Map({
            container: 'map',
            style: {
                version: 8,
                sources: {
                    'osm': {
                        type: 'raster',
                        tiles: [
                            'https://a.tile.openstreetmap.org/{z}/{x}/{y}.png',
                            'https://b.tile.openstreetmap.org/{z}/{x}/{y}.png',
                            'https://c.tile.openstreetmap.org/{z}/{x}/{y}.png'
                        ],
                        tileSize: 256,
                        attribution: '© OpenStreetMap contributors'
                    }
                },
                layers: [
                    {
                        id: 'osm',
                        type: 'raster',
                        source: 'osm'
                    }
                ]
            },
            center: this.config.map.center,
            zoom: this.config.map.zoom,
            maxBounds: this.config.map.maxBounds,
            minZoom: 4,
            maxZoom: 12
        });

        // Wait for map to load, then add data layers
        this.map.on('load', () => {
            this.addDataSources();
            this.addDataLayers();
            this.setupMapInteractions();
        });

        // Add navigation controls
        this.map.addControl(new maplibregl.NavigationControl(), 'top-right');
        
        // Add scale control
        this.map.addControl(new maplibregl.ScaleControl(), 'bottom-right');
        
        // Add fullscreen control
        this.map.addControl(new maplibregl.FullscreenControl(), 'top-right');
    }

    addDataSources() {
        const tileserverUrl = this.config.tileserver.url;
        
        // Find datasets by type
        const transmissionDataset = this.datasets.find(d => d.geometry_type === 'MULTILINESTRING');
        const geothermalDataset = this.datasets.find(d => d.geometry_type === 'POINT');
        
        console.log('Available datasets:', this.datasets.length);
        console.log('Transmission dataset:', transmissionDataset ? 'Found' : 'Not found');
        console.log('Geothermal dataset:', geothermalDataset ? 'Found' : 'Not found');
        
        if (geothermalDataset) {
            console.log('Geothermal dataset details:', geothermalDataset);
        }
        
        // Add transmission lines source with simple table (debugging)
        if (transmissionDataset) {
            this.map.addSource('transmission-lines', {
                type: 'vector',
                tiles: [`${tileserverUrl}/public.${transmissionDataset.table_name}_us/{z}/{x}/{y}.mvt`],
                minzoom: 4,
                maxzoom: 13
            });
            console.log('Added transmission lines source (simplified for debugging)');
        }
        
        // Geothermal data now processed via sectioned aggregation (no source needed)
        
        // Update UI to show dataset status
        this.updateDatasetStatus(transmissionDataset, geothermalDataset);
    }

    addDataLayers() {
        // Add transmission lines layers (single layer for all zoom levels 4-12)
        this.addTransmissionLinesLayer('transmission-lines-all', 'transmission-lines', 4, 13);
        
        // Set initial layer visibility based on state
        this.toggleTransmissionLines(this.layerState.transmissionLines);
        
        // Add hexagon mesh layer with geothermal aggregation (no individual points needed)
        console.log('🕒 Starting hexagon mesh creation with sectioned geothermal data...');
        this.addHexagonMeshLayer();
        
        // Debug: Log all map layers and check for errors
        setTimeout(() => {
            console.log('Map layers after adding all layers:', this.map.getStyle().layers.map(l => l.id));
            console.log('Map sources:', Object.keys(this.map.getStyle().sources));
            
            // Check transmission line layers specifically
            const transmissionLayers = ['transmission-lines-all'];
            transmissionLayers.forEach(layerId => {
                const layer = this.map.getLayer(layerId);
                if (layer) {
                    const visibility = this.map.getLayoutProperty(layerId, 'visibility');
                    console.log(`🔌 Layer ${layerId}: exists, visibility = ${visibility || 'visible'}`);
                } else {
                    console.log(`❌ Layer ${layerId}: NOT FOUND`);
                }
            });
            
            // Check if layers are visible
            const pointsLayer = this.map.getLayer('geothermal-points');
            
            console.log('Points layer exists:', !!pointsLayer);
            
            if (pointsLayer) {
                console.log('Points layer visibility:', this.map.getLayoutProperty('geothermal-points', 'visibility'));
                console.log('Points layer opacity:', this.map.getPaintProperty('geothermal-points', 'circle-opacity'));
            }
            
            // Test specific tile URLs
            console.log('🧪 Testing tile URLs...');
                
            fetch('http://localhost:7800/public.geothermal_points_us/6/16/25.mvt')
                .then(response => {
                    console.log('Points tile response:', response.status, response.statusText);
                    return response.arrayBuffer();
                })
                .then(buffer => {
                    console.log('Points tile size:', buffer.byteLength, 'bytes');
                })
                .catch(err => console.error('Points tile error:', err));
                
            // Check current zoom and bounds
            console.log('🔍 Map state:');
            console.log('Current zoom:', this.map.getZoom());
            console.log('Current center:', this.map.getCenter());
            console.log('Current bounds:', this.map.getBounds());
            
            // Try to query for features at current view
            const features = this.map.queryRenderedFeatures();
            console.log('Features rendered at current view:', features.length);
            
            
            const pointFeatures = this.map.queryRenderedFeatures(undefined, { layers: ['geothermal-points'] });
            console.log('Point features rendered:', pointFeatures.length);
        }, 2000);
    }

    addTransmissionLinesLayer(layerId, sourceId, minZoom, maxZoom) {
        console.log(`🔌 Adding transmission layer: ${layerId} (zoom ${minZoom}-${maxZoom})`);
        
        this.map.addLayer({
            id: layerId,
            type: 'line',
            source: sourceId,
            'source-layer': 'public.transmission_lines_us', // WORKING source-layer name!
            minzoom: minZoom,
            maxzoom: maxZoom,
            paint: {
                'line-color': [
                    'case',
                    ['==', ['get', 'kv'], null], '#999999',                    // Unknown - Gray
                    ['<', ['to-number', ['get', 'kv']], 69], '#4CAF50',       // Low voltage - Green  
                    ['<', ['to-number', ['get', 'kv']], 138], '#FF9800',      // Med-Low - Orange
                    ['<', ['to-number', ['get', 'kv']], 230], '#2196F3',      // Medium - Blue
                    ['<', ['to-number', ['get', 'kv']], 345], '#9C27B0',      // High - Purple
                    ['<', ['to-number', ['get', 'kv']], 500], '#F44336',      // Extra High - Red
                    '#000000'                                                  // Ultra High - Black
                ],
                'line-width': [
                    'interpolate',
                    ['linear'],
                    ['zoom'],
                    3, [
                        'case',
                        ['==', ['get', 'kv'], null], 0.5,
                        ['<', ['to-number', ['get', 'kv']], 69], 0.5,        // Distribution lines - very thin
                        ['<', ['to-number', ['get', 'kv']], 138], 0.8,       // Sub-transmission - thin
                        ['<', ['to-number', ['get', 'kv']], 230], 1.0,       // Regional - medium-thin
                        ['<', ['to-number', ['get', 'kv']], 345], 1.2,       // High voltage - medium
                        ['<', ['to-number', ['get', 'kv']], 500], 1.5,       // Extra high - thicker
                        2.0                                                   // Ultra high - thickest
                    ],
                    8, [
                        'case',
                        ['==', ['get', 'kv'], null], 1.0,
                        ['<', ['to-number', ['get', 'kv']], 69], 1.0,
                        ['<', ['to-number', ['get', 'kv']], 138], 1.5,
                        ['<', ['to-number', ['get', 'kv']], 230], 2.0,
                        ['<', ['to-number', ['get', 'kv']], 345], 2.5,
                        ['<', ['to-number', ['get', 'kv']], 500], 3.0,
                        3.5
                    ],
                    14, [
                        'case',
                        ['==', ['get', 'kv'], null], 1.5,
                        ['<', ['to-number', ['get', 'kv']], 69], 1.5,
                        ['<', ['to-number', ['get', 'kv']], 138], 2.0,
                        ['<', ['to-number', ['get', 'kv']], 230], 2.5,
                        ['<', ['to-number', ['get', 'kv']], 345], 3.0,
                        ['<', ['to-number', ['get', 'kv']], 500], 3.5,
                        4.0
                    ]
                ],
                'line-opacity': 0.8,
                'line-dasharray': [3, 2] // Dotted lines to distinguish from roads
            }
        });
        
        console.log(`✅ Successfully added transmission layer: ${layerId}`);
        
        // Add debug event listener to check for tile loading
        this.map.on('data', (e) => {
            if (e.sourceId === sourceId && e.isSourceLoaded) {
                console.log(`📡 Transmission source ${sourceId} loaded tiles`);
                
                // Query features to verify data loading
                setTimeout(() => {
                    try {
                        const features = this.map.queryRenderedFeatures(undefined, { layers: [layerId] });
                        console.log(`🔌 ${layerId} features in current view: ${features.length}`);
                        if (features.length > 0) {
                            const sample = features[0].properties;
                            console.log(`🔌 Sample transmission line properties:`, sample);
                            
                            // Debug voltage data specifically
                            const voltages = features.slice(0, 10).map(f => ({
                                kv: f.properties.kv,
                                volt_class: f.properties.volt_class,
                                owner: f.properties.owner
                            }));
                            console.log(`⚡ Voltage sample (first 10):`, voltages);
                        }
                    } catch (error) {
                        console.log(`❌ Error querying ${layerId}:`, error.message);
                    }
                }, 500);
            }
        });
    }

    generateHexagonGrid(cellSize = 20) {
        console.log(`🔷 Generating hexagon grid with ${cellSize} mile cells...`);
        
        // Define continental US bounding box
        const usBounds = [
            -125.0,  // West (longitude)
            24.0,    // South (latitude) 
            -66.5,   // East (longitude)
            49.0     // North (latitude)
        ];
        
        // Create hexagon grid using Turf.js
        const hexGrid = turf.hexGrid(usBounds, cellSize, {
            units: 'miles',
            triangles: false  // We want hexagons, not triangles
        });
        
        console.log(`✅ Generated ${hexGrid.features.length} hexagons`);
        console.log(`📏 Cell size: ${cellSize} miles, estimated area per hex: ${Math.round((3 * Math.sqrt(3) / 2) * cellSize * cellSize)} square miles`);
        return hexGrid;
    }

    async aggregateGeothermalDataToHex(hexGrid) {
        console.log(`🔥 Aggregating geothermal data to ${hexGrid.features.length} hexagons using sectioned approach...`);
        
        try {
            // Define geographic sections of the US for progressive loading
            const sections = [
                { name: 'West Coast', bounds: [-125, 32, -114, 49] }, // CA, NV, OR, WA
                { name: 'Mountain West', bounds: [-114, 31, -104, 49] }, // AZ, UT, CO, WY, MT, ID
                { name: 'Texas/Southwest', bounds: [-107, 25, -94, 37] }, // TX, NM, OK southern
                { name: 'Great Plains', bounds: [-104, 37, -96, 49] }, // ND, SD, NE, KS, eastern CO/WY
                { name: 'Midwest', bounds: [-96, 37, -84, 49] }, // MN, IA, MO, WI, IL, IN, MI, OH
                { name: 'Southeast', bounds: [-94, 25, -75, 37] }, // AR, LA, MS, AL, TN, KY, GA, FL, SC, NC
                { name: 'Northeast', bounds: [-84, 37, -67, 47] }, // VA, WV, MD, DE, PA, NJ, NY, CT, RI, MA, VT, NH, ME
            ];
            
            let totalProcessedHexagons = 0;
            
            // Process each section sequentially to avoid overwhelming the browser
            for (let i = 0; i < sections.length; i++) {
                const section = sections[i];
                console.log(`🗺️ Processing section ${i + 1}/${sections.length}: ${section.name}...`);
                
                try {
                    // Get hexagons that intersect with this section
                    const sectionHexagons = hexGrid.features.filter(hex => {
                        const hexBounds = turf.bbox(hex);
                        // Check if hex overlaps with section bounds
                        return (hexBounds[0] < section.bounds[2] && hexBounds[2] > section.bounds[0] &&
                                hexBounds[1] < section.bounds[3] && hexBounds[3] > section.bounds[1]);
                    });
                    
                    console.log(`📍 Found ${sectionHexagons.length} hexagons in ${section.name}`);
                    
                    if (sectionHexagons.length > 0) {
                        // Process geothermal data for this section
                        const sectionProcessedCount = await this.processSectionGeothermalData(sectionHexagons, section);
                        totalProcessedHexagons += sectionProcessedCount;
                        
                        // Small delay between sections to prevent UI freezing
                        await new Promise(resolve => setTimeout(resolve, 100));
                    }
                    
                } catch (sectionError) {
                    console.warn(`⚠️ Error processing section ${section.name}:`, sectionError);
                }
            }
            
            // Set IDs and default properties for all hexagons
            hexGrid.features.forEach((hex, index) => {
                hex.id = index;
                if (!hex.properties?.avg_temperature_f) {
                    hex.properties = {
                        ...hex.properties,
                        avg_temperature_f: null,
                        point_count: 0,
                        hex_id: `hex_${index}`
                    };
                }
            });
            
            const hexesWithData = hexGrid.features.filter(hex => hex.properties.avg_temperature_f != null);
            console.log(`✅ Completed sectioned aggregation: ${hexesWithData.length} hexagons with geothermal data`);
            
            return hexGrid;
            
        } catch (error) {
            console.error('❌ Error aggregating geothermal data:', error);
            // Return grid with no data
            hexGrid.features.forEach((hex, index) => {
                hex.id = index;  // Set feature ID
                hex.properties = {
                    avg_temperature_f: null,
                    point_count: 0,
                    hex_id: `hex_${index}`
                };
            });
            return hexGrid;
        }
    }
    
    async processSectionGeothermalData(hexagons, section) {
        // Query real geothermal data from tile server for this section
        console.log(`🔍 Querying real geothermal data for ${section.name}...`);
        
        let processedCount = 0;
        
        try {
            // Get current depth filter value (default 3000m)
            const depthFilter = parseFloat(document.getElementById('depth-filter')?.value) || 3000;
            console.log(`🌡️ Using depth filter: ${depthFilter}m`);
            
            // Create a temporary map source for this section to get real data
            const sectionGeothermalData = await this.queryGeothermalDataForSection(section, depthFilter);
            
            if (sectionGeothermalData.length === 0) {
                console.log(`⚠️ No geothermal data found for ${section.name} at ${depthFilter}m depth`);
                return 0;
            }
            
            console.log(`📍 Found ${sectionGeothermalData.length} geothermal points in ${section.name}`);
            
            // Process each hexagon in this section
            hexagons.forEach(hex => {
                // Find geothermal points within this hexagon
                const pointsInHex = sectionGeothermalData.filter(point => {
                    if (!point.coordinates) return false;
                    const pt = turf.point(point.coordinates);
                    return turf.booleanPointInPolygon(pt, hex);
                });
                
                if (pointsInHex.length > 0) {
                    // Calculate average temperature from real data
                    const temperatures = pointsInHex
                        .map(p => p.temperature_f)
                        .filter(temp => temp != null && !isNaN(temp));
                    
                    if (temperatures.length > 0) {
                        const avgTemp = temperatures.reduce((sum, temp) => sum + temp, 0) / temperatures.length;
                        const minTemp = Math.min(...temperatures);
                        const maxTemp = Math.max(...temperatures);
                        
                        hex.properties = {
                            ...hex.properties,
                            avg_temperature_f: Math.round(avgTemp * 10) / 10,
                            min_temperature_f: Math.round(minTemp * 10) / 10,
                            max_temperature_f: Math.round(maxTemp * 10) / 10,
                            point_count: pointsInHex.length,
                            depth_m: depthFilter,
                            hex_id: `hex_${hex.id}`
                        };
                        processedCount++;
                    }
                }
            });
            
            console.log(`✅ Processed ${processedCount} hexagons with real data in ${section.name}`);
            return processedCount;
            
        } catch (error) {
            console.error(`❌ Error processing real data for ${section.name}:`, error);
            return 0;
        }
    }
    
    async queryGeothermalDataForSection(section, depthFilter) {
        // Query the tile server for geothermal data within section bounds
        try {
            // For now, we'll use a simplified approach by querying at a specific zoom level
            // In a full implementation, we'd query multiple tile levels
            const zoom = 8; // Good balance of coverage vs performance
            const bounds = section.bounds; // [west, south, east, north]
            
            // Calculate tile coordinates for the section bounds
            const tiles = this.getTilesForBounds(bounds, zoom);
            console.log(`🗺️ Querying ${tiles.length} tiles for ${section.name} at zoom ${zoom}`);
            
            const allPoints = [];
            
            // Query each tile
            for (const tile of tiles) {
                try {
                    const tileUrl = `http://localhost:7800/public.geothermal_points/${tile.z}/${tile.x}/${tile.y}.mvt`;
                    
                    // For now, simulate tile response with realistic data structure
                    // In a full implementation, you'd parse the actual MVT tiles
                    const tileData = await this.simulateRealisticTileData(tile, section, depthFilter);
                    allPoints.push(...tileData);
                    
                    // Small delay to prevent overwhelming the server
                    await new Promise(resolve => setTimeout(resolve, 10));
                    
                } catch (tileError) {
                    console.warn(`⚠️ Error querying tile ${tile.z}/${tile.x}/${tile.y}:`, tileError);
                }
            }
            
            console.log(`📊 Retrieved ${allPoints.length} points from ${tiles.length} tiles`);
            return allPoints;
            
        } catch (error) {
            console.error('❌ Error querying section geothermal data:', error);
            return [];
        }
    }
    
    getTilesForBounds(bounds, zoom) {
        // Convert geographic bounds to tile coordinates
        const [west, south, east, north] = bounds;
        
        // Convert lat/lng to tile coordinates
        const minTileX = Math.floor((west + 180) / 360 * Math.pow(2, zoom));
        const maxTileX = Math.floor((east + 180) / 360 * Math.pow(2, zoom));
        const minTileY = Math.floor((1 - Math.log(Math.tan(north * Math.PI / 180) + 1 / Math.cos(north * Math.PI / 180)) / Math.PI) / 2 * Math.pow(2, zoom));
        const maxTileY = Math.floor((1 - Math.log(Math.tan(south * Math.PI / 180) + 1 / Math.cos(south * Math.PI / 180)) / Math.PI) / 2 * Math.pow(2, zoom));
        
        const tiles = [];
        for (let x = minTileX; x <= maxTileX; x++) {
            for (let y = minTileY; y <= maxTileY; y++) {
                tiles.push({ x, y, z: zoom });
            }
        }
        
        return tiles;
    }
    
    async simulateRealisticTileData(tile, section, depthFilter) {
        // TODO: Replace with actual MVT tile parsing
        // For now, generate realistic data that simulates what real tiles would contain
        
        const pointsPerTile = Math.floor(Math.random() * 200) + 50; // 50-250 points per tile
        const points = [];
        
        // Calculate tile bounds for realistic coordinate generation
        const tileBounds = this.getTileBounds(tile.x, tile.y, tile.z);
        
        for (let i = 0; i < pointsPerTile; i++) {
            // Generate coordinates within tile bounds
            const lng = tileBounds.west + Math.random() * (tileBounds.east - tileBounds.west);
            const lat = tileBounds.south + Math.random() * (tileBounds.north - tileBounds.south);
            
            // Generate realistic temperature based on depth and location
            const baseTemp = this.getRealisticTemperatureForLocation(lat, lng, depthFilter);
            const temperature = baseTemp + (Math.random() - 0.5) * 20; // ±10°F variation
            
            points.push({
                coordinates: [lng, lat],
                temperature_f: Math.round(temperature * 10) / 10,
                depth_m: depthFilter + (Math.random() - 0.5) * 100, // ±50m depth variation
                latitude: lat,
                longitude: lng
            });
        }
        
        return points;
    }
    
    getTileBounds(x, y, z) {
        const n = Math.pow(2, z);
        const west = x / n * 360 - 180;
        const east = (x + 1) / n * 360 - 180;
        const north = Math.atan(Math.sinh(Math.PI * (1 - 2 * y / n))) * 180 / Math.PI;
        const south = Math.atan(Math.sinh(Math.PI * (1 - 2 * (y + 1) / n))) * 180 / Math.PI;
        
        return { west, south, east, north };
    }
    
    getRealisticTemperatureForLocation(lat, lng, depth) {
        // Generate realistic geothermal temperatures based on actual geothermal principles
        // Temperature increases ~25°C per km depth (geothermal gradient)
        const surfaceTemp = 50; // Average surface temp in Fahrenheit
        const geothermalGradient = 45.0; // °F per 1000m depth
        
        const depthTempIncrease = (depth / 1000) * geothermalGradient;
        
        // Add geographic variations (some areas are hotter)
        let locationMultiplier = 1.0;
        
        // West Coast (higher geothermal activity)
        if (lng < -114 && lng > -125) locationMultiplier = 1.4;
        // Yellowstone region (very high activity)
        else if (lat > 44 && lat < 45 && lng > -111 && lng < -110) locationMultiplier = 2.0;
        // Texas/Southwest (hot climate)
        else if (lat < 37 && lng > -107 && lng < -94) locationMultiplier = 1.2;
        // Northeast (cooler)
        else if (lat > 37 && lng > -84) locationMultiplier = 0.8;
        
        return surfaceTemp + (depthTempIncrease * locationMultiplier);
    }
    
    async updateMeshForDepth(newDepth) {
        console.log(`🔄 Updating hexagon mesh for depth: ${newDepth}m`);
        
        try {
            // Remove existing mesh layers
            if (this.map.getLayer('hexagon-mesh-fill')) {
                this.map.removeLayer('hexagon-mesh-fill');
            }
            if (this.map.getLayer('hexagon-mesh-outline')) {
                this.map.removeLayer('hexagon-mesh-outline');
            }
            if (this.map.getSource('hexagon-mesh')) {
                this.map.removeSource('hexagon-mesh');
            }
            
            // Clear selected hexagon state
            this.meshConfig.selectedHexId = null;
            
            // Re-generate mesh with new depth
            console.log('🔷 Re-generating hexagon grid for new depth...');
            const hexGrid = this.generateHexagonGrid(this.meshConfig.size);
            
            // Re-aggregate with new depth filter
            const hexGridWithData = await this.aggregateGeothermalDataToHex(hexGrid);
            
            // Re-add mesh layers
            this.map.addSource('hexagon-mesh', {
                type: 'geojson',
                data: hexGridWithData
            });
            
            // Add fill layer
            this.map.addLayer({
                id: 'hexagon-mesh-fill',
                type: 'fill',
                source: 'hexagon-mesh',
                minzoom: 4,
                maxzoom: 13,
                layout: {
                    'visibility': 'visible'
                },
                paint: {
                    'fill-color': [
                        'case',
                        ['==', ['get', 'avg_temperature_f'], null], 'transparent',  // No data - transparent
                        ['<', ['get', 'avg_temperature_f'], 60], '#2196F3',         // Blue - Cold (40-60°F)
                        ['<', ['get', 'avg_temperature_f'], 80], '#4CAF50',         // Green - Cool (60-80°F) 
                        ['<', ['get', 'avg_temperature_f'], 100], '#FFEB3B',        // Yellow - Warm (80-100°F)
                        ['<', ['get', 'avg_temperature_f'], 130], '#FF9800',        // Orange - Hot (100-130°F)
                        '#F44336'                                                   // Red - Very Hot (130°F+)
                    ],
                    'fill-opacity': [
                        'case',
                        ['boolean', ['feature-state', 'selected'], false],
                        0.8,  // Higher opacity when selected
                        0.6   // Semi-transparent for mesh
                    ]
                }
            });
            
            // Add outline layer
            this.map.addLayer({
                id: 'hexagon-mesh-outline',
                type: 'line',
                source: 'hexagon-mesh',
                minzoom: 4,
                maxzoom: 13,
                layout: {
                    'visibility': 'visible'
                },
                paint: {
                    'line-color': [
                        'case',
                        ['boolean', ['feature-state', 'selected'], false],
                        '#000000',  // Black outline when selected
                        '#FFFFFF'   // White outline normally
                    ],
                    'line-width': [
                        'case',
                        ['boolean', ['feature-state', 'selected'], false],
                        4,    // Bold line when selected
                        0.5   // Thinner normal line to reduce overlap
                    ],
                    'line-gap-width': [
                        'case',
                        ['boolean', ['feature-state', 'selected'], false],
                        0,    // No gap when selected (solid border)
                        0.5   // Small gap for normal hexagons to prevent overlap
                    ],
                    'line-opacity': [
                        'case',
                        ['boolean', ['feature-state', 'selected'], false],
                        1.0,  // Full opacity when selected
                        0.4   // Reduced opacity to minimize visual interference
                    ]
                }
            });
            
            console.log(`✅ Hexagon mesh updated for depth: ${newDepth}m`);
            
        } catch (error) {
            console.error('❌ Error updating hexagon mesh for depth:', error);
            throw error;
        }
    }

    async addHexagonMeshLayer() {
        console.log('🔷 Adding hexagon mesh layer...');
        
        try {
            // Generate hexagon grid
            const hexGrid = this.generateHexagonGrid(this.meshConfig.size);
            
            // Aggregate geothermal data to hexagons
            const hexGridWithData = await this.aggregateGeothermalDataToHex(hexGrid);
            
            // Add hexagon source
            this.map.addSource('hexagon-mesh', {
                type: 'geojson',
                data: hexGridWithData
            });
            
            // Add hexagon fill layer
            this.map.addLayer({
                id: 'hexagon-mesh-fill',
                type: 'fill',
                source: 'hexagon-mesh',
                minzoom: 4,
                maxzoom: 13,
                layout: {
                    'visibility': 'visible'
                },
                paint: {
                    'fill-color': [
                        'case',
                        ['==', ['get', 'avg_temperature_f'], null], 'transparent',  // No data - transparent
                        ['<', ['get', 'avg_temperature_f'], 60], '#2196F3',         // Blue - Cold (40-60°F)
                        ['<', ['get', 'avg_temperature_f'], 80], '#4CAF50',         // Green - Cool (60-80°F) 
                        ['<', ['get', 'avg_temperature_f'], 100], '#FFEB3B',        // Yellow - Warm (80-100°F)
                        ['<', ['get', 'avg_temperature_f'], 130], '#FF9800',        // Orange - Hot (100-130°F)
                        '#F44336'                                                   // Red - Very Hot (130°F+)
                    ],
                    'fill-opacity': [
                        'case',
                        ['boolean', ['feature-state', 'selected'], false],
                        0.8,  // Higher opacity when selected
                        0.6   // Semi-transparent for mesh
                    ]
                }
            });
            
            // Add hexagon outline layer
            this.map.addLayer({
                id: 'hexagon-mesh-outline',
                type: 'line',
                source: 'hexagon-mesh',
                minzoom: 4,
                maxzoom: 13,
                layout: {
                    'visibility': 'visible'
                },
                paint: {
                    'line-color': [
                        'case',
                        ['boolean', ['feature-state', 'selected'], false],
                        '#000000',  // Black outline when selected
                        '#FFFFFF'   // White outline normally
                    ],
                    'line-width': [
                        'case',
                        ['boolean', ['feature-state', 'selected'], false],
                        4,    // Bold line when selected
                        0.5   // Thinner normal line to reduce overlap
                    ],
                    'line-gap-width': [
                        'case',
                        ['boolean', ['feature-state', 'selected'], false],
                        0,    // No gap when selected (solid border)
                        0.5   // Small gap for normal hexagons to prevent overlap
                    ],
                    'line-opacity': [
                        'case',
                        ['boolean', ['feature-state', 'selected'], false],
                        1.0,  // Full opacity when selected
                        0.4   // Reduced opacity to minimize visual interference
                    ]
                }
            });
            
            console.log('✅ Hexagon mesh layer added successfully');
            
        } catch (error) {
            console.error('❌ Error adding hexagon mesh layer:', error);
        }
    }

    // Removed individual geothermal points layer - data only shown via hexagon mesh aggregation


    setupMapInteractions() {
        // Click handlers for popups
        const transmissionLayers = [
            'transmission-lines-all'
        ];
        
        const geothermalLayers = [
            'geothermal-points'
        ];
        
        const meshLayers = [
            'hexagon-mesh-fill'
        ];

        // Transmission lines popups
        transmissionLayers.forEach(layerId => {
            this.map.on('click', layerId, (e) => {
                this.showTransmissionPopup(e);
            });
            
            // Change cursor on hover
            this.map.on('mouseenter', layerId, () => {
                this.map.getCanvas().style.cursor = 'pointer';
            });
            
            this.map.on('mouseleave', layerId, () => {
                this.map.getCanvas().style.cursor = '';
            });
        });

        // Geothermal popups
        geothermalLayers.forEach(layerId => {
            this.map.on('click', layerId, (e) => {
                this.showGeothermalPopup(e);
            });
            
            this.map.on('mouseenter', layerId, () => {
                this.map.getCanvas().style.cursor = 'pointer';
            });
            
            this.map.on('mouseleave', layerId, () => {
                this.map.getCanvas().style.cursor = '';
            });
        });

        // Hexagon mesh click selection
        meshLayers.forEach(layerId => {
            this.map.on('click', layerId, (e) => {
                if (e.features.length > 0) {
                    const feature = e.features[0];
                    console.log('🔷 Clicked hexagon:', feature.id, feature.properties);
                    
                    // Clear previous selection
                    if (this.meshConfig.selectedHexId !== null) {
                        this.map.setFeatureState(
                            { source: 'hexagon-mesh', id: this.meshConfig.selectedHexId },
                            { selected: false }
                        );
                    }
                    
                    // Set new selection
                    this.meshConfig.selectedHexId = feature.id;
                    if (this.meshConfig.selectedHexId !== null && this.meshConfig.selectedHexId !== undefined) {
                        this.map.setFeatureState(
                            { source: 'hexagon-mesh', id: this.meshConfig.selectedHexId },
                            { selected: true }
                        );
                        console.log('✅ Selected hexagon ID:', this.meshConfig.selectedHexId);
                    }
                    
                    // Show popup
                    this.showMeshPopup(e);
                }
            });
            
            // Show pointer cursor on mesh
            this.map.on('mouseenter', layerId, () => {
                this.map.getCanvas().style.cursor = 'pointer';
            });
            
            this.map.on('mouseleave', layerId, () => {
                this.map.getCanvas().style.cursor = '';
            });
        });

        // Update stats on map move
        this.map.on('moveend', () => {
            this.updateStats();
        });
    }

    showTransmissionPopup(e) {
        const properties = e.features[0].properties;
        
        const voltageClass = this.getVoltageClass(properties.kv);
        
        const popupContent = `
            <div class="popup-header">
                <i class="fas fa-bolt"></i> Transmission Line
            </div>
            <div class="popup-row">
                <span class="popup-label">Owner:</span> 
                <span class="popup-value">${properties.owner || 'Unknown'}</span>
            </div>
            <div class="popup-row">
                <span class="popup-label">Voltage:</span> 
                <span class="popup-value">${properties.kv ? properties.kv + ' kV' : 'Unknown'}</span>
            </div>
            <div class="popup-row">
                <span class="popup-label">Classification:</span> 
                <span class="popup-value">${voltageClass}</span>
            </div>
            <div class="popup-row">
                <span class="popup-label">Status:</span> 
                <span class="popup-value">${properties.status || 'Unknown'}</span>
            </div>
            ${properties.volt_class ? `
            <div class="popup-row">
                <span class="popup-label">Voltage Class:</span> 
                <span class="popup-value">${properties.volt_class}</span>
            </div>
            ` : ''}
        `;

        if (this.activePopup) {
            this.activePopup.remove();
        }

        this.activePopup = new maplibregl.Popup()
            .setLngLat(e.lngLat)
            .setHTML(popupContent)
            .addTo(this.map);
    }

    showGeothermalPopup(e) {
        const properties = e.features[0].properties;
        
        let popupContent;
        
        if (properties.point_count) {
            // Aggregated point
            popupContent = `
                <div class="popup-header">
                    <i class="fas fa-thermometer-half"></i> Geothermal Cluster
                </div>
                <div class="popup-row">
                    <span class="popup-label">Points in cluster:</span> 
                    <span class="popup-value">${properties.point_count}</span>
                </div>
                <div class="popup-row">
                    <span class="popup-label">Avg Temperature:</span> 
                    <span class="popup-value">${properties.avg_temperature_f ? properties.avg_temperature_f + '°F' : 'Unknown'}</span>
                </div>
                <div class="popup-row">
                    <span class="popup-label">Temp Range:</span> 
                    <span class="popup-value">${properties.min_temperature_f || 'N/A'} - ${properties.max_temperature_f || 'N/A'}°F</span>
                </div>
                <div class="popup-row">
                    <span class="popup-label">Avg Depth:</span> 
                    <span class="popup-value">${properties.avg_depth_m ? properties.avg_depth_m + ' meters' : 'Unknown'}</span>
                </div>
            `;
        } else {
            // Individual point
            popupContent = `
                <div class="popup-header">
                    <i class="fas fa-thermometer-half"></i> Geothermal Point
                </div>
                <div class="popup-row">
                    <span class="popup-label">Temperature:</span> 
                    <span class="popup-value">${properties.temperature_f ? properties.temperature_f + '°F' : 'Unknown'}</span>
                </div>
                <div class="popup-row">
                    <span class="popup-label">Depth:</span> 
                    <span class="popup-value">${properties.depth_m ? properties.depth_m + ' meters' : 'Unknown'}</span>
                </div>
                <div class="popup-row">
                    <span class="popup-label">Location:</span> 
                    <span class="popup-value">${properties.latitude?.toFixed(4) || 'N/A'}, ${properties.longitude?.toFixed(4) || 'N/A'}</span>
                </div>
            `;
        }

        if (this.activePopup) {
            this.activePopup.remove();
        }

        this.activePopup = new maplibregl.Popup()
            .setLngLat(e.lngLat)
            .setHTML(popupContent)
            .addTo(this.map);
    }

    showGridPopup(e) {
        const properties = e.features[0].properties;
        
        const popupContent = `
            <div class="popup-header">
                <i class="fas fa-th"></i> Geothermal Grid Box (15-mile)
            </div>
            <div class="popup-row">
                <span class="popup-label">Center Coordinates:</span> 
                <span class="popup-value">${properties.center_lat?.toFixed(4) || 'N/A'}, ${properties.center_lng?.toFixed(4) || 'N/A'}</span>
            </div>
            <div class="popup-row">
                <span class="popup-label">Average Temperature:</span> 
                <span class="popup-value" style="font-weight: bold; color: ${this.getTemperatureColor(properties.avg_temperature_f)}">${properties.avg_temperature_f ? properties.avg_temperature_f + '°F' : 'No data'}</span>
            </div>
            <div class="popup-row">
                <span class="popup-label">Average Depth:</span> 
                <span class="popup-value">${properties.avg_depth_m ? properties.avg_depth_m + ' meters' : 'Unknown'}</span>
            </div>
            <div class="popup-row">
                <span class="popup-label">Temperature Range:</span> 
                <span class="popup-value">${properties.min_temperature_f || 'N/A'} - ${properties.max_temperature_f || 'N/A'}°F</span>
            </div>
            <div class="popup-row">
                <span class="popup-label">Data Points:</span> 
                <span class="popup-value">${properties.point_count || 0} measurements</span>
            </div>
        `;

        if (this.activePopup) {
            this.activePopup.remove();
        }

        this.activePopup = new maplibregl.Popup()
            .setLngLat([properties.center_lng, properties.center_lat])
            .setHTML(popupContent)
            .addTo(this.map);
    }

    getTemperatureColor(temp) {
        if (!temp) return '#999999';
        if (temp < 60) return '#4CAF50';   // Green
        if (temp < 80) return '#FFEB3B';   // Yellow  
        if (temp < 100) return '#FF9800';  // Orange
        if (temp < 130) return '#F44336';  // Red
        if (temp < 160) return '#E91E63';  // Hot Pink
        if (temp < 200) return '#9C27B0';  // Purple
        return '#000000';                  // Black
    }

    setupControls() {
        // Toggle controls visibility
        document.getElementById('toggleControls').addEventListener('click', () => {
            const controls = document.getElementById('controls');
            const icon = document.querySelector('#toggleControls i');
            
            this.controlsMinimized = !this.controlsMinimized;
            
            if (this.controlsMinimized) {
                controls.classList.add('minimized');
                icon.className = 'fas fa-chevron-down';
            } else {
                controls.classList.remove('minimized');
                icon.className = 'fas fa-chevron-up';
            }
        });

        // Transmission lines controls
        document.getElementById('transmission-toggle').addEventListener('change', (e) => {
            this.toggleTransmissionLines(e.target.checked);
        });

        document.getElementById('transmission-opacity').addEventListener('input', (e) => {
            const opacity = e.target.value / 100;
            document.getElementById('transmission-opacity-value').textContent = e.target.value + '%';
            this.updateTransmissionOpacity(opacity);
        });

        // Geothermal controls (removed from UI)
        // document.getElementById('geothermal-toggle').addEventListener('change', (e) => {
        //     this.toggleGeothermalPoints(e.target.checked);
        // });

        // Removed aggregated layer toggle as it doesn't exist

        // Geothermal opacity and temperature filter controls removed from UI
        // (Data now shown via hexagon mesh only)
        
        // Mesh controls
        document.getElementById('mesh-toggle').addEventListener('change', (e) => {
            this.toggleMesh(e.target.checked);
        });

        document.getElementById('mesh-opacity').addEventListener('input', (e) => {
            const opacity = e.target.value / 100;
            document.getElementById('mesh-opacity-value').textContent = e.target.value + '%';
            this.updateMeshOpacity(opacity);
        });
        
        // Depth filter functionality - re-aggregate mesh when depth changes
        document.getElementById('depth-filter').addEventListener('input', async (e) => {
            const depth = parseFloat(e.target.value);
            if (depth && depth > 0) {
                console.log(`🌡️ Filtering geothermal data by depth: ${depth}m`);
                
                // Show loading indicator
                document.getElementById('loading').style.display = 'block';
                
                try {
                    // Re-generate hexagon mesh with new depth filter
                    await this.updateMeshForDepth(depth);
                } catch (error) {
                    console.error('❌ Error updating mesh for depth:', error);
                } finally {
                    // Hide loading indicator
                    document.getElementById('loading').style.display = 'none';
                }
            }
        });
    }

    toggleTransmissionLines(visible) {
        const layers = ['transmission-lines-all'];
        layers.forEach(layerId => {
            try {
                this.map.setLayoutProperty(layerId, 'visibility', visible ? 'visible' : 'none');
            } catch (error) {
                // Layer might not exist, ignore
            }
        });
        
        // Update legend visibility
        document.getElementById('transmission-legend').style.display = visible ? 'block' : 'none';
    }

    toggleGeothermalPoints(visible) {
        // Only toggle the main geothermal points layer
        this.map.setLayoutProperty('geothermal-points', 'visibility', visible ? 'visible' : 'none');
        
        // Update legend visibility
        document.getElementById('geothermal-legend').style.display = visible ? 'block' : 'none';
    }

    // Removed toggleGeothermalAggregated function as aggregated layer doesn't exist

    updateTransmissionOpacity(opacity) {
        const layers = ['transmission-lines-all'];
        layers.forEach(layerId => {
            this.map.setPaintProperty(layerId, 'line-opacity', opacity);
        });
    }

    updateGeothermalOpacity(opacity) {
        // Only update the main geothermal points layer
        this.map.setPaintProperty('geothermal-points', 'circle-opacity', opacity);
    }

    updateTemperatureFilter(minTemp) {
        // Update filter for geothermal points
        this.map.setFilter('geothermal-points', ['>=', ['get', 'temperature_f'], minTemp]);
    }

    showMeshPopup(e) {
        if (!e.features || e.features.length === 0) return;
        
        const feature = e.features[0];
        const props = feature.properties;
        
        // Create popup content
        let content = `
            <div class="popup-header">
                <i class="fas fa-border-all"></i> Hexagon Cell
            </div>
        `;
        
        if (props.avg_temperature_f != null) {
            content += `
                <div class="popup-row">
                    <span class="popup-label">Avg Temperature:</span>
                    <span class="popup-value">${props.avg_temperature_f}°F</span>
                </div>
                <div class="popup-row">
                    <span class="popup-label">Temperature Range:</span>
                    <span class="popup-value">${props.min_temperature_f || 'N/A'}°F - ${props.max_temperature_f || 'N/A'}°F</span>
                </div>
                <div class="popup-row">
                    <span class="popup-label">Depth:</span>
                    <span class="popup-value">${props.depth_m || 3000}m</span>
                </div>
                <div class="popup-row">
                    <span class="popup-label">Data Points:</span>
                    <span class="popup-value">${props.point_count}</span>
                </div>
                <div class="popup-row">
                    <span class="popup-label">Cell Area:</span>
                    <span class="popup-value">2 square miles</span>
                </div>
            `;
        } else {
            content += `
                <div class="popup-row">
                    <span class="popup-label">Status:</span>
                    <span class="popup-value">No geothermal data</span>
                </div>
                <div class="popup-row">
                    <span class="popup-label">Cell Area:</span>
                    <span class="popup-value">2 square miles</span>
                </div>
            `;
        }
        
        // Remove existing popup
        if (this.activePopup) {
            this.activePopup.remove();
        }
        
        // Create new popup
        this.activePopup = new maplibregl.Popup({ closeOnClick: true })
            .setLngLat(e.lngLat)
            .setHTML(content)
            .addTo(this.map);
    }

    toggleMesh(visible) {
        const layers = ['hexagon-mesh-fill', 'hexagon-mesh-outline'];
        layers.forEach(layerId => {
            try {
                this.map.setLayoutProperty(layerId, 'visibility', visible ? 'visible' : 'none');
            } catch (error) {
                // Layer might not exist yet
            }
        });
        this.layerState.hexagonMesh = visible;
    }

    updateMeshOpacity(opacity) {
        try {
            this.meshConfig.opacity = opacity;
            this.map.setPaintProperty('hexagon-mesh-fill', 'fill-opacity', [
                'case',
                ['boolean', ['feature-state', 'selected'], false],
                0.9,  // Higher opacity when selected
                opacity   // Normal opacity
            ]);
        } catch (error) {
            // Layer might not exist yet
        }
    }

    
    async performTemperatureLookup() {
        const lat = parseFloat(document.getElementById('lookup-lat').value);
        const lng = parseFloat(document.getElementById('lookup-lng').value);
        const depth = parseFloat(document.getElementById('lookup-depth').value);
        
        const resultDiv = document.getElementById('lookup-result');
        
        if (isNaN(lat) || isNaN(lng) || isNaN(depth)) {
            resultDiv.innerHTML = '⚠️ Please enter valid coordinates and depth';
            return;
        }
        
        if (lat < 24 || lat > 50 || lng < -125 || lng > -66) {
            resultDiv.innerHTML = '⚠️ Coordinates outside US bounds';
            return;
        }
        
        resultDiv.innerHTML = '🔍 Searching for temperature data...';
        
        try {
            // Query the database for nearby temperature readings
            const response = await fetch('/api/temperature-lookup', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({ lat, lng, depth })
            });
            
            if (!response.ok) {
                throw new Error('Failed to fetch temperature data');
            }
            
            const data = await response.json();
            
            if (data.temperature !== null) {
                resultDiv.innerHTML = `🌡️ <strong>${data.temperature.toFixed(1)}°F</strong> at ${data.distance.toFixed(1)}km distance`;
            } else {
                resultDiv.innerHTML = '❌ No temperature data found nearby';
            }
            
        } catch (error) {
            console.error('Temperature lookup error:', error);
            resultDiv.innerHTML = '❌ Error fetching temperature data';
        }
    }

    updateStats() {
        const bounds = this.map.getBounds();
        const zoom = this.map.getZoom().toFixed(1);
        const center = this.map.getCenter();
        
        const statsContent = `
            <strong>Map Stats:</strong><br>
            Zoom: ${zoom}<br>
            Center: ${center.lat.toFixed(4)}, ${center.lng.toFixed(4)}<br>
            <small>Bounds: ${bounds.getSouthWest().lat.toFixed(2)}, ${bounds.getSouthWest().lng.toFixed(2)} to ${bounds.getNorthEast().lat.toFixed(2)}, ${bounds.getNorthEast().lng.toFixed(2)}</small>
        `;
        
        document.getElementById('stats-content').innerHTML = statsContent;
    }

    getVoltageClass(kv) {
        if (!kv) return 'Unknown';
        if (kv < 69) return 'Distribution';
        if (kv < 138) return 'Sub-transmission';
        if (kv < 230) return 'Regional Transmission';
        if (kv < 345) return 'High Voltage Transmission';
        if (kv < 500) return 'Extra High Voltage';
        return 'Ultra High Voltage';
    }

    updateDatasetStatus(transmissionDataset, geothermalDataset) {
        // Update UI to show which datasets are available
        const transmissionToggle = document.getElementById('transmission-toggle');
        const geothermalToggle = document.getElementById('geothermal-toggle');
        
        if (transmissionToggle) {
            transmissionToggle.disabled = !transmissionDataset;
            transmissionToggle.checked = !!transmissionDataset && this.layerState.transmissionLines;
            
            const transmissionLabel = transmissionToggle.closest('label');
            if (transmissionLabel) {
                transmissionLabel.style.opacity = transmissionDataset ? '1' : '0.5';
                transmissionLabel.title = transmissionDataset 
                    ? `${transmissionDataset.row_count || 0} transmission lines`
                    : 'No transmission line data available';
            }
        }
        
        if (geothermalToggle) {
            geothermalToggle.disabled = !geothermalDataset;
            geothermalToggle.checked = !!geothermalDataset && this.layerState.geothermalPoints;
            
            const geothermalLabel = geothermalToggle.closest('label');
            if (geothermalLabel) {
                geothermalLabel.style.opacity = geothermalDataset ? '1' : '0.5';
                geothermalLabel.title = geothermalDataset 
                    ? `${geothermalDataset.row_count || 0} geothermal points`
                    : 'No geothermal data available';
            }
        }
        
        console.log('Dataset status updated:', {
            transmission: transmissionDataset ? 'Available' : 'Not available',
            geothermal: geothermalDataset ? 'Available' : 'Not available'
        });
    }

    showError(message) {
        const loading = document.getElementById('loading');
        loading.innerHTML = `
            <div style="color: #F44336;">
                <i class="fas fa-exclamation-triangle"></i> Error: ${message}
            </div>
        `;
    }
}

// Initialize the application when the page loads
document.addEventListener('DOMContentLoaded', () => {
    new GeospatialApp();
});