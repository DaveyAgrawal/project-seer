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
            transmissionLines: false,
            geothermalPoints: false,
            hexagonMesh: false,
            energynetParcels: false,
            datacenters: false,
            ccusSites: false,
            ccusSaline: false,
            ccusEOR: false,
            ccusUtilization: false,
            ccusOther: false,
            emitters: false,
            emitterPowerPlants: false,
            emitterPetroleum: false,
            emitterWaste: false,
            emitterChemicals: false,
            emitterMinerals: false,
            emitterMetals: false,
            emitterOther: false,
            emitterMinEmissions: 0,
            ethanolPlants: false,
            optimalSites: false,
            geology: false
        };
        
        // Mesh configuration (65 square miles per hexagon)
        this.meshConfig = {
            size: 5,     // miles radius - back to working size
            opacity: 0.7,
            selectedHexId: null
        };
        
        // Cache for parsed tile data to improve performance
        this.tileCache = new Map();
        
        // Universal data cache system
        this.dataCache = {
            metadata: null,
            geothermal: null,
            transmission: null,
            energynet: null,
            datacenters: null,
            ccus: null,
            emitters: null,
            ethanol: null
        };

        // Track data source types for proper layer styling
        this.dataSourceTypes = {
            transmission: 'vector', // 'vector' or 'geojson'
            geothermal: 'api',      // 'api' or 'geojson'
            energynet: 'api',       // 'api' for EnergyNet parcels
            datacenters: 'api'      // 'api' for datacenters
        };
        
        this.init();
    }

    async init() {
        try {
            // Load cache metadata first
            await this.loadCacheMetadata();
            
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

    // Universal Data Cache Management System
    async loadCacheMetadata() {
        try {
            const response = await fetch('/cache/metadata.json');
            if (response.ok) {
                this.dataCache.metadata = await response.json();
                return this.dataCache.metadata;
            }
        } catch (error) {
            console.warn('⚠️ Could not load cache metadata:', error);
        }
        return null;
    }

    async checkDataCacheValidity(dataSource) {
        const metadata = this.dataCache.metadata;
        if (!metadata || !metadata.sources[dataSource]) return false;
        
        const sourceInfo = metadata.sources[dataSource];
        return sourceInfo.cached && sourceInfo.dataVersion;
    }

    async loadCachedData(dataSource, subType = null) {
        try {
            let cachePath;
            if (dataSource === 'geothermal') {
                const depth = subType || this.dataCache.metadata?.sources?.geothermal?.defaultDepth || 3000;
                cachePath = `/cache/geothermal/mesh-${depth}m.json`;
            } else if (dataSource === 'transmission') {
                cachePath = '/cache/transmission/lines.json';
            } else {
                throw new Error(`Unknown data source: ${dataSource}`);
            }

            const response = await fetch(cachePath);
            
            if (response.ok) {
                const cachedData = await response.json();
                this.dataCache[dataSource] = cachedData;
                console.log(`✅ Loaded cached ${dataSource} data:`, cachedData.metadata || 'no metadata');
                return cachedData;
            } else {
                console.warn(`⚠️ Cache file not found: ${cachePath}`);
                return null;
            }
        } catch (error) {
            console.error(`❌ Error loading cached ${dataSource} data:`, error);
            return null;
        }
    }

    async saveCachedData(dataSource, data, subType = null) {
        try {
            console.log(`💾 Saving ${dataSource} data to cache...`, {
                features: data.features?.length || 'N/A',
                size: JSON.stringify(data).length + ' bytes',
                subType: subType
            });
            
            const response = await fetch(`/api/save-cache/${dataSource}`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    data: data,
                    subType: subType
                })
            });
            
            if (response.ok) {
                const result = await response.json();
                console.log(`✅ Successfully saved ${dataSource} cache:`, result);
                
                // Reload metadata to reflect the new cache
                await this.loadCacheMetadata();
                
                return data;
            } else {
                console.error(`❌ Failed to save ${dataSource} cache:`, response.statusText);
                return data;
            }
            
        } catch (error) {
            console.error(`❌ Error saving ${dataSource} cache:`, error);
            return data;
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
        this.map.on('load', async () => {
            // Load custom marker icons
            await this.loadCustomIcons();
            
            await this.addDataSources();
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

    // Load custom marker icons (diamond, square, triangle)
    async loadCustomIcons() {
        const icons = [
            { name: 'diamond', path: '/icons/diamond.svg' },
            { name: 'diamond-green', path: '/icons/diamond-green.svg' },
            { name: 'diamond-orange', path: '/icons/diamond-orange.svg' },
            { name: 'diamond-purple', path: '/icons/diamond-purple.svg' },
            { name: 'diamond-blue', path: '/icons/diamond-blue.svg' },
            { name: 'diamond-gray', path: '/icons/diamond-gray.svg' },
            { name: 'square', path: '/icons/square.svg' },
            { name: 'triangle', path: '/icons/triangle.svg' },
            { name: 'square-cluster', path: '/icons/square-cluster.svg' },
            { name: 'triangle-cluster', path: '/icons/triangle-cluster.svg' }
        ];
        
        for (const icon of icons) {
            try {
                const img = new Image(24, 24);
                img.src = icon.path;
                await new Promise((resolve, reject) => {
                    img.onload = () => {
                        if (!this.map.hasImage(icon.name)) {
                            this.map.addImage(icon.name, img);
                            console.log(`✅ Loaded icon: ${icon.name}`);
                        }
                        resolve();
                    };
                    img.onerror = reject;
                });
            } catch (error) {
                console.error(`❌ Failed to load icon ${icon.name}:`, error);
            }
        }
    }

    async addDataSources() {
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
        
        // Load transmission lines from static GeoJSON file (works in production)
        try {
            console.log('📡 Loading transmission lines from static file...');
            const response = await fetch('/data/transmission_lines.geojson');
            if (response.ok) {
                const transmissionData = await response.json();
                this.map.addSource('transmission-lines', {
                    type: 'geojson',
                    data: transmissionData
                });
                this.dataSourceTypes.transmission = 'geojson';
                console.log('⚡ Successfully loaded transmission lines from static file!');
            } else {
                console.warn('⚠️ Failed to load transmission lines static file');
            }
        } catch (error) {
            console.error('❌ Error loading transmission lines:', error);
        }
        
        // Geothermal data now processed via sectioned aggregation (no source needed)
        
        // Update UI to show dataset status
        this.updateDatasetStatus(transmissionDataset, geothermalDataset);
    }

    addTransmissionVectorSource(transmissionDataset, tileserverUrl) {
        this.map.addSource('transmission-lines', {
            type: 'vector',
            tiles: [`${tileserverUrl}/public.${transmissionDataset.table_name}_us/{z}/{x}/{y}.mvt`],
            minzoom: 4,
            maxzoom: 13
        });
        console.log('Added transmission lines vector source (fallback)');
    }

    addDataLayers() {
        // Add transmission lines layers (single layer for all zoom levels 4-12)
        this.addTransmissionLinesLayer('transmission-lines-all', 'transmission-lines', 4, 13);
        
        // Set initial layer visibility based on state
        this.toggleTransmissionLines(this.layerState.transmissionLines);
        
        // Add hexagon mesh layer FIRST (so it's at the bottom, under all point layers)
        console.log('🕒 Starting hexagon mesh creation with sectioned geothermal data...');
        this.addHexagonMeshLayer();

        // Initialize comparison layers (Conventional @ 4km vs CO₂-EGS @ 5km)
        console.log('🔄 Initializing EGS comparison layers...');
        this.initComparisonLayers();

        // Add EnergyNet parcels layer
        console.log('🏞️ Adding EnergyNet parcels layer...');
        this.addEnergyNetParcelsLayer();

        // Add Datacenter facilities layer (on top of hexagon mesh)
        console.log('🏢 Adding Datacenter facilities layer...');
        this.addDatacenterLayer();

        // Add CCUS sites layer
        console.log('🏭 Adding CCUS sites layer...');
        this.addCCUSLayer();

        // Add Point Source Emitters layer
        console.log('🏭 Adding Point Source Emitters layer...');
        this.addEmittersLayer();

        // Add Ethanol Plants layer
        console.log('🌽 Adding Ethanol Plants layer...');
        this.addEthanolLayer();

        // Add Geology layer
        console.log('🪨 Adding Geology layer...');
        this.addGeologyLayer();

        // Add Optimal Sites layer
        console.log('🎯 Adding Optimal Sites layer...');
        this.addOptimalSitesLayer();

        // Ensure proper layer ordering (hexagon mesh at bottom, points on top)
        setTimeout(() => this.reorderLayers(), 2000);
        
    }

    // Look up geothermal data for a given coordinate by finding the containing hexagon
    // If no hexagon contains the point, find the nearest hexagon with data
    getGeothermalAtLocation(lng, lat) {
        try {
            const source = this.map.getSource('hexagon-mesh');
            if (!source || !source._data) return null;
            
            const hexData = source._data;
            if (!hexData || !hexData.features) return null;
            
            // First, try to find the hexagon that contains this point
            for (const hex of hexData.features) {
                if (!hex.geometry || !hex.geometry.coordinates) continue;
                
                // Check if point is inside polygon using ray casting
                const coords = hex.geometry.coordinates[0]; // Outer ring
                if (this.pointInPolygon([lng, lat], coords)) {
                    // If this hexagon has data, return it
                    if (hex.properties && hex.properties.avg_temperature_f !== null) {
                        return { ...hex.properties, isNearest: false };
                    }
                    // Otherwise, fall through to find nearest with data
                    break;
                }
            }
            
            // No hexagon contains the point or the containing hexagon has no data
            // Find the nearest hexagon WITH geothermal data
            let nearestHex = null;
            let minDistance = Infinity;
            
            for (const hex of hexData.features) {
                if (!hex.properties || hex.properties.avg_temperature_f === null) continue;
                if (!hex.geometry || !hex.geometry.coordinates) continue;
                
                // Calculate centroid of hexagon
                const coords = hex.geometry.coordinates[0];
                let centroidLng = 0, centroidLat = 0;
                for (let i = 0; i < coords.length - 1; i++) {
                    centroidLng += coords[i][0];
                    centroidLat += coords[i][1];
                }
                centroidLng /= (coords.length - 1);
                centroidLat /= (coords.length - 1);
                
                // Calculate distance (simple Euclidean, good enough for nearby points)
                const dist = Math.sqrt(Math.pow(lng - centroidLng, 2) + Math.pow(lat - centroidLat, 2));
                
                if (dist < minDistance) {
                    minDistance = dist;
                    nearestHex = hex;
                }
            }
            
            if (nearestHex) {
                // Convert distance to approximate miles (1 degree ≈ 69 miles at equator)
                const distMiles = Math.round(minDistance * 69);
                return { 
                    ...nearestHex.properties, 
                    isNearest: true, 
                    distanceMiles: distMiles 
                };
            }
            
            return null;
        } catch (error) {
            console.error('Error looking up geothermal data:', error);
            return null;
        }
    }

    // Ray casting algorithm to check if point is in polygon
    pointInPolygon(point, polygon) {
        const x = point[0], y = point[1];
        let inside = false;
        
        for (let i = 0, j = polygon.length - 1; i < polygon.length; j = i++) {
            const xi = polygon[i][0], yi = polygon[i][1];
            const xj = polygon[j][0], yj = polygon[j][1];
            
            if (((yi > y) !== (yj > y)) && (x < (xj - xi) * (y - yi) / (yj - yi) + xi)) {
                inside = !inside;
            }
        }
        return inside;
    }

    // Convert Fahrenheit to Celsius
    fToC(fahrenheit) {
        if (fahrenheit === null || fahrenheit === undefined) return null;
        return Math.round(((fahrenheit - 32) * 5 / 9) * 10) / 10;
    }

    // Generate geothermal info HTML for popups
    getGeothermalInfoHTML(lng, lat) {
        const geoData = this.getGeothermalAtLocation(lng, lat);
        const currentDepth = document.getElementById('depth-filter')?.value || 3000;
        
        if (!geoData || geoData.avg_temperature_f === null) {
            return `
                <div style="margin-top: 10px; padding: 10px; background: #F5F5F5; border-radius: 4px; border-left: 4px solid #9E9E9E;">
                    <div style="font-weight: bold; color: #666; margin-bottom: 6px;">🌡️ Geothermal Resource</div>
                    <div style="color: #999; font-size: 12px;">No geothermal data available at this location</div>
                </div>
            `;
        }
        
        const tempColor = this.getTemperatureColor(geoData.avg_temperature_f);
        const avgTempC = this.fToC(geoData.avg_temperature_f);
        const minTempC = this.fToC(geoData.min_temperature_f);
        const maxTempC = this.fToC(geoData.max_temperature_f);
        
        // Show indicator if this is from the nearest hexagon rather than the containing one
        const nearestIndicator = geoData.isNearest 
            ? `<div style="font-size: 11px; color: #FF6F00; margin-bottom: 6px; font-style: italic;">📍 Nearest data (~${geoData.distanceMiles} miles away)</div>` 
            : '';
        
        return `
            <div style="margin-top: 10px; padding: 10px; background: linear-gradient(135deg, #FFF3E0, #FFECB3); border-radius: 4px; border-left: 4px solid ${tempColor};">
                <div style="font-weight: bold; color: #E65100; margin-bottom: 8px;">🌡️ Geothermal Resource (${currentDepth}m depth)</div>
                ${nearestIndicator}
                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 4px; font-size: 12px;">
                    <div><span style="color: #666;">Avg Temp:</span> <span style="font-weight: bold; color: ${tempColor};">${avgTempC}°C</span></div>
                    <div><span style="color: #666;">Data Points:</span> <span style="font-weight: bold;">${geoData.point_count || 'N/A'}</span></div>
                    ${minTempC !== null ? `<div><span style="color: #666;">Min:</span> ${minTempC}°C</div>` : ''}
                    ${maxTempC !== null ? `<div><span style="color: #666;">Max:</span> ${maxTempC}°C</div>` : ''}
                    ${geoData.avg_depth_m ? `<div><span style="color: #666;">Avg Depth:</span> ${Math.round(geoData.avg_depth_m)}m</div>` : ''}
                </div>
            </div>
        `;
    }

    reorderLayers() {
        console.log('🔄 Reordering layers...');
        
        // Mesh layers should be at the bottom (rendered first)
        const meshLayers = [
            'geology-fill',             // Geology at very bottom
            'hexagon-mesh-fill',
            'hexagon-mesh-outline',
            'compare-co2egs-fill',      // CO₂-EGS comparison (5km) - bottom
            'compare-co2egs-outline',
            'compare-conventional-fill', // Conventional comparison (4km) - on top of CO₂-EGS
            'compare-conventional-outline',
            'co2egs-fill',
            'co2egs-outline'
        ];
        
        const pointLayers = [
            'energynet-pins',
            'energynet-unclustered-pins',
            'datacenter-points',
            'ccus-points',
            'emitter-points',
            'emitter-no-data',
            'ethanol-points',
            'ethanol-icons',
            'optimal-sites-points',
            'optimal-sites-labels'
        ];
        
        // Move each point layer to the top in order
        for (const layerId of pointLayers) {
            try {
                if (this.map.getLayer(layerId)) {
                    this.map.moveLayer(layerId);
                }
            } catch (e) {
                // Layer might not exist yet
            }
        }
    }

    addTransmissionLinesLayer(layerId, sourceId, minZoom, maxZoom) {
        console.log(`🔌 Adding transmission layer: ${layerId} (zoom ${minZoom}-${maxZoom})`);
        
        // Create layer configuration based on source type
        const layerConfig = {
            id: layerId,
            type: 'line',
            source: sourceId,
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
        };
        
        // Only add source-layer for vector tiles (not for GeoJSON)
        if (this.dataSourceTypes.transmission === 'vector') {
            layerConfig['source-layer'] = 'public.transmission_lines_us';
        }
        
        this.map.addLayer(layerConfig);
        
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
            
            // Filter empty hexagons using real US boundary data
            const filteredGrid = await this.filterHexagonsToUSBoundary(hexGrid, hexesWithData.length);
            
            return filteredGrid;
            
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
    
    async filterHexagonsToUSBoundary(hexGrid, dataHexagonCount) {
        try {
            console.log(`🗺️ Loading US boundary data for precise trimming...`);
            
            // Load US boundary as GeoJSON (simpler approach)
            const response = await fetch('https://raw.githubusercontent.com/holtzy/D3-graph-gallery/master/DATA/world.geojson');
            if (!response.ok) {
                console.warn('⚠️ Could not load boundary data, using simplified filtering');
                return this.filterHexagonsSimplified(hexGrid, dataHexagonCount);
            }
            
            const worldData = await response.json();
            
            // Extract USA from world data (or use a direct USA-only source)
            const usaFeature = worldData.features.find(feature => 
                feature.properties && (
                    feature.properties.name === 'United States of America' || 
                    feature.properties.NAME === 'United States of America' ||
                    feature.properties.name === 'USA' ||
                    feature.properties.NAME === 'USA'
                )
            );
            
            if (!usaFeature) {
                console.warn('⚠️ Could not find USA in boundary data, using simplified filtering');
                return this.filterHexagonsSimplified(hexGrid, dataHexagonCount);
            }
            
            const usBoundary = usaFeature;
            
                
            // Filter hexagons: keep all with data + empty ones within US boundaries
            const filteredFeatures = hexGrid.features.filter(hex => {
                // Always keep hexagons with geothermal data
                if (hex.properties && hex.properties.avg_temperature_f != null) {
                    return true;
                }
                
                // For empty hexagons, check if they intersect with US boundary
                try {
                    return turf.booleanIntersects(hex, usBoundary) || turf.booleanWithin(hex, usBoundary);
                } catch (error) {
                    // If intersection fails, keep the hexagon to be safe
                    return true;
                }
            });
            
            // Update grid with filtered features
            const originalCount = hexGrid.features.length;
            hexGrid.features = filteredFeatures;
            const removedCount = originalCount - filteredFeatures.length;
            
            console.log(`🇺🇸 Trimmed to US boundaries: kept ${filteredFeatures.length} hexagons, removed ${removedCount} ocean/border hexagons`);
            
            return hexGrid;
            
        } catch (error) {
            console.error('❌ Error filtering hexagons to US boundary:', error);
            console.warn('⚠️ Falling back to original grid without boundary filtering');
            return hexGrid;
        }
    }
    
    async filterHexagonsSimplified(hexGrid, dataHexagonCount) {
        // Simplified fallback filtering (similar to our earlier approach but more conservative)
        
        const filteredFeatures = hexGrid.features.filter(hex => {
            // Always keep hexagons with geothermal data
            if (hex.properties && hex.properties.avg_temperature_f != null) {
                return true;
            }
            
            // For empty hexagons, use conservative coordinate checks
            const center = turf.centroid(hex);
            const [lng, lat] = center.geometry.coordinates;
            
            // Very conservative exclusion of obvious ocean areas only
            const isObviouslyOcean = (
                (lng > -66.0 && lat > 25.0 && lat < 45.0) ||  // Atlantic
                (lng < -125.0 && lat > 32.0 && lat < 48.0) ||  // Pacific  
                (lng > -95.0 && lng < -80.0 && lat < 25.0)     // Deep Gulf
            );
            
            return !isObviouslyOcean;
        });
        
        const originalCount = hexGrid.features.length;
        hexGrid.features = filteredFeatures;
        const removedCount = originalCount - filteredFeatures.length;
        
        console.log(`🌊 Simplified filtering: kept ${filteredFeatures.length} hexagons, removed ${removedCount} obvious ocean hexagons`);
        return hexGrid;
    }
    
    async processSectionGeothermalData(hexagons, section) {
        // Query real geothermal data from tile server for this section
        
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
        // Query the working database API for geothermal data within section bounds
        try {
            const bounds = section.bounds; // [west, south, east, north]
            const depthTolerance = 50;
            
            console.log(`🗺️ Querying database API for ${section.name} bounds: [${bounds.join(', ')}]`);
            
            // Use our working database API endpoint
            const response = await fetch('/api/geothermal-tile-data', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    bounds: {
                        west: bounds[0],
                        south: bounds[1], 
                        east: bounds[2],
                        north: bounds[3]
                    },
                    depth: depthFilter,
                    depthTolerance: depthTolerance
                })
            });
            
            if (!response.ok) {
                throw new Error(`API request failed: ${response.status}`);
            }
            
            const data = await response.json();
            
            return data.points;
            
        } catch (error) {
            console.error(`❌ Error querying section geothermal data for ${section.name}:`, error);
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
    
    async parseRealMVTTile(tile, section, depthFilter) {
        // Get real geothermal data directly from the database via API
        try {
            const tileKey = `${tile.z}/${tile.x}/${tile.y}`;
            const cacheKey = `${tileKey}_${depthFilter}`;
            
            
            // Use direct database API instead of vector tiles
            return await this.fetchRealGeothermalData(tile, depthFilter);
            
        } catch (error) {
            const tileKey = `${tile.z}/${tile.x}/${tile.y}`;
            console.error(`❌ Error parsing MVT tile ${tileKey}:`, error);
            console.error('Error details:', error.stack);
            
            console.warn(`⚠️ Falling back to generated data for tile ${tileKey}`);
            return this.generateFallbackData(tile, depthFilter);
        }
    }
    
    async fetchRealGeothermalData(tile, depthFilter) {
        // Fetch real geothermal data directly from database API
        const tileKey = `${tile.z}/${tile.x}/${tile.y}`;
        const cacheKey = `${tileKey}_${depthFilter}`;
        
        // Check cache first
        if (this.tileCache.has(cacheKey)) {
            return this.tileCache.get(cacheKey);
        }
        
        try {
            // Get tile bounds for spatial filtering
            const tileBounds = this.getTileBounds(tile.x, tile.y, tile.z);
            const depthTolerance = 50; // ±50m depth tolerance
            
            // Create a direct database query
            const apiUrl = '/api/geothermal-tile-data';
            const response = await fetch(apiUrl, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    bounds: {
                        west: tileBounds.west,
                        east: tileBounds.east,
                        south: tileBounds.south,
                        north: tileBounds.north
                    },
                    depth: depthFilter,
                    depthTolerance: depthTolerance
                })
            });
            
            if (!response.ok) {
                console.warn(`⚠️ API request failed for tile ${tileKey}: ${response.status}`);
                return this.generateFallbackData(tile, depthFilter);
            }
            
            const data = await response.json();
            
            // Cache the results
            this.tileCache.set(cacheKey, data.points);
            
            return data.points;
            
        } catch (error) {
            console.error(`❌ Error fetching real data for tile ${tileKey}:`, error);
            return this.generateFallbackData(tile, depthFilter);
        }
    }
    
    generateFallbackData(tile, depthFilter) {
        // Generate dense, realistic geothermal data for proper mesh coverage
        const tileBounds = this.getTileBounds(tile.x, tile.y, tile.z);
        const points = [];
        
        // Create much more dense coverage - 50-100 points per tile for better mesh
        const numPoints = 75 + Math.floor(Math.random() * 25); // 75-100 points per tile
        
        // Calculate tile center for realistic geological patterns
        const centerLat = (tileBounds.north + tileBounds.south) / 2;
        const centerLng = (tileBounds.west + tileBounds.east) / 2;
        
        for (let i = 0; i < numPoints; i++) {
            const lng = tileBounds.west + Math.random() * (tileBounds.east - tileBounds.west);
            const lat = tileBounds.south + Math.random() * (tileBounds.north - tileBounds.south);
            
            // Create realistic geological temperature patterns
            // Simulate geological hotspots and gradients
            const distanceFromCenter = Math.sqrt(
                Math.pow((lat - centerLat) * 111, 2) + // ~111 km per degree lat
                Math.pow((lng - centerLng) * Math.cos(centerLat * Math.PI / 180) * 111, 2)
            );
            
            // Base temperature varies with location and depth
            let baseTemp = 150 + (depthFilter / 3000) * 80; // Deeper = warmer
            
            // Add geological variation - some areas are naturally hotter
            const geologicalHotspot1 = Math.exp(-Math.pow((lat - centerLat) * 400, 2) - Math.pow((lng - centerLng) * 400, 2)) * 60;
            const geologicalHotspot2 = Math.exp(-Math.pow((lat - (centerLat + 0.1)) * 300, 2) - Math.pow((lng - (centerLng - 0.1)) * 300, 2)) * 40;
            
            // Add regional geological patterns
            const regionalVariation = Math.sin(lat * 20) * Math.cos(lng * 15) * 25;
            
            // Random local variation
            const localVariation = (Math.random() - 0.5) * 30;
            
            const finalTemp = baseTemp + geologicalHotspot1 + geologicalHotspot2 + regionalVariation + localVariation;
            
            points.push({
                coordinates: [lng, lat],
                temperature_f: Math.max(150, Math.min(350, finalTemp)), // Clamp to realistic range
                depth_m: depthFilter + (Math.random() - 0.5) * 100, // ±50m depth variation
                latitude: lat,
                longitude: lng,
                gid: `fallback_${tile.x}_${tile.y}_${tile.z}_${i}`
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
    
    tileCoordinatesToLatLng(x, y, tileX, tileY, zoom) {
        // Convert tile pixel coordinates to geographic coordinates
        const tileSize = 4096; // MVT tile extent
        const worldSize = tileSize * Math.pow(2, zoom);
        
        const worldX = tileX * tileSize + x;
        const worldY = tileY * tileSize + y;
        
        const lng = (worldX / worldSize) * 360 - 180;
        const lat = Math.atan(Math.sinh(Math.PI * (1 - 2 * worldY / worldSize))) * 180 / Math.PI;
        
        return { lng, lat };
    }
    
    
    async updateMeshForDepth(newDepth) {
        console.log(`🔄 Updating hexagon mesh for depth: ${newDepth}m`);
        
        // Track if CO₂-EGS layer was visible before update
        const co2egsWasVisible = this.map.getLayer('co2egs-fill') && 
            this.map.getLayoutProperty('co2egs-fill', 'visibility') === 'visible';
        const currentViewMode = document.getElementById('co2egs-view-mode')?.value || 'all';
        const currentOpacity = this.map.getLayer('co2egs-fill') ? 
            this.map.getPaintProperty('co2egs-fill', 'fill-opacity') : 0.7;
        
        try {
            // Remove existing CO₂-EGS layers (they use the same source)
            if (this.map.getLayer('co2egs-fill')) {
                this.map.removeLayer('co2egs-fill');
            }
            if (this.map.getLayer('co2egs-outline')) {
                this.map.removeLayer('co2egs-outline');
            }
            
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
            
            // Try to load from cache first
            let hexGridWithData = null;
            if (await this.checkDataCacheValidity('geothermal')) {
                console.log(`💨 Attempting to load cached geothermal mesh for ${newDepth}m...`);
                hexGridWithData = await this.loadCachedData('geothermal', newDepth);
            }
            
            // Generate if no cache available
            if (!hexGridWithData) {
                console.log(`🔷 Re-generating hexagon grid for depth ${newDepth}m (no cache available)...`);
                const hexGrid = this.generateHexagonGrid(this.meshConfig.size);
                
                // Re-aggregate with new depth filter
                hexGridWithData = await this.aggregateGeothermalDataToHex(hexGrid);
            } else {
                console.log(`⚡ Successfully loaded geothermal mesh for ${newDepth}m from cache!`);
            }
            
            // Re-add mesh layers
            this.map.addSource('hexagon-mesh', {
                type: 'geojson',
                data: hexGridWithData
            });
            
            // Add fill layer - preserve current visibility state
            const meshWasVisible = this.layerState.hexagonMesh;
            this.map.addLayer({
                id: 'hexagon-mesh-fill',
                type: 'fill',
                source: 'hexagon-mesh',
                minzoom: 4,
                maxzoom: 13,
                layout: {
                    'visibility': meshWasVisible ? 'visible' : 'none'
                },
                paint: {
                    'fill-color': [
                        'case',
                        ['==', ['get', 'avg_temperature_f'], null], 'transparent',  // No data - transparent
                        ['<', ['get', 'avg_temperature_f'], 180], '#2196F3',        // Blue - Cool (150-180°F)
                        ['<', ['get', 'avg_temperature_f'], 220], '#4CAF50',        // Green - Moderate (180-220°F) 
                        ['<', ['get', 'avg_temperature_f'], 260], '#FFEB3B',        // Yellow - Warm (220-260°F)
                        ['<', ['get', 'avg_temperature_f'], 300], '#FF9800',        // Orange - Hot (260-300°F)
                        '#F44336'                                                   // Red - Very Hot (300°F+)
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
                    'visibility': meshWasVisible ? 'visible' : 'none'
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
            
            // Recreate CO₂-EGS layers with the new source data
            this.addCO2EGSLayer();
            
            // Restore CO₂-EGS visibility and settings
            if (co2egsWasVisible) {
                this.map.setLayoutProperty('co2egs-fill', 'visibility', 'visible');
                this.map.setLayoutProperty('co2egs-outline', 'visibility', 'visible');
                this.updateCO2EGSViewMode(currentViewMode);
                this.map.setPaintProperty('co2egs-fill', 'fill-opacity', currentOpacity);
            }
            
            // Save the newly generated mesh to cache
            await this.saveCachedData('geothermal', hexGridWithData, newDepth);
            
        } catch (error) {
            console.error('❌ Error updating hexagon mesh for depth:', error);
            throw error;
        }
    }

    async addEnergyNetParcelsLayer() {
        console.log('🏞️ Adding EnergyNet land parcels layer...');
        
        try {
            // Add source for EnergyNet parcels
            if (!this.map.getSource('energynet-parcels')) {
                this.map.addSource('energynet-parcels', {
                    type: 'geojson',
                    data: {
                        type: 'FeatureCollection',
                        features: []
                    }
                });
                console.log('✅ Added EnergyNet parcels source');
            }
            
            // Add fill layer for parcels
            this.map.addLayer({
                id: 'energynet-parcels-fill',
                type: 'fill',
                source: 'energynet-parcels',
                minzoom: 4,
                maxzoom: 18,
                paint: {
                    'fill-color': '#FF1493',    // Bright pink - highly visible
                    'fill-opacity': 0.7         // 70% opacity for better visibility
                }
            });
            
            // Add outline layer for parcels
            this.map.addLayer({
                id: 'energynet-parcels-outline',
                type: 'line',
                source: 'energynet-parcels',
                minzoom: 4,
                maxzoom: 18,
                paint: {
                    'line-color': '#0066CC',    // Bright blue for outline
                    'line-width': [
                        'interpolate',
                        ['linear'],
                        ['zoom'],
                        4, 1.0,   // Slightly thicker at low zoom for visibility
                        8, 1.5,   // Medium at mid zoom
                        14, 2.0   // Thicker at high zoom
                    ],
                    'line-opacity': 0.9
                }
            });
            
            console.log('✅ Successfully added EnergyNet parcels layers');
            
            // Add clustered pins layer for low zoom levels (0-7.5)
            await this.addEnergyNetPinsLayer();
            
            // Load parcel data
            await this.loadEnergyNetParcels();
            
            // Set initial visibility
            this.toggleEnergyNetParcels(this.layerState.energynetParcels);
            
            // Add click handlers for popup
            this.map.on('click', 'energynet-parcels-fill', (e) => {
                this.showEnergyNetParcelPopup(e);
            });
            
            // Add hover cursor
            this.map.on('mouseenter', 'energynet-parcels-fill', () => {
                this.map.getCanvas().style.cursor = 'pointer';
            });
            
            this.map.on('mouseleave', 'energynet-parcels-fill', () => {
                this.map.getCanvas().style.cursor = '';
            });
            
        } catch (error) {
            console.error('❌ Error adding EnergyNet parcels layer:', error);
        }
    }

    // Force refresh EnergyNet data (can be called from browser console)
    async forceRefreshEnergyNet() {
        console.log('🔄 Force refreshing EnergyNet data (no cache)...');
        
        // Reload data (always fresh)
        await this.loadEnergyNetParcels();
        
        // Make sure layers are visible if they should be
        if (this.layerState.energynetParcels) {
            this.toggleEnergyNetParcels(true);
        }
        
        // Debug layer status
        this.debugEnergyNetLayers();
    }
    
    // Debug function to check layer status
    debugEnergyNetLayers() {
        console.log('🔍 EnergyNet Debug Info:');
        const source = this.map.getSource('energynet-parcels');
        const fillLayer = this.map.getLayer('energynet-parcels-fill');
        const outlineLayer = this.map.getLayer('energynet-parcels-outline');
        
        console.log('📊 Source exists:', !!source);
        console.log('🎨 Fill layer exists:', !!fillLayer);
        console.log('🖊️ Outline layer exists:', !!outlineLayer);
        
        if (fillLayer) {
            const visibility = this.map.getLayoutProperty('energynet-parcels-fill', 'visibility');
            console.log('👁️ Fill layer visibility:', visibility || 'visible');
        }
        
        if (source && source._data) {
            console.log('📍 Source has data:', source._data.features?.length || 0, 'features');
        }
        
        console.log('🎛️ Layer state energynetParcels:', this.layerState.energynetParcels);
    }
    
    // Navigate to New Mexico parcels (can be called from browser console)
    goToNewMexicoParcels() {
        console.log('🗺️ Navigating to New Mexico EnergyNet parcels...');
        // Center coordinates for New Mexico parcels: approximately -104.0, 32.5
        this.map.flyTo({
            center: [-104.0, 32.5],
            zoom: 8,
            duration: 2000
        });
        
        setTimeout(() => {
            this.forceRefreshEnergyNet();
        }, 2500);
    }

    async loadEnergyNetParcels() {
        try {
            const zoom = this.map.getZoom();
            console.log(`📡 Loading EnergyNet parcels data for zoom level ${zoom.toFixed(1)} (no cache - always fresh)...`);
            
            const response = await fetch(`/api/energynet-parcels?zoom=${Math.floor(zoom)}`, {
                headers: {
                    'Cache-Control': 'no-cache',
                    'Pragma': 'no-cache'
                }
            });
            
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            
            const data = await response.json();
            console.log(`🏞️ Loaded ${data.features?.length || 0} EnergyNet parcels from view '${data.metadata?.view_used}' (fresh from API)`);
            
            // Log coordinate bounds for debugging
            if (data.features && data.features.length > 0) {
                const firstFeature = data.features[0];
                console.log(`🎯 First parcel coordinates:`, firstFeature.geometry.coordinates[0]);
                const bounds = this.calculateBounds(data.features);
                console.log(`🗺️ Parcel bounds: ${bounds.minLng.toFixed(3)}, ${bounds.minLat.toFixed(3)} to ${bounds.maxLng.toFixed(3)}, ${bounds.maxLat.toFixed(3)}`);
                
                // Check current map view
                const mapBounds = this.map.getBounds();
                console.log(`👁️ Current map view: ${mapBounds.getWest().toFixed(3)}, ${mapBounds.getSouth().toFixed(3)} to ${mapBounds.getEast().toFixed(3)}, ${mapBounds.getNorth().toFixed(3)}`);
            }
            
            // Update source data without removing (safer for layers)
            const source = this.map.getSource('energynet-parcels');
            if (source) {
                source.setData(data);
                console.log(`✅ Updated existing EnergyNet source with fresh data`);
            } else {
                // Add fresh source only if it doesn't exist
                this.map.addSource('energynet-parcels', {
                    type: 'geojson',
                    data: data
                });
                console.log(`✅ Added new EnergyNet parcels source`);
            }
            
            // Ensure layers are visible
            const isVisible = this.layerState.energynetParcels;
            console.log(`👁️ EnergyNet layer visibility state: ${isVisible}`);
            
        } catch (error) {
            console.error('❌ Error loading EnergyNet parcels:', error);
            
            // Show empty data on error
            const source = this.map.getSource('energynet-parcels');
            if (source) {
                source.setData({
                    type: 'FeatureCollection',
                    features: []
                });
            }
        }
    }

    calculateBounds(features) {
        let minLng = Infinity, minLat = Infinity, maxLng = -Infinity, maxLat = -Infinity;
        
        features.forEach(feature => {
            if (feature.geometry && feature.geometry.coordinates) {
                const coords = feature.geometry.coordinates[0]; // First ring of polygon
                coords.forEach(([lng, lat]) => {
                    minLng = Math.min(minLng, lng);
                    maxLng = Math.max(maxLng, lng);
                    minLat = Math.min(minLat, lat);
                    maxLat = Math.max(maxLat, lat);
                });
            }
        });
        
        return { minLng, minLat, maxLng, maxLat };
    }

    showEnergyNetParcelPopup(e) {
        const feature = e.features[0];
        const props = feature.properties;
        
        // Close any existing popup
        if (this.activePopup) {
            this.activePopup.remove();
        }
        
        // Create popup content matching EnergyNet.com format
        const popupContent = `
            <div class="popup-header">
                <i class="fas fa-map-marked-alt"></i> Land Parcel
            </div>
            <div class="popup-content">
                <div class="popup-row">
                    <span class="popup-label">Listing:</span> 
                    <span class="popup-value">${props.listing_id || 'Unknown'}</span>
                </div>
                <div class="popup-row">
                    <span class="popup-label">Parcel:</span> 
                    <span class="popup-value">${props.parcel_id || 'Unknown'}</span>
                </div>
                <div class="popup-row">
                    <span class="popup-label">State:</span> 
                    <span class="popup-value">${props.state || 'Unknown'}</span>
                </div>
                <div class="popup-row">
                    <span class="popup-label">Area:</span> 
                    <span class="popup-value">${props.acres ? `${props.acres.toLocaleString()} acres` : 'Unknown'}</span>
                </div>
                <div class="popup-row">
                    <span class="popup-label">Type:</span> 
                    <span class="popup-value">${props.description || 'Government Land Lease'}</span>
                </div>
            </div>
        `;
        
        // Create and show popup
        this.activePopup = new maplibregl.Popup({ closeOnClick: true })
            .setLngLat(e.lngLat)
            .setHTML(popupContent)
            .addTo(this.map);
    }

    toggleEnergyNetParcels(visible) {
        const fillLayerId = 'energynet-parcels-fill';
        const outlineLayerId = 'energynet-parcels-outline';
        
        // Pin layer IDs
        const clustersLayerId = 'energynet-clusters';
        const unclusteredLayerId = 'energynet-unclustered-pins';
        
        const visibility = visible ? 'visible' : 'none';
        
        // Toggle parcel layers (zoom 7.5+)
        if (this.map.getLayer(fillLayerId)) {
            this.map.setLayoutProperty(fillLayerId, 'visibility', visibility);
        }
        
        if (this.map.getLayer(outlineLayerId)) {
            this.map.setLayoutProperty(outlineLayerId, 'visibility', visibility);
        }
        
        // Toggle pin layers (zoom 0-7.5)
        if (this.map.getLayer(clustersLayerId)) {
            this.map.setLayoutProperty(clustersLayerId, 'visibility', visibility);
        }
        
        if (this.map.getLayer(unclusteredLayerId)) {
            this.map.setLayoutProperty(unclusteredLayerId, 'visibility', visibility);
        }
        
        this.layerState.energynetParcels = visible;
        console.log(`🏞️ EnergyNet parcels and pins visibility: ${visible ? 'ON' : 'OFF'}`);
    }

    async addHexagonMeshLayer() {
        console.log('🔷 Adding hexagon mesh layer...');
        
        try {
            // Try to load from cache first
            const currentDepth = parseFloat(document.getElementById('depth-filter')?.value) || 3000;
            let hexGridWithData = null;
            
            if (await this.checkDataCacheValidity('geothermal')) {
                console.log('💨 Attempting to load cached geothermal mesh...');
                hexGridWithData = await this.loadCachedData('geothermal', currentDepth);
            }
            
            // Fallback to generation if no cache or cache invalid
            if (!hexGridWithData) {
                console.log('🔧 Cache not available, generating fresh geothermal mesh...');
                
                // Generate hexagon grid
                const hexGrid = this.generateHexagonGrid(this.meshConfig.size);
                
                // Aggregate geothermal data to hexagons  
                hexGridWithData = await this.aggregateGeothermalDataToHex(hexGrid);
                
                // Save for future use (this will log the data for manual caching)
                await this.saveCachedData('geothermal', hexGridWithData, currentDepth);
            } else {
                console.log('⚡ Successfully loaded geothermal mesh from cache!');
            }
            
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
                    'visibility': 'none'  // Hidden by default - user must enable
                },
                paint: {
                    'fill-color': [
                        'case',
                        ['==', ['get', 'avg_temperature_f'], null], 'transparent',  // No data - transparent
                        ['<', ['get', 'avg_temperature_f'], 180], '#2196F3',        // Blue - Cool (150-180°F)
                        ['<', ['get', 'avg_temperature_f'], 220], '#4CAF50',        // Green - Moderate (180-220°F) 
                        ['<', ['get', 'avg_temperature_f'], 260], '#FFEB3B',        // Yellow - Warm (220-260°F)
                        ['<', ['get', 'avg_temperature_f'], 300], '#FF9800',        // Orange - Hot (260-300°F)
                        '#F44336'                                                   // Red - Very Hot (300°F+)
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
                    'visibility': 'none'  // Hidden by default - user must enable
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
            
            // Add CO₂-EGS Resource Potential layer (uses same source, different color scheme)
            this.addCO2EGSLayer();
            
        } catch (error) {
            console.error('❌ Error adding hexagon mesh layer:', error);
        }
    }

    // Add CO₂-EGS Resource Potential layer
    // Shows regions unlocked by CO₂-EGS vs conventional water-based EGS
    // CO₂-EGS viable at 150°C+, Conventional/Water-EGS viable at 200°C+
    addCO2EGSLayer() {
        console.log('🔥 Adding CO₂-EGS Resource Potential layer...');
        
        try {
            // Temperature thresholds in Fahrenheit (converted from Celsius)
            // 150°C = 302°F, 200°C = 392°F
            const CO2_EGS_MIN_F = 302;      // 150°C - minimum for CO₂-EGS viability
            const FERVO_THRESHOLD_F = 392;  // 200°C - Fervo/water-EGS commercial threshold (both viable above this)
            
            // Add CO₂-EGS fill layer - TWO ZONES:
            // Teal (150-200°C): CO₂-EGS only - unlocked resource
            // Amber (200°C+): Both viable - CO₂-EGS and conventional EGS
            this.map.addLayer({
                id: 'co2egs-fill',
                type: 'fill',
                source: 'hexagon-mesh',
                minzoom: 4,
                maxzoom: 13,
                layout: {
                    'visibility': 'none'  // Hidden by default
                },
                paint: {
                    'fill-color': [
                        'case',
                        // No data or below CO₂-EGS threshold - transparent
                        ['==', ['get', 'avg_temperature_f'], null], 'transparent',
                        ['<', ['get', 'avg_temperature_f'], CO2_EGS_MIN_F], 'transparent',
                        // All CO₂-EGS viable (150°C+) - Blue (default view mode)
                        '#00BCD4'
                    ],
                    'fill-opacity': 0.7
                }
            });
            
            // Add CO₂-EGS outline layer
            this.map.addLayer({
                id: 'co2egs-outline',
                type: 'line',
                source: 'hexagon-mesh',
                minzoom: 4,
                maxzoom: 13,
                layout: {
                    'visibility': 'none'  // Hidden by default
                },
                paint: {
                    'line-color': '#FFFFFF',
                    'line-width': 0.5,
                    'line-opacity': 0.5
                }
            });
            
            console.log('✅ CO₂-EGS Resource Potential layer added successfully');
            
        } catch (error) {
            console.error('❌ Error adding CO₂-EGS layer:', error);
        }
    }

    // Initialize comparison layers (Conventional @ 4km vs CO₂-EGS @ 5km)
    async initComparisonLayers() {
        console.log('🔄 Initializing EGS comparison layers...');
        
        // Add empty sources for comparison layers
        if (!this.map.getSource('compare-conventional')) {
            this.map.addSource('compare-conventional', {
                type: 'geojson',
                data: { type: 'FeatureCollection', features: [] }
            });
        }
        
        if (!this.map.getSource('compare-co2egs')) {
            this.map.addSource('compare-co2egs', {
                type: 'geojson',
                data: { type: 'FeatureCollection', features: [] }
            });
        }
        
        // Temperature thresholds in Fahrenheit
        const CO2_EGS_MIN_F = 302;      // 150°C
        const FERVO_THRESHOLD_F = 392;  // 200°C
        
        // Add CO₂-EGS layer FIRST (150°C+ at 5km) - Light Teal - renders at bottom
        this.map.addLayer({
            id: 'compare-co2egs-fill',
            type: 'fill',
            source: 'compare-co2egs',
            minzoom: 4,
            maxzoom: 13,
            layout: { 'visibility': 'none' },
            paint: {
                'fill-color': [
                    'case',
                    ['==', ['get', 'avg_temperature_f'], null], 'transparent',
                    ['<', ['get', 'avg_temperature_f'], CO2_EGS_MIN_F], 'transparent',
                    '#66BB6A'  // Light green for 150°C+
                ],
                'fill-opacity': 0.7
            }
        });
        
        this.map.addLayer({
            id: 'compare-co2egs-outline',
            type: 'line',
            source: 'compare-co2egs',
            minzoom: 4,
            maxzoom: 13,
            layout: { 'visibility': 'none' },
            paint: {
                'line-color': [
                    'case',
                    ['==', ['get', 'avg_temperature_f'], null], 'transparent',
                    ['<', ['get', 'avg_temperature_f'], CO2_EGS_MIN_F], 'transparent',
                    '#66BB6A'
                ],
                'line-width': 0.5,
                'line-opacity': 0.5
            }
        });
        
        // Add Conventional EGS layer SECOND (200°C+ at 4km) - Light Yellow - renders ON TOP
        this.map.addLayer({
            id: 'compare-conventional-fill',
            type: 'fill',
            source: 'compare-conventional',
            minzoom: 4,
            maxzoom: 13,
            layout: { 'visibility': 'none' },
            paint: {
                'fill-color': [
                    'case',
                    ['==', ['get', 'avg_temperature_f'], null], 'transparent',
                    ['<', ['get', 'avg_temperature_f'], FERVO_THRESHOLD_F], 'transparent',
                    '#FFD54F'  // Light yellow/amber for 200°C+
                ],
                'fill-opacity': 0.85
            }
        });
        
        this.map.addLayer({
            id: 'compare-conventional-outline',
            type: 'line',
            source: 'compare-conventional',
            minzoom: 4,
            maxzoom: 13,
            layout: { 'visibility': 'none' },
            paint: {
                'line-color': [
                    'case',
                    ['==', ['get', 'avg_temperature_f'], null], 'transparent',
                    ['<', ['get', 'avg_temperature_f'], FERVO_THRESHOLD_F], 'transparent',
                    '#FFD54F'
                ],
                'line-width': 0.5,
                'line-opacity': 0.5
            }
        });
        
        console.log('✅ Comparison layer structure initialized');
        
        // Add click handlers for comparison layers to show geothermal info
        this.map.on('click', 'compare-conventional-fill', (e) => {
            this.showComparisonPopup(e, 'conventional');
        });
        this.map.on('click', 'compare-co2egs-fill', (e) => {
            this.showComparisonPopup(e, 'co2egs');
        });
        
        // Change cursor on hover
        this.map.on('mouseenter', 'compare-conventional-fill', () => {
            this.map.getCanvas().style.cursor = 'pointer';
        });
        this.map.on('mouseleave', 'compare-conventional-fill', () => {
            this.map.getCanvas().style.cursor = '';
        });
        this.map.on('mouseenter', 'compare-co2egs-fill', () => {
            this.map.getCanvas().style.cursor = 'pointer';
        });
        this.map.on('mouseleave', 'compare-co2egs-fill', () => {
            this.map.getCanvas().style.cursor = '';
        });
    }

    // Show popup for comparison layer hexagon click
    showComparisonPopup(e, layerType) {
        const props = e.features[0].properties;
        const tempF = props.avg_temperature_f;
        const tempC = tempF ? ((tempF - 32) * 5/9).toFixed(1) : 'N/A';
        const basementDepth = props.basement_depth;
        
        // Get the depth from the layer that was clicked
        const depthInput = document.getElementById(`compare-${layerType}-depth`);
        const layerDepth = depthInput ? parseInt(depthInput.value) : (layerType === 'conventional' ? 4000 : 5000);
        
        let popupHTML = `
            <div class="popup-content" style="max-width: 250px;">
                <div class="popup-header" style="background: #FF5722; color: white; padding: 8px; margin: -10px -10px 10px -10px; border-radius: 4px 4px 0 0;">
                    <h3 style="margin: 0; font-size: 13px;">🌡️ Geothermal Resource</h3>
                </div>
                <div style="background: #f5f5f5; padding: 8px; border-radius: 4px;">
                    <div style="font-size: 14px; font-weight: bold; margin-bottom: 6px; color: #333;">
                        ${tempC}°C <span style="font-weight: normal; color: #666;">(${tempF ? tempF.toFixed(0) : 'N/A'}°F)</span>
                    </div>
                    <div style="font-size: 11px; color: #666;">
                        at ${layerDepth/1000}km depth
                    </div>
                    ${basementDepth ? `<div style="font-size: 11px; color: #666; margin-top: 4px;">Basement: ${basementDepth.toFixed(0)}m</div>` : ''}
                </div>
            </div>
        `;
        
        new maplibregl.Popup()
            .setLngLat(e.lngLat)
            .setHTML(popupHTML)
            .addTo(this.map);
    }

    // Load comparison layer data - tries cache first, then falls back to current mesh
    async loadComparisonLayerData(layerType) {
        // Get custom depth from input field
        const depthInput = document.getElementById(`compare-${layerType}-depth`);
        const depth = depthInput ? parseInt(depthInput.value) : (layerType === 'conventional' ? 4000 : 5000);
        const sourceId = `compare-${layerType}`;
        
        // Store the depth used for this layer
        this.comparisonSettings = this.comparisonSettings || {};
        this.comparisonSettings[layerType] = { depth };
        
        console.log(`📡 Loading ${layerType} at ${depth}m...`);
        document.getElementById('compare-stats').textContent = `Loading ${layerType} at ${depth}m...`;
        
        try {
            // Try to load from server cache first
            let hexData = null;
            try {
                hexData = await this.loadCachedData('geothermal', depth);
                if (hexData) {
                    console.log(`⚡ Loaded ${layerType} from cache at ${depth}m!`);
                }
            } catch (e) {
                console.log(`No cache for ${depth}m`);
            }
            
            // If no cache, use current mesh data as fallback
            // (User should set depth to 4000 or 5000 first)
            if (!hexData) {
                const meshSource = this.map.getSource('hexagon-mesh');
                if (meshSource && meshSource._data) {
                    hexData = JSON.parse(JSON.stringify(meshSource._data));
                    console.log(`📋 Using current mesh for ${layerType}`);
                }
            }
            
            if (hexData) {
                const source = this.map.getSource(sourceId);
                if (source) source.setData(hexData);
                this.dataCache[sourceId] = hexData;
                console.log(`✅ ${layerType} data loaded at ${depth}m`);
            }
            
            this.updateComparisonStats();
            
        } catch (error) {
            console.error(`Error loading ${layerType}:`, error);
            document.getElementById('compare-stats').textContent = `Error loading ${layerType}`;
        }
    }

    // Toggle comparison layer visibility
    async toggleComparisonLayer(layerType, visible) {
        const fillLayer = `compare-${layerType}-fill`;
        const outlineLayer = `compare-${layerType}-outline`;
        
        if (visible) {
            // Load this layer's data if not already loaded
            const sourceId = `compare-${layerType}`;
            if (!this.dataCache[sourceId]?.features?.length) {
                await this.loadComparisonLayerData(layerType);
            }
            
            // Update paint property with custom temperature threshold
            this.updateComparisonLayerPaint(layerType);
            
            // Apply basement depth filter
            const basementSlider = document.getElementById(`compare-${layerType}-basement`);
            if (basementSlider) {
                const maxBasementDepth = parseInt(basementSlider.value) || 6000;
                await this.applyComparisonBasementFilter(layerType, maxBasementDepth);
            }
            
            // Show layers
            if (this.map.getLayer(fillLayer)) {
                this.map.setLayoutProperty(fillLayer, 'visibility', 'visible');
            }
            if (this.map.getLayer(outlineLayer)) {
                this.map.setLayoutProperty(outlineLayer, 'visibility', 'visible');
            }
        } else {
            // Hide layers
            if (this.map.getLayer(fillLayer)) {
                this.map.setLayoutProperty(fillLayer, 'visibility', 'none');
            }
            if (this.map.getLayer(outlineLayer)) {
                this.map.setLayoutProperty(outlineLayer, 'visibility', 'none');
            }
        }
        
        this.updateComparisonStats();
        this.reorderLayers();
    }

    // Reload comparison layer with new parameters (temp/depth changed)
    async reloadComparisonLayer(layerType) {
        const sourceId = `compare-${layerType}`;
        
        // Clear cache to force reload
        delete this.dataCache[sourceId];
        
        // Reload data with new depth
        await this.loadComparisonLayerData(layerType);
        
        // Update the layer paint property with new temperature threshold
        this.updateComparisonLayerPaint(layerType);
    }

    // Update comparison layer paint property based on custom temperature
    updateComparisonLayerPaint(layerType) {
        const fillLayer = `compare-${layerType}-fill`;
        if (!this.map.getLayer(fillLayer)) return;
        
        // Get custom temperature from input (convert °C to °F)
        const tempInput = document.getElementById(`compare-${layerType}-temp`);
        const depthInput = document.getElementById(`compare-${layerType}-depth`);
        const tempC = tempInput ? parseInt(tempInput.value) : (layerType === 'conventional' ? 200 : 150);
        const depthM = depthInput ? parseInt(depthInput.value) : (layerType === 'conventional' ? 4000 : 5000);
        const tempF = (tempC * 9/5) + 32;
        
        // Set fill color based on temperature threshold
        const color = layerType === 'conventional' ? '#FFD54F' : '#81C784';
        
        this.map.setPaintProperty(fillLayer, 'fill-color', [
            'case',
            ['==', ['get', 'avg_temperature_f'], null], 'transparent',
            ['<', ['get', 'avg_temperature_f'], tempF], 'transparent',
            color
        ]);
        
        // Update legend text
        const legendEl = document.getElementById(`compare-legend-${layerType}`);
        if (legendEl) {
            const label = layerType === 'conventional' ? 'Conventional EGS' : 'CO₂-EGS';
            legendEl.textContent = `${label} (${tempC}°C+ @ ${depthM/1000}km)`;
        }
        
        console.log(`🎨 Updated ${layerType} layer paint: ${tempC}°C (${tempF}°F) @ ${depthM}m`);
    }

    // Apply basement depth filter to comparison layer
    async applyComparisonBasementFilter(layerType, maxBasementDepth) {
        const sourceId = `compare-${layerType}`;
        const fillLayer = `compare-${layerType}-fill`;
        
        if (!this.map.getLayer(fillLayer)) return;
        
        try {
            // Get geology data for depth lookup
            const geologySource = this.map.getSource('geology');
            if (!geologySource || !geologySource._data) {
                console.warn('⚠️ Geology data not loaded - cannot filter by basement depth');
                return;
            }
            
            const geologyData = geologySource._data;
            const compareSource = this.map.getSource(sourceId);
            if (!compareSource || !this.dataCache[sourceId]) {
                console.warn(`⚠️ ${layerType} comparison data not loaded`);
                return;
            }
            
            // Build spatial index from geology points
            const cellSize = 0.5;
            const geologyGrid = {};
            for (const feat of geologyData.features) {
                const [lon, lat] = feat.geometry.coordinates;
                const cellKey = `${Math.floor(lon / cellSize)},${Math.floor(lat / cellSize)}`;
                if (!geologyGrid[cellKey]) geologyGrid[cellKey] = [];
                geologyGrid[cellKey].push({
                    lon, lat,
                    depth: feat.properties.dt
                });
            }
            
            // For each hexagon, find nearest geology point and check depth
            const hexData = JSON.parse(JSON.stringify(this.dataCache[sourceId]));
            let filteredCount = 0;
            let totalCount = 0;
            
            for (const hex of hexData.features) {
                totalCount++;
                const coords = hex.geometry.coordinates[0];
                let centroidLon = 0, centroidLat = 0;
                for (const [lon, lat] of coords) {
                    centroidLon += lon;
                    centroidLat += lat;
                }
                centroidLon /= coords.length;
                centroidLat /= coords.length;
                
                // Look up depth in nearby cells
                const nearbyPoints = [];
                for (let dx = -1; dx <= 1; dx++) {
                    for (let dy = -1; dy <= 1; dy++) {
                        const key = `${Math.floor(centroidLon / cellSize) + dx},${Math.floor(centroidLat / cellSize) + dy}`;
                        if (geologyGrid[key]) nearbyPoints.push(...geologyGrid[key]);
                    }
                }
                
                // Find closest geology point
                let minDist = Infinity;
                let closestDepth = null;
                for (const pt of nearbyPoints) {
                    const dist = Math.sqrt((pt.lon - centroidLon) ** 2 + (pt.lat - centroidLat) ** 2);
                    if (dist < minDist) {
                        minDist = dist;
                        closestDepth = pt.depth;
                    }
                }
                
                hex.properties.basement_depth = closestDepth;
                if (closestDepth !== null && closestDepth > maxBasementDepth) {
                    hex.properties._filtered = true;
                    filteredCount++;
                } else {
                    hex.properties._filtered = false;
                }
            }
            
            // Update the source data
            compareSource.setData(hexData);
            
            // Apply filter
            this.map.setFilter(fillLayer, ['!=', ['get', '_filtered'], true]);
            this.map.setFilter(`compare-${layerType}-outline`, ['!=', ['get', '_filtered'], true]);
            
            console.log(`🎯 ${layerType} basement filter: max ${maxBasementDepth}m, filtered ${filteredCount}/${totalCount}`);
            
            this.updateComparisonStats();
        } catch (error) {
            console.error(`❌ Error applying basement filter to ${layerType}:`, error);
        }
    }

    // Update comparison stats
    updateComparisonStats() {
        const statsEl = document.getElementById('compare-stats');
        const conventionalChecked = document.getElementById('compare-conventional')?.checked;
        const co2egsChecked = document.getElementById('compare-co2egs')?.checked;
        
        if (!conventionalChecked && !co2egsChecked) {
            statsEl.textContent = 'Enable layers to see comparison stats';
            return;
        }
        
        // Get custom temperature thresholds from input fields (convert °C to °F)
        const conventionalTempC = parseInt(document.getElementById('compare-conventional-temp')?.value) || 200;
        const co2egsTempC = parseInt(document.getElementById('compare-co2egs-temp')?.value) || 150;
        const conventionalTempF = (conventionalTempC * 9/5) + 32;  // Convert to Fahrenheit
        const co2egsTempF = (co2egsTempC * 9/5) + 32;
        
        // Get depths for display
        const conventionalDepth = parseInt(document.getElementById('compare-conventional-depth')?.value) || 4000;
        const co2egsDepth = parseInt(document.getElementById('compare-co2egs-depth')?.value) || 5000;
        
        // Hexagon area: 5 mile radius = ~65 sq miles per hexagon
        // Formula: (3 * sqrt(3) / 2) * r^2 ≈ 2.598 * 25 ≈ 65 sq mi
        const hexAreaSqMiles = 65;
        const hexAreaAcres = hexAreaSqMiles * 640;  // 640 acres per sq mile = 41,600 acres per hex
        const plantSpacingSqMi = 100;
        const avgPlantMW = 20;
        const mwPerHexagon = (hexAreaSqMiles / plantSpacingSqMi) * avgPlantMW;  // ~13 MW per hex
        
        let conventionalCount = 0;
        let co2egsCount = 0;
        
        // Count conventional cells using custom temperature threshold
        if (this.dataCache['compare-conventional']?.features) {
            for (const hex of this.dataCache['compare-conventional'].features) {
                const temp = hex.properties?.avg_temperature_f;
                if (temp && temp >= conventionalTempF) {
                    conventionalCount++;
                }
            }
        }
        
        // Count CO₂-EGS cells using custom temperature threshold
        if (this.dataCache['compare-co2egs']?.features) {
            for (const hex of this.dataCache['compare-co2egs'].features) {
                const temp = hex.properties?.avg_temperature_f;
                if (temp && temp >= co2egsTempF) {
                    co2egsCount++;
                }
            }
        }
        
        console.log(`📊 Stats: Conventional=${conventionalCount} hexes, CO₂-EGS=${co2egsCount} hexes`);
        
        // Calculate areas and GW
        const conventionalAcres = conventionalCount * hexAreaAcres;
        const conventionalSqMiles = conventionalCount * hexAreaSqMiles;
        const co2egsAcres = co2egsCount * hexAreaAcres;
        const co2egsSqMiles = co2egsCount * hexAreaSqMiles;
        
        const conventionalGW = (conventionalCount * mwPerHexagon) / 1000;
        const co2egsGW = (co2egsCount * mwPerHexagon) / 1000;
        
        // CO₂-EGS only = total CO₂-EGS minus conventional overlap
        const co2egsOnlyCount = Math.max(0, co2egsCount - conventionalCount);
        const co2egsOnlyAcres = co2egsOnlyCount * hexAreaAcres;
        const co2egsOnlySqMiles = co2egsOnlyCount * hexAreaSqMiles;
        const additionalGW = (co2egsOnlyCount * mwPerHexagon) / 1000;
        
        // Format numbers with commas
        const fmt = (n) => n.toLocaleString();
        
        let statsHTML = '<div style="font-size: 10px; line-height: 1.5;">';
        
        if (conventionalChecked) {
            statsHTML += `<div style="color: #F9A825; margin-bottom: 4px;">
                <strong>⬛ Conventional EGS (${conventionalDepth/1000}km, ${conventionalTempC}°C+)</strong><br>
                ${fmt(conventionalCount)} cells · ${fmt(conventionalSqMiles)} sq mi<br>
                <strong style="font-size: 11px;">~${conventionalGW.toFixed(1)} GW potential</strong>
            </div>`;
        }
        if (co2egsChecked) {
            statsHTML += `<div style="color: #43A047; margin-bottom: 4px;">
                <strong>⬛ CO₂-EGS (${co2egsDepth/1000}km, ${co2egsTempC}°C+)</strong><br>
                ${fmt(co2egsCount)} cells · ${fmt(co2egsSqMiles)} sq mi<br>
                <strong style="font-size: 11px;">~${co2egsGW.toFixed(1)} GW potential</strong>
            </div>`;
        }
        if (conventionalChecked && co2egsChecked) {
            statsHTML += `<div style="margin-top: 6px; padding: 6px; background: #E8F5E9; border-radius: 4px; border-left: 3px solid #66BB6A;">
                <strong style="color: #43A047;">🌱 NEW Regions Unlocked by CO₂-EGS</strong><br>
                <span style="color: #333;">${fmt(co2egsOnlyCount)} cells · ${fmt(co2egsOnlySqMiles)} sq mi</span><br>
                <span style="color: #333;">${fmt(co2egsOnlyAcres)} acres</span><br>
                <strong style="color: #66BB6A; font-size: 12px;">~${additionalGW.toFixed(1)} GW additional capacity</strong>
            </div>`;
        }
        
        statsHTML += '<div style="margin-top: 6px; font-size: 9px; color: #999;">Based on 1× 20MW plant per 100 sq mi</div>';
        statsHTML += '</div>';
        statsEl.innerHTML = statsHTML;
    }

    // Toggle CO₂-EGS layer visibility and calculate stats
    toggleCO2EGS(visible) {
        const layers = ['co2egs-fill', 'co2egs-outline'];
        layers.forEach(layerId => {
            try {
                if (this.map.getLayer(layerId)) {
                    this.map.setLayoutProperty(layerId, 'visibility', visible ? 'visible' : 'none');
                }
            } catch (error) {
                // Layer might not exist yet
            }
        });
        
        if (visible) {
            this.calculateCO2EGSStats();
        } else {
            document.getElementById('co2egs-stats').textContent = 'Select depth and enable to view resource potential';
        }
        
        this.reorderLayers();
    }

    // Update CO₂-EGS layer opacity
    updateCO2EGSOpacity(opacity) {
        try {
            if (this.map.getLayer('co2egs-fill')) {
                this.map.setPaintProperty('co2egs-fill', 'fill-opacity', opacity);
            }
        } catch (error) {
            console.error('Error updating CO₂-EGS opacity:', error);
        }
    }

    // Update CO₂-EGS view mode (filter which zones are displayed)
    updateCO2EGSViewMode(mode) {
        if (!this.map.getLayer('co2egs-fill')) return;
        
        // Temperature thresholds in Fahrenheit
        const CO2_EGS_MIN_F = 302;      // 150°C
        const FERVO_THRESHOLD_F = 392;  // 200°C
        let fillColor;
        
        switch (mode) {
            case 'co2-all':
                // Show all CO₂-EGS viable (150°C+) in blue
                fillColor = [
                    'case',
                    ['==', ['get', 'avg_temperature_f'], null], 'transparent',
                    ['<', ['get', 'avg_temperature_f'], CO2_EGS_MIN_F], 'transparent',
                    '#00BCD4'  // Blue/teal for all CO₂-EGS viable (150°C+)
                ];
                break;
            case 'water-egs':
                // Show only water-based EGS viable (200°C+) in amber
                fillColor = [
                    'case',
                    ['==', ['get', 'avg_temperature_f'], null], 'transparent',
                    ['<', ['get', 'avg_temperature_f'], FERVO_THRESHOLD_F], 'transparent',
                    '#FFC107'  // Amber for water-based EGS (200°C+)
                ];
                break;
            case 'comparison':
                // Show both with different colors: blue for CO₂-EGS only (150-200°C), amber for both viable (200°C+)
                fillColor = [
                    'case',
                    ['==', ['get', 'avg_temperature_f'], null], 'transparent',
                    ['<', ['get', 'avg_temperature_f'], CO2_EGS_MIN_F], 'transparent',
                    ['<', ['get', 'avg_temperature_f'], FERVO_THRESHOLD_F], '#00BCD4',  // Blue - CO₂-EGS only
                    '#FFC107'  // Amber - both viable
                ];
                break;
            case 'all':
            default:
                // Default: all CO₂-EGS viable (150°C+) in blue
                fillColor = [
                    'case',
                    ['==', ['get', 'avg_temperature_f'], null], 'transparent',
                    ['<', ['get', 'avg_temperature_f'], CO2_EGS_MIN_F], 'transparent',
                    '#00BCD4'  // Blue for all CO₂-EGS viable
                ];
                break;
        }
        
        this.map.setPaintProperty('co2egs-fill', 'fill-color', fillColor);
        
        // Recalculate stats for current view mode
        if (document.getElementById('co2egs-toggle')?.checked) {
            this.calculateCO2EGSStats();
        }
        
        console.log(`🔥 CO₂-EGS view mode changed to: ${mode}`);
    }

    // Update CO₂-EGS layer filter based on max depth to basement
    updateCO2EGSBasementFilter(maxDepth) {
        if (!this.map.getLayer('co2egs-fill')) return;
        
        // We need to cross-reference hexagon locations with geology depth data
        // Load geology data and create a spatial lookup
        this.applyCO2EGSBasementFilter(maxDepth);
    }

    async applyCO2EGSBasementFilter(maxDepth) {
        try {
            // Get geology data for depth lookup
            const geologySource = this.map.getSource('geology');
            if (!geologySource || !geologySource._data) {
                console.warn('⚠️ Geology data not loaded - cannot filter by basement depth');
                return;
            }
            
            const geologyData = geologySource._data;
            const hexSource = this.map.getSource('hexagon-mesh');
            if (!hexSource || !hexSource._data) {
                console.warn('⚠️ Hexagon mesh not loaded');
                return;
            }
            
            // Build spatial index from geology points (grid-based for fast lookup)
            const cellSize = 0.5; // degrees
            const geologyGrid = {};
            for (const feat of geologyData.features) {
                const [lon, lat] = feat.geometry.coordinates;
                const cellKey = `${Math.floor(lon / cellSize)},${Math.floor(lat / cellSize)}`;
                if (!geologyGrid[cellKey]) geologyGrid[cellKey] = [];
                geologyGrid[cellKey].push({
                    lon, lat,
                    depth: feat.properties.dt // depth to basement in meters
                });
            }
            
            // For each hexagon, find nearest geology point and check depth
            const hexData = JSON.parse(JSON.stringify(hexSource._data));
            let filteredCount = 0;
            let totalCount = 0;
            
            for (const hex of hexData.features) {
                totalCount++;
                // Get hexagon centroid
                const coords = hex.geometry.coordinates[0];
                let centroidLon = 0, centroidLat = 0;
                for (const [lon, lat] of coords) {
                    centroidLon += lon;
                    centroidLat += lat;
                }
                centroidLon /= coords.length;
                centroidLat /= coords.length;
                
                // Look up depth in nearby cells
                const cellKey = `${Math.floor(centroidLon / cellSize)},${Math.floor(centroidLat / cellSize)}`;
                const nearbyPoints = [];
                for (let dx = -1; dx <= 1; dx++) {
                    for (let dy = -1; dy <= 1; dy++) {
                        const key = `${Math.floor(centroidLon / cellSize) + dx},${Math.floor(centroidLat / cellSize) + dy}`;
                        if (geologyGrid[key]) nearbyPoints.push(...geologyGrid[key]);
                    }
                }
                
                // Find closest geology point
                let minDist = Infinity;
                let closestDepth = null;
                for (const pt of nearbyPoints) {
                    const dist = Math.sqrt((pt.lon - centroidLon) ** 2 + (pt.lat - centroidLat) ** 2);
                    if (dist < minDist) {
                        minDist = dist;
                        closestDepth = pt.depth;
                    }
                }
                
                // Store depth on hexagon for filtering
                hex.properties.basement_depth = closestDepth;
                
                // Mark as filtered if depth exceeds max
                if (closestDepth !== null && closestDepth > maxDepth) {
                    hex.properties._filtered = true;
                    filteredCount++;
                } else {
                    hex.properties._filtered = false;
                }
            }
            
            // Update the source data
            hexSource.setData(hexData);
            
            // Apply filter to CO2-EGS layer
            this.map.setFilter('co2egs-fill', ['!=', ['get', '_filtered'], true]);
            this.map.setFilter('co2egs-outline', ['!=', ['get', '_filtered'], true]);
            
            console.log(`🎯 CO₂-EGS basement filter: max ${maxDepth}m, filtered out ${filteredCount}/${totalCount} hexagons`);
            
            // Recalculate stats
            if (document.getElementById('co2egs-toggle')?.checked) {
                this.calculateCO2EGSStats();
            }
        } catch (error) {
            console.error('❌ Error applying basement depth filter:', error);
        }
    }

    // Set depth and trigger update
    async setDepthAndUpdate(depth) {
        const depthFilter = document.getElementById('depth-filter');
        if (depthFilter) {
            depthFilter.value = depth;
            
            // Show loading indicator
            document.getElementById('loading').style.display = 'block';
            
            try {
                await this.updateMeshForDepth(depth);
                
                // Apply basement depth filters to comparison layers if they're active
                const conventionalChecked = document.getElementById('compare-conventional')?.checked;
                const co2egsChecked = document.getElementById('compare-co2egs')?.checked;
                
                if (conventionalChecked) {
                    const maxDepth = parseInt(document.getElementById('compare-conventional-basement')?.value) || 6000;
                    await this.applyComparisonBasementFilter('conventional', maxDepth);
                }
                if (co2egsChecked) {
                    const maxDepth = parseInt(document.getElementById('compare-co2egs-basement')?.value) || 6000;
                    await this.applyComparisonBasementFilter('co2egs', maxDepth);
                }
            } catch (error) {
                console.error('❌ Error updating mesh for depth:', error);
            } finally {
                document.getElementById('loading').style.display = 'none';
            }
        }
    }

    // Apply comparison presets - DEPRECATED (use EGS Technology Comparison controls instead)
    async applyPreset(preset) {
        console.log(`🎯 Applying preset: ${preset} (deprecated - use EGS Technology Comparison)`);
        // This function is no longer used since CO2-EGS Resource Potential was removed
    }

    // Show popup for CO₂-EGS layer click
    showCO2EGSPopup(e) {
        const props = e.features[0].properties;
        const tempF = props.avg_temperature_f;
        
        if (!tempF) {
            return; // No data, don't show popup
        }
        
        const tempC = this.fToC(tempF);
        const currentDepth = document.getElementById('depth-filter')?.value || 3000;
        
        // Temperature thresholds in Fahrenheit
        const CO2_EGS_MIN_F = 302;      // 150°C
        const FERVO_THRESHOLD_F = 392;  // 200°C
        
        // Determine zone - TWO ZONES ONLY
        let zoneName, zoneColor, zoneDescription, zoneIcon;
        if (tempF < CO2_EGS_MIN_F) {
            zoneName = 'Below EGS Threshold';
            zoneColor = '#9E9E9E';
            zoneDescription = 'Temperature too low for economic EGS development';
            zoneIcon = '❄️';
        } else if (tempF < FERVO_THRESHOLD_F) {
            zoneName = 'CO₂-EGS Only Zone';
            zoneColor = '#00BCD4';
            zoneDescription = 'Unlocked by CO₂-EGS! Below Fervo/water-EGS commercial threshold (200°C). CO₂-EGS thermosiphon effect enables economic development.';
            zoneIcon = '🔹';
        } else {
            zoneName = 'Both Viable Zone';
            zoneColor = '#FFC107';
            zoneDescription = 'Both CO₂-EGS and conventional water-EGS are viable at 200°C+. CO₂-EGS has thermodynamic advantages (lower viscosity, thermosiphon, better heat mining).';
            zoneIcon = '🔸';
        }
        
        const popupHTML = `
            <div class="popup-content" style="max-width: 350px;">
                <div class="popup-header" style="background: ${zoneColor}; color: ${tempF >= FERVO_THRESHOLD_F ? '#333' : 'white'}; padding: 10px; margin: -10px -10px 10px -10px; border-radius: 4px 4px 0 0;">
                    <h3 style="margin: 0; font-size: 14px;">${zoneIcon} ${zoneName}</h3>
                </div>
                <div style="background: #f5f5f5; padding: 10px; border-radius: 4px; margin-bottom: 10px;">
                    <div style="font-size: 28px; font-weight: bold; color: ${zoneColor}; text-align: center;">
                        ${tempC}°C
                    </div>
                    <div style="text-align: center; font-size: 12px; color: #666;">
                        at ${currentDepth}m depth
                    </div>
                </div>
                <div style="font-size: 12px; color: #555; line-height: 1.4; margin-bottom: 10px;">
                    ${zoneDescription}
                </div>
                <div style="font-size: 11px; color: #888; border-top: 1px solid #eee; padding-top: 8px;">
                    <div><strong>Data Points:</strong> ${props.point_count || 'N/A'}</div>
                    ${props.min_temperature_f ? `<div><strong>Temp Range:</strong> ${this.fToC(props.min_temperature_f)}°C - ${this.fToC(props.max_temperature_f)}°C</div>` : ''}
                </div>
            </div>
        `;
        
        if (this.activePopup) {
            this.activePopup.remove();
        }
        
        this.activePopup = new maplibregl.Popup({
            closeButton: true,
            closeOnClick: false,
            maxWidth: '380px'
        })
            .setLngLat(e.lngLat)
            .setHTML(popupHTML)
            .addTo(this.map);
    }

    // Calculate and display CO₂-EGS resource statistics
    calculateCO2EGSStats() {
        const statsEl = document.getElementById('co2egs-stats');
        if (!statsEl) return;
        
        try {
            const source = this.map.getSource('hexagon-mesh');
            if (!source || !source._data) {
                statsEl.textContent = 'No geothermal data available';
                return;
            }
            
            const hexData = source._data;
            if (!hexData || !hexData.features) {
                statsEl.textContent = 'No hexagon data available';
                return;
            }
            
            // Temperature thresholds in Fahrenheit
            const CO2_EGS_MIN_F = 302;      // 150°C
            const FERVO_THRESHOLD_F = 392;  // 200°C
            
            let co2OnlyCount = 0;
            let bothViableCount = 0;
            let totalWithData = 0;
            
            // Hexagon area calculation:
            // Using 5-mile radius hexagons, area ≈ 65 sq miles each
            // Formula: (3 * sqrt(3) / 2) * r^2 ≈ 2.598 * 25 ≈ 65 sq mi
            const hexAreaSqMiles = 65;
            
            for (const hex of hexData.features) {
                const temp = hex.properties?.avg_temperature_f;
                if (temp === null || temp === undefined) continue;
                
                totalWithData++;
                
                if (temp >= CO2_EGS_MIN_F && temp < FERVO_THRESHOLD_F) {
                    co2OnlyCount++;
                } else if (temp >= FERVO_THRESHOLD_F) {
                    bothViableCount++;  // 200°C+ is both viable (CO₂-EGS and conventional)
                }
            }
            
            // GW estimate methodology:
            // - 1 plant per 100 sq miles (conservative spacing)
            // - Average plant size: 20 MW
            // - Each hexagon is ~65 sq mi → 0.65 plants per hex → ~13 MW per hexagon
            const plantSpacingSqMi = 100;
            const avgPlantMW = 20;
            const mwPerHexagon = (hexAreaSqMiles / plantSpacingSqMi) * avgPlantMW;  // ~13 MW
            
            const co2OnlyGW = (co2OnlyCount * mwPerHexagon) / 1000;
            const bothViableGW = (bothViableCount * mwPerHexagon) / 1000;
            const totalCO2GW = ((co2OnlyCount + bothViableCount) * mwPerHexagon) / 1000;
            
            const currentDepth = document.getElementById('depth-filter')?.value || 3000;
            
            // Calculate total area in sq miles
            const co2OnlyArea = Math.round(co2OnlyCount * hexAreaSqMiles).toLocaleString();
            const bothViableArea = Math.round(bothViableCount * hexAreaSqMiles).toLocaleString();
            
            statsEl.innerHTML = `
                <div style="font-size: 11px; line-height: 1.4;">
                    <div style="font-weight: bold; margin-bottom: 4px;">At ${currentDepth}m depth:</div>
                    <div style="color: #00838F;">🔹 CO₂-EGS Only (150-200°C): ${co2OnlyCount} cells (${co2OnlyArea} mi²)</div>
                    <div style="color: #FF8F00;">🔸 Both Viable (200°C+): ${bothViableCount} cells (${bothViableArea} mi²)</div>
                    <div style="margin-top: 6px; padding-top: 6px; border-top: 1px dashed #ccc;">
                        <div style="font-weight: bold; color: #00838F;">
                            CO₂-EGS unlocks: ~${co2OnlyGW.toFixed(1)} GW*
                        </div>
                        <div style="font-size: 10px; color: #666; margin-top: 2px;">
                            Total CO₂-EGS viable: ~${totalCO2GW.toFixed(1)} GW
                        </div>
                        <div style="font-size: 10px; color: #999; margin-top: 2px;">
                            *1 plant/100 mi² × 20 MW avg
                        </div>
                    </div>
                </div>
            `;
            
            console.log(`🔥 CO₂-EGS Stats: CO₂-only=${co2OnlyCount}, Both=${bothViableCount}`);
            
        } catch (error) {
            console.error('Error calculating CO₂-EGS stats:', error);
            statsEl.textContent = 'Error calculating statistics';
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

        const co2egsLayers = [
            'co2egs-fill'
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

        // CO₂-EGS layer click handler
        co2egsLayers.forEach(layerId => {
            this.map.on('click', layerId, (e) => {
                if (e.features.length > 0) {
                    this.showCO2EGSPopup(e);
                }
            });
            
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
            const avgTempC = this.fToC(properties.avg_temperature_f);
            const minTempC = this.fToC(properties.min_temperature_f);
            const maxTempC = this.fToC(properties.max_temperature_f);
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
                    <span class="popup-value">${avgTempC !== null ? avgTempC + '°C' : 'Unknown'}</span>
                </div>
                <div class="popup-row">
                    <span class="popup-label">Temp Range:</span> 
                    <span class="popup-value">${minTempC !== null ? minTempC : 'N/A'} - ${maxTempC !== null ? maxTempC : 'N/A'}°C</span>
                </div>
                <div class="popup-row">
                    <span class="popup-label">Avg Depth:</span> 
                    <span class="popup-value">${properties.avg_depth_m ? properties.avg_depth_m + ' meters' : 'Unknown'}</span>
                </div>
            `;
        } else {
            // Individual point
            const tempC = this.fToC(properties.temperature_f);
            popupContent = `
                <div class="popup-header">
                    <i class="fas fa-thermometer-half"></i> Geothermal Point
                </div>
                <div class="popup-row">
                    <span class="popup-label">Temperature:</span> 
                    <span class="popup-value">${tempC !== null ? tempC + '°C' : 'Unknown'}</span>
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
        
        const avgTempC = this.fToC(properties.avg_temperature_f);
        const minTempC = this.fToC(properties.min_temperature_f);
        const maxTempC = this.fToC(properties.max_temperature_f);
        
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
                <span class="popup-value" style="font-weight: bold; color: ${this.getTemperatureColor(properties.avg_temperature_f)}">${avgTempC !== null ? avgTempC + '°C' : 'No data'}</span>
            </div>
            <div class="popup-row">
                <span class="popup-label">Average Depth:</span> 
                <span class="popup-value">${properties.avg_depth_m ? properties.avg_depth_m + ' meters' : 'Unknown'}</span>
            </div>
            <div class="popup-row">
                <span class="popup-label">Temperature Range:</span> 
                <span class="popup-value">${minTempC !== null ? minTempC : 'N/A'} - ${maxTempC !== null ? maxTempC : 'N/A'}°C</span>
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

        // EnergyNet parcels controls
        document.getElementById('energynet-toggle').addEventListener('change', (e) => {
            this.toggleEnergyNetParcels(e.target.checked);
        });

        document.getElementById('energynet-opacity').addEventListener('input', (e) => {
            const opacity = e.target.value / 100;
            document.getElementById('energynet-opacity-value').textContent = e.target.value + '%';
            this.updateEnergyNetParcelsOpacity(opacity);
        });
        
        // Update Active Listings button
        document.getElementById('update-listings-btn').addEventListener('click', () => {
            this.updateActiveListings();
        });

        // Datacenter controls
        document.getElementById('datacenter-toggle').addEventListener('change', (e) => {
            this.toggleDatacenters(e.target.checked);
        });

        // Update Data Centers button
        document.getElementById('update-datacenters-btn').addEventListener('click', () => {
            this.updateDatacenters();
        });

        // Optimal Sites controls
        document.getElementById('optimal-sites-toggle').addEventListener('change', (e) => {
            this.toggleOptimalSites(e.target.checked);
        });
        document.getElementById('optimal-depth').addEventListener('input', (e) => {
            document.getElementById('optimal-depth-value').textContent = e.target.value;
            if (this.layerState.optimalSites) {
                this.calculateOptimalSites();
            }
        });
        document.getElementById('optimal-count').addEventListener('input', (e) => {
            document.getElementById('optimal-count-value').textContent = e.target.value;
            if (this.layerState.optimalSites) {
                this.calculateOptimalSites();
            }
        });
        document.getElementById('optimal-use-ethanol').addEventListener('change', () => {
            if (this.layerState.optimalSites) {
                this.calculateOptimalSites();
            }
        });
        document.getElementById('optimal-use-emitters').addEventListener('change', () => {
            if (this.layerState.optimalSites) {
                this.calculateOptimalSites();
            }
        });

        // Ethanol controls
        document.getElementById('ethanol-toggle').addEventListener('change', (e) => {
            this.toggleEthanol(e.target.checked);
        });

        // Geology controls
        document.getElementById('geology-toggle').addEventListener('change', (e) => {
            this.toggleGeology(e.target.checked);
        });
        document.getElementById('geology-opacity').addEventListener('input', (e) => {
            this.updateGeologyOpacity(parseFloat(e.target.value));
        });
        document.getElementById('geology-min-depth').addEventListener('input', (e) => {
            document.getElementById('geology-min-depth-value').textContent = e.target.value;
            this.updateGeologyFilter();
        });
        // CO2-EGS overlap checkbox removed from UI
        // document.getElementById('geology-co2egs-overlap').addEventListener('change', (e) => {
        //     this.filterGeologyByCO2EGSOverlap(e.target.checked);
        // });

        // Emitters controls
        document.getElementById('emitters-toggle').addEventListener('change', (e) => {
            this.toggleEmitters(e.target.checked);
        });

        // Emitter sub-category controls
        document.getElementById('emitter-power-plants').addEventListener('change', (e) => {
            this.layerState.emitterPowerPlants = e.target.checked;
            this.updateEmittersFilter();
        });
        document.getElementById('emitter-petroleum').addEventListener('change', (e) => {
            this.layerState.emitterPetroleum = e.target.checked;
            this.updateEmittersFilter();
        });
        document.getElementById('emitter-waste').addEventListener('change', (e) => {
            this.layerState.emitterWaste = e.target.checked;
            this.updateEmittersFilter();
        });
        document.getElementById('emitter-chemicals').addEventListener('change', (e) => {
            this.layerState.emitterChemicals = e.target.checked;
            this.updateEmittersFilter();
        });
        document.getElementById('emitter-minerals').addEventListener('change', (e) => {
            this.layerState.emitterMinerals = e.target.checked;
            this.updateEmittersFilter();
        });
        document.getElementById('emitter-metals').addEventListener('change', (e) => {
            this.layerState.emitterMetals = e.target.checked;
            this.updateEmittersFilter();
        });
        document.getElementById('emitter-other').addEventListener('change', (e) => {
            this.layerState.emitterOther = e.target.checked;
            this.updateEmittersFilter();
        });

        // Emissions slider
        document.getElementById('emissions-slider').addEventListener('input', (e) => {
            const value = parseInt(e.target.value);
            this.layerState.emitterMinEmissions = value;
            document.getElementById('emissions-value').textContent = value.toLocaleString();
            this.updateEmittersFilter();
        });

        // CCUS controls
        document.getElementById('ccus-toggle').addEventListener('change', (e) => {
            this.toggleCCUS(e.target.checked);
        });

        // CCUS sub-category controls
        document.getElementById('ccus-saline').addEventListener('change', (e) => {
            this.layerState.ccusSaline = e.target.checked;
            this.updateCCUSFilter();
        });
        document.getElementById('ccus-eor').addEventListener('change', (e) => {
            this.layerState.ccusEOR = e.target.checked;
            this.updateCCUSFilter();
        });
        document.getElementById('ccus-utilization').addEventListener('change', (e) => {
            this.layerState.ccusUtilization = e.target.checked;
            this.updateCCUSFilter();
        });
        document.getElementById('ccus-other').addEventListener('change', (e) => {
            this.layerState.ccusOther = e.target.checked;
            this.updateCCUSFilter();
        });

        // Mesh controls
        document.getElementById('mesh-toggle').addEventListener('change', (e) => {
            this.toggleMesh(e.target.checked);
        });

        document.getElementById('mesh-opacity').addEventListener('input', (e) => {
            const opacity = e.target.value / 100;
            document.getElementById('mesh-opacity-value').textContent = e.target.value + '%';
            this.updateMeshOpacity(opacity);
        });

        // CO₂-EGS controls - COMMENTED OUT (UI elements removed, using EGS Technology Comparison instead)
        /*
        document.getElementById('co2egs-toggle').addEventListener('change', (e) => {
            this.toggleCO2EGS(e.target.checked);
        });

        document.getElementById('co2egs-opacity').addEventListener('input', (e) => {
            const opacity = e.target.value / 100;
            document.getElementById('co2egs-opacity-value').textContent = e.target.value + '%';
            this.updateCO2EGSOpacity(opacity);
        });

        // CO₂-EGS view mode selector
        document.getElementById('co2egs-view-mode').addEventListener('change', (e) => {
            this.updateCO2EGSViewMode(e.target.value);
        });

        // CO₂-EGS max basement depth filter
        document.getElementById('co2egs-max-basement-depth').addEventListener('input', (e) => {
            const maxDepth = parseInt(e.target.value);
            document.getElementById('co2egs-max-basement-depth-value').textContent = maxDepth;
            this.updateCO2EGSBasementFilter(maxDepth);
        });

        // Quick depth buttons
        document.getElementById('depth-3000').addEventListener('click', () => {
            this.setDepthAndUpdate(3000);
        });
        document.getElementById('depth-4000').addEventListener('click', () => {
            this.setDepthAndUpdate(4000);
        });
        document.getElementById('depth-5000').addEventListener('click', () => {
            this.setDepthAndUpdate(5000);
        });
        */

        // EGS Comparison layer controls
        document.getElementById('compare-conventional').addEventListener('change', (e) => {
            this.toggleComparisonLayer('conventional', e.target.checked);
        });
        document.getElementById('compare-co2egs').addEventListener('change', (e) => {
            this.toggleComparisonLayer('co2egs', e.target.checked);
        });
        
        // Comparison layer parameter inputs - reload layer when changed
        document.getElementById('compare-conventional-temp').addEventListener('change', () => {
            if (document.getElementById('compare-conventional').checked) {
                this.reloadComparisonLayer('conventional');
            }
        });
        document.getElementById('compare-conventional-depth').addEventListener('change', () => {
            if (document.getElementById('compare-conventional').checked) {
                this.reloadComparisonLayer('conventional');
            }
        });
        document.getElementById('compare-co2egs-temp').addEventListener('change', () => {
            if (document.getElementById('compare-co2egs').checked) {
                this.reloadComparisonLayer('co2egs');
            }
        });
        document.getElementById('compare-co2egs-depth').addEventListener('change', () => {
            if (document.getElementById('compare-co2egs').checked) {
                this.reloadComparisonLayer('co2egs');
            }
        });
        
        // Comparison layer basement depth sliders
        document.getElementById('compare-conventional-basement').addEventListener('input', (e) => {
            const maxDepth = parseInt(e.target.value);
            document.getElementById('compare-conventional-basement-value').textContent = maxDepth;
            if (document.getElementById('compare-conventional').checked) {
                this.applyComparisonBasementFilter('conventional', maxDepth);
            }
        });
        document.getElementById('compare-co2egs-basement').addEventListener('input', (e) => {
            const maxDepth = parseInt(e.target.value);
            document.getElementById('compare-co2egs-basement-value').textContent = maxDepth;
            if (document.getElementById('compare-co2egs').checked) {
                this.applyComparisonBasementFilter('co2egs', maxDepth);
            }
        });
        
        document.getElementById('compare-show-both').addEventListener('click', async () => {
            document.getElementById('compare-conventional').checked = true;
            document.getElementById('compare-co2egs').checked = true;
            await this.toggleComparisonLayer('conventional', true);
            await this.toggleComparisonLayer('co2egs', true);
        });
        document.getElementById('compare-clear').addEventListener('click', () => {
            document.getElementById('compare-conventional').checked = false;
            document.getElementById('compare-co2egs').checked = false;
            this.toggleComparisonLayer('conventional', false);
            this.toggleComparisonLayer('co2egs', false);
        });
        
        // Depth filter functionality - re-aggregate mesh when depth changes
        document.getElementById('depth-filter').addEventListener('change', async (e) => {
            const depth = parseFloat(e.target.value);
            if (depth && depth > 0) {
                console.log(`🌡️ Filtering geothermal data by depth: ${depth}m`);
                
                // Show loading indicator
                document.getElementById('loading').style.display = 'block';
                
                try {
                    // Re-generate hexagon mesh with new depth filter
                    await this.updateMeshForDepth(depth);
                    
                    // Apply basement depth filters to comparison layers if they're active
                    const conventionalChecked = document.getElementById('compare-conventional')?.checked;
                    const co2egsChecked = document.getElementById('compare-co2egs')?.checked;
                    
                    if (conventionalChecked) {
                        const maxDepth = parseInt(document.getElementById('compare-conventional-basement')?.value) || 6000;
                        await this.applyComparisonBasementFilter('conventional', maxDepth);
                    }
                    if (co2egsChecked) {
                        const maxDepth = parseInt(document.getElementById('compare-co2egs-basement')?.value) || 6000;
                        await this.applyComparisonBasementFilter('co2egs', maxDepth);
                    }
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

    updateEnergyNetParcelsOpacity(opacity) {
        const fillLayerId = 'energynet-parcels-fill';
        const outlineLayerId = 'energynet-parcels-outline';
        
        // Pin layer IDs
        const clustersLayerId = 'energynet-clusters';
        const unclusteredLayerId = 'energynet-unclustered-pins';
        
        // Update parcel layers opacity
        if (this.map.getLayer(fillLayerId)) {
            this.map.setPaintProperty(fillLayerId, 'fill-opacity', opacity);
        }
        
        if (this.map.getLayer(outlineLayerId)) {
            // Keep outline slightly more opaque
            this.map.setPaintProperty(outlineLayerId, 'line-opacity', Math.min(1.0, opacity + 0.2));
        }
        
        // Update pin layers opacity
        if (this.map.getLayer(clustersLayerId)) {
            this.map.setPaintProperty(clustersLayerId, 'circle-opacity', opacity);
        }
        
        if (this.map.getLayer(unclusteredLayerId)) {
            this.map.setPaintProperty(unclusteredLayerId, 'circle-opacity', opacity);
        }
    }

    async addEnergyNetPinsLayer() {
        console.log('📍 Adding EnergyNet clustered pins layer...');
        
        try {
            // Load pins data
            const response = await fetch('/api/energynet-pins', {
                headers: {
                    'Cache-Control': 'no-cache',
                    'Pragma': 'no-cache'
                }
            });
            
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            
            const pinsData = await response.json();
            console.log(`📍 Loaded ${pinsData.features?.length || 0} EnergyNet pins`);
            
            // Add clustered source for pins
            this.map.addSource('energynet-pins', {
                type: 'geojson',
                data: pinsData,
                cluster: true,
                clusterMaxZoom: 14,
                clusterRadius: 50,
                clusterProperties: {
                    // Sum total acres in each cluster
                    sum_acres: ['+', ['get', 'acres']]
                }
            });
            
            // Add cluster layer - triangle shape for land parcels
            this.map.addLayer({
                id: 'energynet-clusters',
                type: 'symbol',
                source: 'energynet-pins',
                minzoom: 0,
                maxzoom: 7.5,
                filter: ['has', 'point_count'],
                layout: {
                    'icon-image': 'triangle-cluster',
                    'icon-size': [
                        'step',
                        ['get', 'point_count'],
                        1.0,    // size for clusters with 1-99 points
                        100, 1.3,  // size for clusters with 100-999 points  
                        750, 1.6   // size for clusters with 1000+ points
                    ],
                    'icon-allow-overlap': true,
                    'icon-ignore-placement': true,
                    'text-field': '{point_count_abbreviated}',
                    'text-font': ['DIN Offc Pro Medium', 'Arial Unicode MS Bold'],
                    'text-size': 12,
                    'text-offset': [0, 0.2]
                },
                paint: {
                    'text-color': 'white'
                }
            });
            
            // Add unclustered point layer (individual pins) - triangle shape
            this.map.addLayer({
                id: 'energynet-unclustered-pins',
                type: 'symbol',
                source: 'energynet-pins',
                minzoom: 0,
                maxzoom: 7.5,
                filter: ['!', ['has', 'point_count']],
                layout: {
                    'icon-image': 'triangle',
                    'icon-size': 0.8,
                    'icon-allow-overlap': true,
                    'icon-ignore-placement': true
                }
            });
            
            // Add click handlers for clusters and pins
            this.map.on('click', 'energynet-clusters', (e) => {
                const features = this.map.queryRenderedFeatures(e.point, {
                    layers: ['energynet-clusters']
                });
                const clusterId = features[0].properties.cluster_id;
                this.map.getSource('energynet-pins').getClusterExpansionZoom(clusterId, (err, zoom) => {
                    if (!err) {
                        this.map.easeTo({
                            center: features[0].geometry.coordinates,
                            zoom: zoom
                        });
                    }
                });
            });
            
            // Add click handler for individual pins (same popup as parcels)
            this.map.on('click', 'energynet-unclustered-pins', (e) => {
                this.showEnergyNetParcelPopup(e);
            });
            
            // Add hover cursor
            this.map.on('mouseenter', 'energynet-clusters', () => {
                this.map.getCanvas().style.cursor = 'pointer';
            });
            this.map.on('mouseleave', 'energynet-clusters', () => {
                this.map.getCanvas().style.cursor = '';
            });
            
            this.map.on('mouseenter', 'energynet-unclustered-pins', () => {
                this.map.getCanvas().style.cursor = 'pointer';
            });
            this.map.on('mouseleave', 'energynet-unclustered-pins', () => {
                this.map.getCanvas().style.cursor = '';
            });
            
            console.log('✅ Successfully added EnergyNet clustered pins layer');
            
        } catch (error) {
            console.error('❌ Error adding EnergyNet pins layer:', error);
        }
    }

    async updateActiveListings() {
        const button = document.getElementById('update-listings-btn');
        const icon = button.querySelector('i');
        
        try {
            // Disable button and show loading state
            button.disabled = true;
            button.classList.add('loading');
            button.innerHTML = '<i class="fas fa-sync-alt"></i> Updating...';
            
            // Disable EnergyNet layer toggle during update
            const energynetToggle = document.getElementById('energynet-toggle');
            energynetToggle.disabled = true;
            
            console.log('🔄 Starting active listings update...');
            
            // Connect to the scraper endpoint with Server-Sent Events
            const response = await fetch('/api/scrape-update', {
                method: 'POST',
                headers: {
                    'Accept': 'text/event-stream',
                }
            });
            
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            
            const reader = response.body.getReader();
            const decoder = new TextDecoder();
            
            while (true) {
                const { value, done } = await reader.read();
                if (done) break;
                
                const chunk = decoder.decode(value);
                const lines = chunk.split('\n');
                
                for (const line of lines) {
                    if (line.startsWith('data: ')) {
                        try {
                            const data = JSON.parse(line.substring(6));
                            this.handleScraperProgress(data, button);
                        } catch (e) {
                            console.warn('Failed to parse SSE data:', line);
                        }
                    }
                }
            }
            
        } catch (error) {
            console.error('❌ Error updating active listings:', error);
            button.innerHTML = '<i class="fas fa-exclamation-triangle"></i> Update Failed';
            setTimeout(() => {
                button.innerHTML = '<i class="fas fa-sync-alt"></i> Update Active Listings';
                button.disabled = false;
                button.classList.remove('loading');
            }, 3000);
        } finally {
            // Re-enable EnergyNet layer toggle
            const energynetToggle = document.getElementById('energynet-toggle');
            energynetToggle.disabled = false;
        }
    }

    handleScraperProgress(data, button) {
        const { status, message, currentListing, totalListings } = data;
        
        console.log(`📡 Scraper progress: ${status} - ${message}`);
        
        switch (status) {
            case 'starting':
                button.innerHTML = '<i class="fas fa-sync-alt"></i> Starting...';
                break;
                
            case 'discovering':
                button.innerHTML = '<i class="fas fa-search"></i> Discovering...';
                break;
                
            case 'discovered':
                button.innerHTML = `<i class="fas fa-list"></i> Found ${totalListings} listings`;
                break;
                
            case 'cleanup':
                button.innerHTML = '<i class="fas fa-broom"></i> Cleaning up...';
                break;
                
            case 'processing':
                if (currentListing && totalListings) {
                    const progress = Math.round((currentListing / totalListings) * 100);
                    button.innerHTML = `<i class="fas fa-cog"></i> ${progress}% (${currentListing}/${totalListings})`;
                } else {
                    button.innerHTML = '<i class="fas fa-cog"></i> Processing...';
                }
                break;
                
            case 'complete':
                button.innerHTML = '<i class="fas fa-check"></i> Update Complete';
                console.log('✅ Scraper update completed successfully');
                
                // Refresh the EnergyNet parcels and pins data
                setTimeout(async () => {
                    console.log('🔄 Refreshing EnergyNet parcels and pins data...');
                    await this.loadEnergyNetParcels();
                    await this.refreshEnergyNetPins();
                    
                    // Reset button
                    button.innerHTML = '<i class="fas fa-sync-alt"></i> Update Active Listings';
                    button.disabled = false;
                    button.classList.remove('loading');
                }, 1000);
                break;
                
            case 'error':
                button.innerHTML = '<i class="fas fa-exclamation-triangle"></i> Error';
                console.error('❌ Scraper error:', message);
                setTimeout(() => {
                    button.innerHTML = '<i class="fas fa-sync-alt"></i> Update Active Listings';
                    button.disabled = false;
                    button.classList.remove('loading');
                }, 3000);
                break;
        }
    }

    async refreshEnergyNetPins() {
        try {
            console.log('📍 Refreshing EnergyNet pins data...');

            // Load fresh pins data
            const response = await fetch('/api/energynet-pins', {
                headers: {
                    'Cache-Control': 'no-cache',
                    'Pragma': 'no-cache'
                }
            });

            if (response.ok) {
                const pinsData = await response.json();
                console.log(`📍 Refreshed ${pinsData.features?.length || 0} EnergyNet pins`);

                // Update the existing pins source
                const source = this.map.getSource('energynet-pins');
                if (source) {
                    source.setData(pinsData);
                    console.log('✅ Updated EnergyNet pins source with fresh data');
                }
            }
        } catch (error) {
            console.error('❌ Error refreshing EnergyNet pins:', error);
        }
    }

    // ==== DATACENTER METHODS ====

    async addDatacenterLayer() {
        console.log('🏢 Adding Datacenter facilities layer...');

        try {
            // Add source for datacenters (no clustering - show all squares)
            if (!this.map.getSource('datacenters')) {
                this.map.addSource('datacenters', {
                    type: 'geojson',
                    data: {
                        type: 'FeatureCollection',
                        features: []
                    }
                });
                console.log('✅ Added datacenters source');
            }

            // Add datacenter points layer - square shape, visible at all zoom levels
            this.map.addLayer({
                id: 'datacenter-points',
                type: 'symbol',
                source: 'datacenters',
                layout: {
                    'icon-image': 'square',
                    'icon-size': [
                        'interpolate', ['linear'], ['zoom'],
                        4, 0.6,   // smaller at low zoom
                        8, 0.9,   // normal at mid zoom
                        12, 1.0   // full size at high zoom
                    ],
                    'icon-allow-overlap': true,
                    'icon-ignore-placement': true
                }
            });

            console.log('✅ Successfully added datacenter layers');

            // Load datacenter data
            await this.loadDatacenters();

            // Set initial visibility
            this.toggleDatacenters(this.layerState.datacenters);

            // Add click handlers for clusters
            this.map.on('click', 'datacenter-clusters', (e) => {
                const features = this.map.queryRenderedFeatures(e.point, {
                    layers: ['datacenter-clusters']
                });
                const clusterId = features[0].properties.cluster_id;
                this.map.getSource('datacenters').getClusterExpansionZoom(clusterId, (err, zoom) => {
                    if (!err) {
                        this.map.easeTo({
                            center: features[0].geometry.coordinates,
                            zoom: zoom
                        });
                    }
                });
            });

            // Add click handler for individual datacenters
            this.map.on('click', 'datacenter-points', (e) => {
                this.showDatacenterPopup(e);
            });

            // Add hover cursor
            this.map.on('mouseenter', 'datacenter-clusters', () => {
                this.map.getCanvas().style.cursor = 'pointer';
            });
            this.map.on('mouseleave', 'datacenter-clusters', () => {
                this.map.getCanvas().style.cursor = '';
            });

            this.map.on('mouseenter', 'datacenter-points', () => {
                this.map.getCanvas().style.cursor = 'pointer';
            });
            this.map.on('mouseleave', 'datacenter-points', () => {
                this.map.getCanvas().style.cursor = '';
            });

        } catch (error) {
            console.error('❌ Error adding datacenter layer:', error);
        }
    }

    async loadDatacenters() {
        try {
            console.log('📡 Loading datacenter facilities data...');

            const response = await fetch('/api/datacenters', {
                headers: {
                    'Cache-Control': 'no-cache',
                    'Pragma': 'no-cache'
                }
            });

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }

            const data = await response.json();
            console.log(`🏢 Loaded ${data.features?.length || 0} US datacenter facilities`);

            // Update the map source
            const source = this.map.getSource('datacenters');
            if (source) {
                source.setData(data);
                console.log('✅ Updated datacenters source with data');
            }

            // Cache the data
            this.dataCache.datacenters = data;

        } catch (error) {
            console.error('❌ Error loading datacenters:', error);
        }
    }

    // ==== CCUS METHODS ====

    async addCCUSLayer() {
        console.log('🏭 Adding CCUS sites layer...');

        try {
            // Add empty GeoJSON source for CCUS sites
            this.map.addSource('ccus-sites', {
                type: 'geojson',
                data: {
                    type: 'FeatureCollection',
                    features: []
                }
            });

            // Add CCUS points layer - diamond shape with colors by storage classification
            this.map.addLayer({
                id: 'ccus-points',
                type: 'symbol',
                source: 'ccus-sites',
                layout: {
                    'icon-image': [
                        'match',
                        ['get', 'storage_classification'],
                        'Dedicated Saline Storage', 'diamond-green',
                        'Enhanced Oil Recovery', 'diamond-orange',
                        'Utilization', 'diamond-purple',
                        'Dedicated Saline Storage & Enhanced Oil Recovery', 'diamond-blue',
                        'diamond-gray'  // Default for Other, Unavailable, etc.
                    ],
                    'icon-size': 1.0,
                    'icon-allow-overlap': true,
                    'icon-ignore-placement': true
                }
            });

            console.log('✅ Successfully added CCUS layer');

            // Load CCUS data
            await this.loadCCUS();

            // Set initial visibility
            this.toggleCCUS(this.layerState.ccusSites);

            // Add click handler for CCUS sites
            this.map.on('click', 'ccus-points', (e) => {
                this.showCCUSPopup(e);
            });

            // Add hover cursor
            this.map.on('mouseenter', 'ccus-points', () => {
                this.map.getCanvas().style.cursor = 'pointer';
            });
            this.map.on('mouseleave', 'ccus-points', () => {
                this.map.getCanvas().style.cursor = '';
            });

        } catch (error) {
            console.error('❌ Error adding CCUS layer:', error);
        }
    }

    async loadCCUS() {
        try {
            console.log('📡 Loading CCUS sites data...');

            const response = await fetch('/api/ccus-sites', {
                headers: {
                    'Cache-Control': 'no-cache',
                    'Pragma': 'no-cache'
                }
            });

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }

            const data = await response.json();
            console.log(`🏭 Loaded ${data.features?.length || 0} CCUS sites`);

            // Update the map source
            const source = this.map.getSource('ccus-sites');
            if (source) {
                source.setData(data);
                console.log('✅ Updated CCUS source with data');
            }

            // Update UI count
            const countEl = document.getElementById('ccus-count');
            if (countEl) {
                countEl.textContent = `${data.features?.length || 0} CCUS sites loaded`;
            }

            // Cache the data
            this.dataCache.ccus = data;

        } catch (error) {
            console.error('❌ Error loading CCUS sites:', error);
            const countEl = document.getElementById('ccus-count');
            if (countEl) {
                countEl.textContent = 'Error loading CCUS data';
            }
        }
    }

    // ==== OPTIMAL SITES METHODS ====

    async addOptimalSitesLayer() {
        console.log('🎯 Adding Optimal Sites layer...');

        try {
            // Add empty GeoJSON source for optimal sites
            this.map.addSource('optimal-sites', {
                type: 'geojson',
                data: {
                    type: 'FeatureCollection',
                    features: []
                }
            });

            // Add optimal sites markers layer
            this.map.addLayer({
                id: 'optimal-sites-points',
                type: 'circle',
                source: 'optimal-sites',
                paint: {
                    'circle-color': '#FFD700',  // Gold
                    'circle-radius': [
                        'interpolate', ['linear'], ['get', 'rank'],
                        1, 20,
                        10, 12,
                        25, 8
                    ],
                    'circle-opacity': 0.9,
                    'circle-stroke-width': 3,
                    'circle-stroke-color': '#FF4500'  // Orange-red border
                }
            });

            // Add rank labels
            this.map.addLayer({
                id: 'optimal-sites-labels',
                type: 'symbol',
                source: 'optimal-sites',
                layout: {
                    'text-field': ['concat', '#', ['get', 'rank']],
                    'text-size': 12,
                    'text-font': ['Open Sans Bold'],
                    'text-allow-overlap': true
                },
                paint: {
                    'text-color': '#000000',
                    'text-halo-color': '#FFFFFF',
                    'text-halo-width': 2
                }
            });

            console.log('✅ Successfully added optimal sites layer');

            // Set initial visibility (hidden by default)
            this.toggleOptimalSites(false);

            // Add click handler
            this.map.on('click', 'optimal-sites-points', (e) => {
                this.showOptimalSitePopup(e);
            });

            // Add hover cursor
            this.map.on('mouseenter', 'optimal-sites-points', () => {
                this.map.getCanvas().style.cursor = 'pointer';
            });
            this.map.on('mouseleave', 'optimal-sites-points', () => {
                this.map.getCanvas().style.cursor = '';
            });

        } catch (error) {
            console.error('❌ Error adding optimal sites layer:', error);
        }
    }

    toggleOptimalSites(visible) {
        this.layerState.optimalSites = visible;
        const layers = ['optimal-sites-points', 'optimal-sites-labels'];
        
        layers.forEach(layerId => {
            try {
                if (this.map.getLayer(layerId)) {
                    this.map.setLayoutProperty(layerId, 'visibility', visible ? 'visible' : 'none');
                }
            } catch (error) {
                // Layer might not exist, ignore
            }
        });

        if (visible) {
            this.calculateOptimalSites();
        } else {
            // Clear the layer
            const source = this.map.getSource('optimal-sites');
            if (source) {
                source.setData({ type: 'FeatureCollection', features: [] });
            }
            document.getElementById('optimal-sites-count').textContent = 'Check box to find optimal sites';
        }

        this.reorderLayers();
    }

    calculateOptimalSites() {
        console.log('🎯 Calculating optimal sites...');
        
        const countEl = document.getElementById('optimal-sites-count');
        countEl.textContent = 'Calculating...';

        // Check which data sources to use
        const useEthanol = document.getElementById('optimal-use-ethanol')?.checked ?? true;
        const useEmitters = document.getElementById('optimal-use-emitters')?.checked ?? true;

        if (!useEthanol && !useEmitters) {
            countEl.textContent = 'Select at least one data source';
            const source = this.map.getSource('optimal-sites');
            if (source) source.setData({ type: 'FeatureCollection', features: [] });
            return;
        }

        // Get hexagon mesh data for geothermal
        const hexSource = this.map.getSource('hexagon-mesh');
        if (!hexSource || !hexSource._data) {
            countEl.textContent = 'No geothermal data available - enable hexagon mesh first';
            return;
        }

        const topCount = parseInt(document.getElementById('optimal-count').value) || 10;
        const scoredSites = [];
        
        // Process ethanol facilities (small dataset, process all)
        if (useEthanol && this.dataCache.ethanol?.features) {
            for (const facility of this.dataCache.ethanol.features) {
                const props = facility.properties;
                const coords = facility.geometry.coordinates;
                const lng = coords[0];
                const lat = coords[1];

                const emissions = props.emissions_mt_co2e || 0;
                // Skip facilities with no emissions for optimal site calculation
                if (emissions <= 0) continue;
                
                const maxEmissions = 600000;
                const emissionsScore = Math.min(100, (emissions / maxEmissions) * 100);

                const geoData = this.getGeothermalAtLocation(lng, lat);
                let geoScore = 0;
                if (geoData && geoData.avg_temperature_f) {
                    const temp = geoData.avg_temperature_f;
                    geoScore = Math.min(100, Math.max(0, ((temp - 150) / 250) * 100));
                }

                const combinedScore = (emissionsScore * 0.5) + (geoScore * 0.5);

                if (combinedScore > 0) {
                    scoredSites.push({
                        type: 'Feature',
                        geometry: facility.geometry,
                        properties: {
                            facilityName: props.Company || props.Site || 'Unknown',
                            siteName: props.Site || 'N/A',
                            state: props.State || '',
                            sourceType: 'ethanol',
                            emissions: emissions,
                            emissionsScore: Math.round(emissionsScore * 10) / 10,
                            geoScore: Math.round(geoScore * 10) / 10,
                            combinedScore: Math.round(combinedScore * 10) / 10,
                            geoTemp: geoData?.avg_temperature_f || null,
                            geoPointCount: geoData?.point_count || 0
                        }
                    });
                }
            }
        }

        // Process point source emitters - OPTIMIZED: only process top emitters by emissions
        if (useEmitters && this.dataCache.emitters?.features) {
            // Pre-filter and sort emitters by emissions to only process top 500
            const emittersWithEmissions = this.dataCache.emitters.features
                .filter(f => {
                    const e = f.properties.total_emissions_2023;
                    return e && e > 0;
                })
                .sort((a, b) => (b.properties.total_emissions_2023 || 0) - (a.properties.total_emissions_2023 || 0))
                .slice(0, 500); // Only process top 500 emitters
            
            console.log(`🎯 Processing top ${emittersWithEmissions.length} emitters by emissions`);
            
            for (const facility of emittersWithEmissions) {
                const props = facility.properties;
                const coords = facility.geometry.coordinates;
                const lng = coords[0];
                const lat = coords[1];

                const emissions = props.total_emissions_2023 || 0;
                const maxEmissions = 20000000;
                const emissionsScore = Math.min(100, (emissions / maxEmissions) * 100);

                const geoData = this.getGeothermalAtLocation(lng, lat);
                let geoScore = 0;
                if (geoData && geoData.avg_temperature_f) {
                    const temp = geoData.avg_temperature_f;
                    geoScore = Math.min(100, Math.max(0, ((temp - 150) / 250) * 100));
                }

                const combinedScore = (emissionsScore * 0.5) + (geoScore * 0.5);

                if (combinedScore > 0) {
                    scoredSites.push({
                        type: 'Feature',
                        geometry: facility.geometry,
                        properties: {
                            facilityName: props.facility_name || 'Unknown',
                            siteName: props.city || 'N/A',
                            state: props.state || '',
                            sourceType: 'emitter',
                            industryType: props.industry_type_sectors || 'N/A',
                            emissions: emissions,
                            emissionsScore: Math.round(emissionsScore * 10) / 10,
                            geoScore: Math.round(geoScore * 10) / 10,
                            combinedScore: Math.round(combinedScore * 10) / 10,
                            geoTemp: geoData?.avg_temperature_f || null,
                            geoPointCount: geoData?.point_count || 0
                        }
                    });
                }
            }
        }

        // Sort by combined score and take top N
        scoredSites.sort((a, b) => b.properties.combinedScore - a.properties.combinedScore);
        const topSites = scoredSites.slice(0, topCount);

        // Add rank
        topSites.forEach((site, index) => {
            site.properties.rank = index + 1;
        });

        // Update the map source
        const source = this.map.getSource('optimal-sites');
        if (source) {
            source.setData({
                type: 'FeatureCollection',
                features: topSites
            });
        }

        // Update status with source info
        const sources = [];
        if (useEthanol) sources.push('ethanol');
        if (useEmitters) sources.push('emitters');
        countEl.textContent = `Top ${topSites.length} sites (${sources.join(' + ')})`;
        console.log(`🎯 Found ${topSites.length} optimal sites from ${sources.join(' + ')}`);

        this.reorderLayers();
    }

    showOptimalSitePopup(e) {
        const feature = e.features[0];
        const props = feature.properties;

        const formatEmissions = (val) => {
            if (!val) return 'No data';
            return Number(val).toLocaleString() + ' metric tons CO₂e';
        };

        const sourceIcon = props.sourceType === 'ethanol' ? '🌽' : '🏭';
        const sourceLabel = props.sourceType === 'ethanol' ? 'Ethanol Facility' : 'Point Source Emitter';
        const sourceColor = props.sourceType === 'ethanol' ? '#D4A017' : '#F44336';

        const popupHTML = `
            <div class="popup-content" style="max-width: 380px;">
                <div class="popup-header" style="background: linear-gradient(135deg, #FFD700, #FF8C00); color: #333; padding: 10px; margin: -10px -10px 10px -10px; border-radius: 4px 4px 0 0;">
                    <h3 style="margin: 0; font-size: 16px;">🎯 #${props.rank} Optimal Site</h3>
                    <div style="font-size: 11px; margin-top: 4px;">${sourceIcon} ${sourceLabel}</div>
                </div>
                <div style="background: #FFF8E1; padding: 10px; border-radius: 4px; margin-bottom: 10px;">
                    <div style="font-size: 24px; font-weight: bold; color: #FF6F00; text-align: center;">
                        Score: ${props.combinedScore}/100
                    </div>
                    <div style="display: flex; justify-content: space-around; margin-top: 8px; font-size: 12px;">
                        <div style="text-align: center;">
                            <div style="color: #666;">Emissions</div>
                            <div style="font-weight: bold; color: #C62828;">${props.emissionsScore}/100</div>
                        </div>
                        <div style="text-align: center;">
                            <div style="color: #666;">Geothermal</div>
                            <div style="font-weight: bold; color: #E65100;">${props.geoScore}/100</div>
                        </div>
                    </div>
                </div>
                <div class="popup-row" style="margin-bottom: 6px;">
                    <span style="font-weight: bold; color: #666;">Facility:</span>
                    <span style="color: #333;">${props.facilityName || 'Unknown'}</span>
                </div>
                <div class="popup-row" style="margin-bottom: 6px;">
                    <span style="font-weight: bold; color: #666;">Location:</span>
                    <span style="color: #333;">${props.siteName || 'N/A'}, ${props.state || ''}</span>
                </div>
                ${props.industryType ? `
                <div class="popup-row" style="margin-bottom: 6px;">
                    <span style="font-weight: bold; color: #666;">Industry:</span>
                    <span style="color: #333;">${props.industryType}</span>
                </div>
                ` : ''}
                <div class="popup-row" style="margin-bottom: 6px;">
                    <span style="font-weight: bold; color: #666;">Emissions:</span>
                    <span style="color: #C62828; font-weight: bold;">${formatEmissions(props.emissions)}</span>
                </div>
                <div class="popup-row" style="margin-bottom: 6px;">
                    <span style="font-weight: bold; color: #666;">Geothermal Temp:</span>
                    <span style="color: #E65100; font-weight: bold;">${props.geoTemp ? this.fToC(props.geoTemp) + '°C' : 'No data'}</span>
                </div>
                <div class="popup-row" style="margin-bottom: 6px;">
                    <span style="font-weight: bold; color: #666;">Geo Data Points:</span>
                    <span style="color: #333;">${props.geoPointCount || 0}</span>
                </div>
            </div>
        `;

        if (this.activePopup) {
            this.activePopup.remove();
        }

        this.activePopup = new maplibregl.Popup({
            closeButton: true,
            closeOnClick: false,
            maxWidth: '420px'
        })
            .setLngLat(e.lngLat)
            .setHTML(popupHTML)
            .addTo(this.map);
    }

    // ==== ETHANOL METHODS ====

    async addEthanolLayer() {
        console.log('🌽 Adding Ethanol Plants layer...');

        try {
            // Add empty GeoJSON source for ethanol plants
            this.map.addSource('ethanol-plants', {
                type: 'geojson',
                data: {
                    type: 'FeatureCollection',
                    features: []
                }
            });

            // Add ethanol points layer - using circles with corn/wheat color
            this.map.addLayer({
                id: 'ethanol-points',
                type: 'circle',
                source: 'ethanol-plants',
                paint: {
                    'circle-color': [
                        'case',
                        ['==', ['get', 'emissions_mt_co2e'], null], '#666666',  // Gray if no emissions
                        ['!', ['has', 'emissions_mt_co2e']], '#666666',          // Gray if no emissions
                        '#D4A017'  // Golden/wheat color if has emissions data
                    ],
                    'circle-radius': [
                        'case',
                        ['==', ['get', 'emissions_mt_co2e'], null], 6,
                        ['!', ['has', 'emissions_mt_co2e']], 6,
                        ['interpolate', ['linear'], ['get', 'emissions_mt_co2e'],
                            0, 6,
                            100000, 9,
                            300000, 13,
                            500000, 18
                        ]
                    ],
                    'circle-opacity': 0.9,
                    'circle-stroke-width': 2,
                    'circle-stroke-color': '#FFFFFF'
                }
            });

            // Add ethanol icon/text layer on top
            this.map.addLayer({
                id: 'ethanol-icons',
                type: 'symbol',
                source: 'ethanol-plants',
                layout: {
                    'text-field': '🌽',
                    'text-size': [
                        'case',
                        ['==', ['get', 'emissions_mt_co2e'], null], 12,
                        ['!', ['has', 'emissions_mt_co2e']], 12,
                        ['interpolate', ['linear'], ['get', 'emissions_mt_co2e'],
                            0, 12,
                            100000, 16,
                            300000, 22,
                            500000, 28
                        ]
                    ],
                    'text-allow-overlap': true,
                    'text-ignore-placement': true
                }
            });

            console.log('✅ Successfully added ethanol layer');

            // Load ethanol data
            await this.loadEthanol();

            // Set initial visibility (hidden by default)
            this.toggleEthanol(this.layerState.ethanolPlants);

            // Add click handler for ethanol
            this.map.on('click', 'ethanol-points', (e) => {
                this.showEthanolPopup(e);
            });

            // Add hover cursor
            this.map.on('mouseenter', 'ethanol-points', () => {
                this.map.getCanvas().style.cursor = 'pointer';
            });
            this.map.on('mouseleave', 'ethanol-points', () => {
                this.map.getCanvas().style.cursor = '';
            });

        } catch (error) {
            console.error('❌ Error adding ethanol layer:', error);
        }
    }

    async loadEthanol() {
        try {
            console.log('📡 Loading ethanol plants data...');

            const response = await fetch('/api/ethanol-plants', {
                headers: {
                    'Cache-Control': 'no-cache',
                    'Pragma': 'no-cache'
                }
            });

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }

            const data = await response.json();
            console.log(`🌽 Loaded ${data.features?.length || 0} ethanol plants`);

            // Update the map source
            const source = this.map.getSource('ethanol-plants');
            if (source) {
                source.setData(data);
                console.log('✅ Updated ethanol source with data');
            }

            // Update UI count
            const countEl = document.getElementById('ethanol-count');
            if (countEl) {
                const withEmissions = data.features?.filter(f => f.properties.emissions_mt_co2e).length || 0;
                countEl.textContent = `${data.features?.length || 0} plants (${withEmissions} with emissions data)`;
            }

            // Cache the data
            this.dataCache.ethanol = data;

        } catch (error) {
            console.error('❌ Error loading ethanol:', error);
            const countEl = document.getElementById('ethanol-count');
            if (countEl) {
                countEl.textContent = 'Error loading ethanol data';
            }
        }
    }

    toggleEthanol(visible) {
        const layers = ['ethanol-points', 'ethanol-icons'];
        layers.forEach(layerId => {
            try {
                if (this.map.getLayer(layerId)) {
                    this.map.setLayoutProperty(layerId, 'visibility', visible ? 'visible' : 'none');
                }
            } catch (error) {
                // Layer might not exist, ignore
            }
        });
        // Ensure proper layer ordering
        this.reorderLayers();
    }

    showEthanolPopup(e) {
        const feature = e.features[0];
        const props = feature.properties;

        // Format emissions
        const formatEmissions = (val) => {
            if (!val) return 'No data';
            return Number(val).toLocaleString() + ' metric tons CO₂e';
        };

        // Format capacity
        const formatCapacity = (val) => {
            if (!val) return 'N/A';
            return Number(val).toLocaleString() + ' MMgal/yr';
        };

        const popupHTML = `
            <div class="popup-content" style="max-width: 350px;">
                <div class="popup-header" style="background: #4CAF50; color: white; padding: 10px; margin: -10px -10px 10px -10px; border-radius: 4px 4px 0 0;">
                    <h3 style="margin: 0; font-size: 14px;">${props.Company || 'Unknown Company'}</h3>
                </div>
                <div class="popup-row" style="margin-bottom: 6px;">
                    <span style="font-weight: bold; color: #666;">Site:</span>
                    <span style="color: #333;">${props.Site || 'N/A'}</span>
                </div>
                <div class="popup-row" style="margin-bottom: 6px;">
                    <span style="font-weight: bold; color: #666;">State:</span>
                    <span style="color: #333;">${props.State || 'N/A'}</span>
                </div>
                <div class="popup-row" style="margin-bottom: 6px;">
                    <span style="font-weight: bold; color: #666;">Capacity:</span>
                    <span style="color: #333;">${formatCapacity(props.Cap_Mmgal)}</span>
                </div>
                <div class="popup-row" style="margin-bottom: 6px;">
                    <span style="font-weight: bold; color: #666;">PADD Region:</span>
                    <span style="color: #333;">${props.PADD || 'N/A'}</span>
                </div>
                ${props.emissions_mt_co2e ? `
                <div class="popup-row" style="margin-bottom: 6px; background: #E8F5E9; padding: 8px; border-radius: 4px;">
                    <span style="font-weight: bold; color: #2E7D32;">EPA Emissions:</span>
                    <span style="color: #2E7D32; font-weight: bold;">${formatEmissions(props.emissions_mt_co2e)}</span>
                </div>
                ` : `
                <div class="popup-row" style="margin-bottom: 6px; background: #FFF3E0; padding: 8px; border-radius: 4px;">
                    <span style="color: #E65100;">No emissions data available</span>
                </div>
                `}
                <div class="popup-row" style="margin-top: 8px; font-size: 11px; color: #666;">
                    <span>Source: ${props.Source || 'EIA'}</span>
                </div>
                ${this.getGeothermalInfoHTML(e.lngLat.lng, e.lngLat.lat)}
            </div>
        `;

        // Close existing popup
        if (this.activePopup) {
            this.activePopup.remove();
        }

        this.activePopup = new maplibregl.Popup({
            closeButton: true,
            closeOnClick: false,
            maxWidth: '400px'
        })
            .setLngLat(e.lngLat)
            .setHTML(popupHTML)
            .addTo(this.map);
    }

    // ==== GEOLOGY METHODS ====

    async addGeologyLayer() {
        console.log('🪨 Adding Geology layer...');

        try {
            // Add empty GeoJSON source for geology
            this.map.addSource('geology', {
                type: 'geojson',
                data: {
                    type: 'FeatureCollection',
                    features: []
                }
            });

            // Add geology layer as circles (point data for performance)
            // Color by depth to basement: shallow = green (favorable for HDR), deep = red
            this.map.addLayer({
                id: 'geology-fill',
                type: 'circle',
                source: 'geology',
                paint: {
                    'circle-color': [
                        'interpolate',
                        ['linear'],
                        ['get', 'dt'],  // depth to basement in meters
                        0, '#1B5E20',      // Deep green - basement at surface
                        1000, '#4CAF50',   // Green - very shallow (<1km)
                        2000, '#8BC34A',   // Light green - shallow (favorable)
                        3000, '#FFEB3B',   // Yellow - moderate (~3km)
                        5000, '#FF9800',   // Orange - getting deep
                        8000, '#FF5722',   // Deep orange - deep
                        12000, '#B71C1C'   // Dark red - very deep (unfavorable)
                    ],
                    'circle-radius': [
                        'interpolate', ['linear'], ['zoom'],
                        4, 4,    // Small at low zoom
                        6, 8,    // Medium at mid zoom
                        8, 12    // Larger at high zoom
                    ],
                    'circle-opacity': 0.7,
                    'circle-stroke-width': 0.5,
                    'circle-stroke-color': '#333',
                    'circle-stroke-opacity': 0.3
                }
            });

            console.log('✅ Successfully added geology layer');

            // Load geology data
            await this.loadGeology();

            // Apply initial filter (exclude mountains by default)
            this.updateGeologyFilter();

            // Set initial visibility (hidden by default)
            this.toggleGeology(this.layerState.geology);

            // Add click handler for geology
            this.map.on('click', 'geology-fill', (e) => {
                this.showGeologyPopup(e);
            });

            // Add hover cursor
            this.map.on('mouseenter', 'geology-fill', () => {
                this.map.getCanvas().style.cursor = 'pointer';
            });
            this.map.on('mouseleave', 'geology-fill', () => {
                this.map.getCanvas().style.cursor = '';
            });

        } catch (error) {
            console.error('❌ Error adding geology layer:', error);
        }
    }

    async loadGeology() {
        try {
            console.log('📡 Loading geology data...');

            const response = await fetch('/api/geology', {
                headers: {
                    'Cache-Control': 'no-cache',
                    'Pragma': 'no-cache'
                }
            });

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }

            const data = await response.json();
            console.log(`🪨 Loaded ${data.features?.length || 0} geology cells`);

            // Update the map source
            const source = this.map.getSource('geology');
            if (source) {
                source.setData(data);
                console.log('✅ Updated geology source with data');
            }

            // Update UI count
            const countEl = document.getElementById('geology-count');
            if (countEl) {
                const favorable = data.features?.filter(f => f.properties.q === 'F').length || 0;
                countEl.textContent = `${data.features?.length || 0} cells (${favorable} favorable)`;
            }

            // Cache the data
            this.dataCache.geology = data;

        } catch (error) {
            console.error('❌ Error loading geology:', error);
            const countEl = document.getElementById('geology-count');
            if (countEl) {
                countEl.textContent = 'Error loading geology data';
            }
        }
    }

    toggleGeology(visible) {
        const layers = ['geology-fill'];
        layers.forEach(layerId => {
            try {
                if (this.map.getLayer(layerId)) {
                    this.map.setLayoutProperty(layerId, 'visibility', visible ? 'visible' : 'none');
                }
            } catch (error) {
                // Layer might not exist, ignore
            }
        });
        this.layerState.geology = visible;
    }

    updateGeologyOpacity(opacity) {
        try {
            if (this.map.getLayer('geology-fill')) {
                this.map.setPaintProperty('geology-fill', 'circle-opacity', opacity);
            }
        } catch (error) {
            console.error('Error updating geology opacity:', error);
        }
    }

    updateGeologyFilter() {
        try {
            const minDepth = parseInt(document.getElementById('geology-min-depth').value) || 0;
            const overlapOnly = document.getElementById('geology-co2egs-overlap')?.checked || false;
            
            if (this.map.getLayer('geology-fill')) {
                // If overlap filter is active, we filter via data (not map filter)
                // So only apply minDepth filter here
                if (minDepth > 0) {
                    this.map.setFilter('geology-fill', ['>=', ['get', 'dt'], minDepth]);
                } else {
                    this.map.setFilter('geology-fill', null);
                }
                console.log(`🪨 Geology filter: minDepth=${minDepth}m`);
            }
        } catch (error) {
            console.error('Error updating geology filter:', error);
        }
    }

    async filterGeologyByCO2EGSOverlap(enableOverlap) {
        console.log(`🪨 Filtering geology by CO₂-EGS overlap: ${enableOverlap}`);
        
        try {
            const source = this.map.getSource('geology');
            if (!source) {
                console.error('Geology source not found');
                return;
            }

            const countEl = document.getElementById('geology-count');

            if (!enableOverlap) {
                // Restore full geology data
                if (this.dataCache.geologyFull) {
                    source.setData(this.dataCache.geologyFull);
                    if (countEl) {
                        const favorable = this.dataCache.geologyFull.features?.filter(f => f.properties.q === 'F').length || 0;
                        countEl.textContent = `${this.dataCache.geologyFull.features?.length || 0} cells (${favorable} favorable)`;
                    }
                }
                return;
            }

            // Get CO₂-EGS data (5km depth, 150°C+ threshold)
            const co2egsData = this.dataCache['compare-co2egs'];
            if (!co2egsData || !co2egsData.features || co2egsData.features.length === 0) {
                console.warn('CO₂-EGS data not loaded yet. Please enable the CO₂-EGS layer first.');
                alert('Please enable the "CO₂-EGS (5km, 150°C+)" layer in the Comparison section first to load the data.');
                document.getElementById('geology-co2egs-overlap').checked = false;
                return;
            }

            // Get full geology data
            const geologyData = this.dataCache.geologyFull || this.dataCache.geology;
            if (!geologyData || !geologyData.features) {
                console.error('Geology data not loaded');
                return;
            }

            // Store full data for restoration
            if (!this.dataCache.geologyFull) {
                this.dataCache.geologyFull = JSON.parse(JSON.stringify(geologyData));
            }

            // Show loading state
            if (countEl) {
                countEl.textContent = 'Filtering... please wait';
            }

            // Use setTimeout to allow UI to update before heavy computation
            await new Promise(resolve => setTimeout(resolve, 50));

            console.log(`🪨 Filtering ${geologyData.features.length} geology points against ${co2egsData.features.length} CO₂-EGS hexagons...`);

            // Build a Set of grid cells that contain viable CO₂-EGS hexagons
            // Using a coarser grid (1 degree) for faster lookup with bounding box only
            const GRID_SIZE = 1.0;
            const co2egsGridSet = new Set();
            const CO2_EGS_MIN_F = 302; // 150°C threshold
            
            for (const hex of co2egsData.features) {
                const temp = hex.properties?.avg_temperature_f;
                if (!temp || temp < CO2_EGS_MIN_F) continue;
                
                const coords = hex.geometry?.coordinates?.[0];
                if (!coords) continue;
                
                // Get bounding box and add all grid cells it touches
                let minLon = Infinity, maxLon = -Infinity, minLat = Infinity, maxLat = -Infinity;
                for (const pt of coords) {
                    minLon = Math.min(minLon, pt[0]);
                    maxLon = Math.max(maxLon, pt[0]);
                    minLat = Math.min(minLat, pt[1]);
                    maxLat = Math.max(maxLat, pt[1]);
                }
                
                // Add all grid cells this hexagon touches
                for (let lon = Math.floor(minLon / GRID_SIZE); lon <= Math.floor(maxLon / GRID_SIZE); lon++) {
                    for (let lat = Math.floor(minLat / GRID_SIZE); lat <= Math.floor(maxLat / GRID_SIZE); lat++) {
                        co2egsGridSet.add(`${lon},${lat}`);
                    }
                }
            }

            console.log(`🪨 Built spatial index with ${co2egsGridSet.size} grid cells containing CO₂-EGS areas`);

            // Fast filter: only keep geology points in grid cells that have CO₂-EGS coverage
            const filteredFeatures = [];
            
            for (const feature of geologyData.features) {
                const [lon, lat] = feature.geometry.coordinates;
                const gridKey = `${Math.floor(lon / GRID_SIZE)},${Math.floor(lat / GRID_SIZE)}`;
                
                if (co2egsGridSet.has(gridKey)) {
                    filteredFeatures.push(feature);
                }
            }

            console.log(`🪨 Filtered to ${filteredFeatures.length} geology points within CO₂-EGS grid cells`);

            // Update the source with filtered data
            const filteredData = {
                type: 'FeatureCollection',
                features: filteredFeatures
            };
            source.setData(filteredData);

            // Update count
            if (countEl) {
                const favorable = filteredFeatures.filter(f => f.properties.q === 'F').length;
                countEl.textContent = `${filteredFeatures.length} cells in CO₂-EGS zones (${favorable} favorable)`;
            }

        } catch (error) {
            console.error('Error filtering geology by CO₂-EGS overlap:', error);
        }
    }

    showGeologyPopup(e) {
        const feature = e.features[0];
        const props = feature.properties;

        // Format values
        const formatMeters = (val) => {
            if (val === null || val === undefined) return 'N/A';
            return Number(val).toLocaleString() + ' m';
        };

        // Decode qualitative label from abbreviated form
        const qualMap = { 'F': 'Favorable', 'M': 'Moderate', 'U': 'Unfavorable' };
        const qualitative = qualMap[props.q] || 'Unknown';

        // Determine qualitative color
        const qualColors = {
            'Favorable': '#4CAF50',
            'Moderate': '#FF9800',
            'Unfavorable': '#F44336'
        };
        const qualColor = qualColors[qualitative] || '#666';

        const popupHTML = `
            <div class="popup-content" style="max-width: 350px;">
                <div class="popup-header" style="background: #795548; color: white; padding: 10px; margin: -10px -10px 10px -10px; border-radius: 4px 4px 0 0;">
                    <h3 style="margin: 0; font-size: 14px;">🪨 Hot Dry Rock Access</h3>
                </div>
                <div class="popup-row" style="margin-bottom: 8px; padding: 8px; background: ${qualColor}22; border-left: 4px solid ${qualColor}; border-radius: 4px;">
                    <span style="font-weight: bold; color: ${qualColor};">${qualitative}</span>
                    <span style="color: #666; font-size: 11px;"> for HDR/EGS drilling</span>
                </div>
                <div class="popup-row" style="margin-bottom: 6px;">
                    <span style="font-weight: bold; color: #666;">Depth to Basement (HDR):</span>
                    <span style="color: #333;">${formatMeters(props.dt)}</span>
                </div>
                <div class="popup-row" style="margin-bottom: 6px;">
                    <span style="font-weight: bold; color: #666;">Overburden Thickness:</span>
                    <span style="color: #333;">${formatMeters(props.st)}</span>
                </div>
                <div class="popup-row" style="margin-top: 8px; font-size: 11px; color: #666;">
                    <span>Source: USGS 3-Layer Geologic Model</span>
                </div>
            </div>
        `;

        // Close existing popup
        if (this.activePopup) {
            this.activePopup.remove();
        }

        this.activePopup = new maplibregl.Popup({
            closeButton: true,
            closeOnClick: false,
            maxWidth: '400px'
        })
            .setLngLat(e.lngLat)
            .setHTML(popupHTML)
            .addTo(this.map);
    }

    // ==== EMITTERS METHODS ====

    async addEmittersLayer() {
        console.log('🏭 Adding Point Source Emitters layer...');

        try {
            // Add empty GeoJSON source for emitters
            this.map.addSource('emitters', {
                type: 'geojson',
                data: {
                    type: 'FeatureCollection',
                    features: []
                }
            });

            // Add emitters points layer with colors by industry type
            // Circle size scales with emissions, gray with X for no data
            this.map.addLayer({
                id: 'emitter-points',
                type: 'circle',
                source: 'emitters',
                paint: {
                    'circle-color': [
                        'case',
                        // Gray for no emissions data
                        ['==', ['get', 'total_emissions_2023'], null], '#888888',
                        ['==', ['get', 'total_emissions_2023'], 0], '#888888',
                        // Colors by industry type
                        ['==', ['get', 'industry_type_sectors'], 'Power Plants'], '#F44336',
                        ['all', ['has', 'industry_type_sectors'], ['in', 'Petroleum', ['get', 'industry_type_sectors']]], '#FF9800',
                        ['all', ['has', 'industry_type_sectors'], ['in', 'Natural Gas', ['get', 'industry_type_sectors']]], '#FF9800',
                        ['==', ['get', 'industry_type_sectors'], 'Waste'], '#795548',
                        ['all', ['has', 'industry_type_sectors'], ['in', 'Waste', ['get', 'industry_type_sectors']]], '#795548',
                        ['==', ['get', 'industry_type_sectors'], 'Chemicals'], '#9C27B0',
                        ['all', ['has', 'industry_type_sectors'], ['in', 'Chemicals', ['get', 'industry_type_sectors']]], '#9C27B0',
                        ['==', ['get', 'industry_type_sectors'], 'Minerals'], '#607D8B',
                        ['==', ['get', 'industry_type_sectors'], 'Metals'], '#455A64',
                        '#9E9E9E'  // Default gray for Other
                    ],
                    'circle-radius': [
                        'interpolate', ['linear'], 
                        ['coalesce', ['to-number', ['get', 'total_emissions_2023']], 0],
                        0, 5,
                        50000, 7,
                        100000, 9,
                        500000, 12,
                        1000000, 15,
                        5000000, 20,
                        16000000, 28
                    ],
                    'circle-opacity': [
                        'case',
                        ['==', ['get', 'total_emissions_2023'], null], 0.5,
                        ['==', ['get', 'total_emissions_2023'], 0], 0.5,
                        0.8
                    ],
                    'circle-stroke-width': 2,
                    'circle-stroke-color': '#FFFFFF'
                }
            });

            // Add X symbol for emitters without emissions data
            this.map.addLayer({
                id: 'emitter-no-data',
                type: 'symbol',
                source: 'emitters',
                filter: ['any', 
                    ['==', ['get', 'total_emissions_2023'], null],
                    ['==', ['get', 'total_emissions_2023'], 0]
                ],
                layout: {
                    'text-field': '✕',
                    'text-size': 10,
                    'text-allow-overlap': true
                },
                paint: {
                    'text-color': '#FFFFFF',
                    'text-opacity': 0.8
                }
            });

            console.log('✅ Successfully added emitters layer');

            // Load emitters data
            await this.loadEmitters();

            // Set initial visibility
            this.toggleEmitters(this.layerState.emitters);

            // Add click handler for emitters
            this.map.on('click', 'emitter-points', (e) => {
                this.showEmitterPopup(e);
            });

            // Add hover cursor
            this.map.on('mouseenter', 'emitter-points', () => {
                this.map.getCanvas().style.cursor = 'pointer';
            });
            this.map.on('mouseleave', 'emitter-points', () => {
                this.map.getCanvas().style.cursor = '';
            });

        } catch (error) {
            console.error('❌ Error adding emitters layer:', error);
        }
    }

    async loadEmitters() {
        try {
            console.log('📡 Loading point source emitters data...');

            const response = await fetch('/api/emitters', {
                headers: {
                    'Cache-Control': 'no-cache',
                    'Pragma': 'no-cache'
                }
            });

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }

            const data = await response.json();
            console.log(`🏭 Loaded ${data.features?.length || 0} point source emitters`);

            // Update the map source
            const source = this.map.getSource('emitters');
            if (source) {
                source.setData(data);
                console.log('✅ Updated emitters source with data');
            }

            // Update UI count
            const countEl = document.getElementById('emitters-count');
            if (countEl) {
                countEl.textContent = `${data.features?.length || 0} emitters loaded`;
            }

            // Update slider max based on actual data
            if (data.metadata?.max_emissions) {
                const slider = document.getElementById('emissions-slider');
                if (slider) {
                    slider.max = Math.ceil(data.metadata.max_emissions);
                }
            }

            // Cache the data
            this.dataCache.emitters = data;

        } catch (error) {
            console.error('❌ Error loading emitters:', error);
            const countEl = document.getElementById('emitters-count');
            if (countEl) {
                countEl.textContent = 'Error loading emitters data';
            }
        }
    }

    toggleEmitters(visible) {
        const layers = ['emitter-points', 'emitter-no-data'];
        layers.forEach(layerId => {
            try {
                if (this.map.getLayer(layerId)) {
                    this.map.setLayoutProperty(layerId, 'visibility', visible ? 'visible' : 'none');
                }
            } catch (error) {
                // Layer might not exist, ignore
            }
        });
        this.reorderLayers();
    }

    updateEmittersFilter() {
        if (!this.map.getLayer('emitter-points')) return;
        if (!this.dataCache.emitters) return;

        // Filter the cached data and update the source directly (faster than complex map filters)
        const allFeatures = this.dataCache.emitters.features || [];
        
        const filteredFeatures = allFeatures.filter(f => {
            const props = f.properties;
            const emissions = props.total_emissions_2023 || 0;
            const sector = props.industry_type_sectors || '';
            
            // Check emissions threshold
            if (emissions < this.layerState.emitterMinEmissions) return false;
            
            // Check category
            if (sector === 'Power Plants' || sector.includes('Power Plants')) {
                return this.layerState.emitterPowerPlants;
            }
            if (sector === 'Petroleum and Natural Gas Systems' || sector.includes('Petroleum') || sector.includes('Natural Gas')) {
                return this.layerState.emitterPetroleum;
            }
            if (sector === 'Waste' || sector.includes('Waste')) {
                return this.layerState.emitterWaste;
            }
            if (sector === 'Chemicals' || sector.includes('Chemicals')) {
                return this.layerState.emitterChemicals;
            }
            if (sector === 'Minerals' || sector.includes('Minerals')) {
                return this.layerState.emitterMinerals;
            }
            if (sector === 'Metals' || sector.includes('Metals')) {
                return this.layerState.emitterMetals;
            }
            // Other category
            return this.layerState.emitterOther;
        });

        // Update the source with filtered data
        const source = this.map.getSource('emitters');
        if (source) {
            source.setData({
                type: 'FeatureCollection',
                features: filteredFeatures
            });
        }

        // Update count
        const countEl = document.getElementById('emitters-count');
        if (countEl) {
            countEl.textContent = `${filteredFeatures.length} of ${allFeatures.length} emitters shown`;
        }

        console.log(`🏭 Emitters filter updated: showing ${filteredFeatures.length} of ${allFeatures.length}`);
    }

    showEmitterPopup(e) {
        const feature = e.features[0];
        const props = feature.properties;

        // Format emissions with commas
        const formatEmissions = (val) => {
            if (!val) return 'N/A';
            return Number(val).toLocaleString() + ' metric tons CO₂e';
        };

        const popupHTML = `
            <div class="popup-content" style="max-width: 350px;">
                <div class="popup-header" style="background: #F44336; color: white; padding: 10px; margin: -10px -10px 10px -10px; border-radius: 4px 4px 0 0;">
                    <h3 style="margin: 0; font-size: 14px;">${props.facility_name || 'Unknown Facility'}</h3>
                </div>
                <div class="popup-row" style="margin-bottom: 6px;">
                    <span style="font-weight: bold; color: #666;">Address:</span>
                    <span style="color: #333;">${props.address || 'N/A'}</span>
                </div>
                <div class="popup-row" style="margin-bottom: 6px;">
                    <span style="font-weight: bold; color: #666;">City:</span>
                    <span style="color: #333;">${props.city || 'N/A'}, ${props.state || ''} ${props.zip_code || ''}</span>
                </div>
                <div class="popup-row" style="margin-bottom: 6px;">
                    <span style="font-weight: bold; color: #666;">County:</span>
                    <span style="color: #333;">${props.county || 'N/A'}</span>
                </div>
                <div class="popup-row" style="margin-bottom: 6px;">
                    <span style="font-weight: bold; color: #666;">Industry Type:</span>
                    <span style="color: #333;">${props.industry_type_sectors || 'N/A'}</span>
                </div>
                <div class="popup-row" style="margin-bottom: 6px;">
                    <span style="font-weight: bold; color: #666;">Subparts:</span>
                    <span style="color: #333;">${props.industry_type_subparts || 'N/A'}</span>
                </div>
                <div class="popup-row" style="margin-bottom: 6px;">
                    <span style="font-weight: bold; color: #666;">NAICS Code:</span>
                    <span style="color: #333;">${props.primary_naics_code || 'N/A'}</span>
                </div>
                <div class="popup-row" style="margin-bottom: 6px; background: #FFEBEE; padding: 8px; border-radius: 4px;">
                    <span style="font-weight: bold; color: #C62828;">2023 Emissions:</span>
                    <span style="color: #C62828; font-weight: bold;">${formatEmissions(props.total_emissions_2023)}</span>
                </div>
                <div class="popup-row" style="margin-top: 8px; font-size: 11px; color: #666;">
                    <span>Facility ID: ${props.facility_id || 'N/A'} | FRS ID: ${props.frs_id || 'N/A'}</span>
                </div>
                ${this.getGeothermalInfoHTML(e.lngLat.lng, e.lngLat.lat)}
            </div>
        `;

        // Close existing popup
        if (this.activePopup) {
            this.activePopup.remove();
        }

        this.activePopup = new maplibregl.Popup({
            closeButton: true,
            closeOnClick: false,
            maxWidth: '400px'
        })
            .setLngLat(e.lngLat)
            .setHTML(popupHTML)
            .addTo(this.map);
    }

    showCCUSPopup(e) {
        const feature = e.features[0];
        const props = feature.properties;

        // Format capacity with commas
        const formatCapacity = (val) => {
            if (!val) return 'N/A';
            return Number(val).toLocaleString() + ' metric tons/year';
        };

        const popupHTML = `
            <div class="popup-content" style="max-width: 350px;">
                <div class="popup-header" style="background: #9C27B0; color: white; padding: 10px; margin: -10px -10px 10px -10px; border-radius: 4px 4px 0 0;">
                    <h3 style="margin: 0; font-size: 14px;">${props.project_name || 'Unknown Project'}</h3>
                </div>
                <div class="popup-row" style="margin-bottom: 6px;">
                    <span style="font-weight: bold; color: #666;">Entities:</span>
                    <span style="color: #333;">${props.entities || 'N/A'}</span>
                </div>
                <div class="popup-row" style="margin-bottom: 6px;">
                    <span style="font-weight: bold; color: #666;">Location:</span>
                    <span style="color: #333;">${props.location || 'N/A'}, ${props.state || ''}</span>
                </div>
                <div class="popup-row" style="margin-bottom: 6px;">
                    <span style="font-weight: bold; color: #666;">Status:</span>
                    <span style="color: #333; font-weight: bold;">${props.status || 'N/A'}</span>
                </div>
                <div class="popup-row" style="margin-bottom: 6px;">
                    <span style="font-weight: bold; color: #666;">Sector:</span>
                    <span style="color: #333;">${props.sector_description || props.sector_classification || 'N/A'}</span>
                </div>
                <div class="popup-row" style="margin-bottom: 6px;">
                    <span style="font-weight: bold; color: #666;">Subsector:</span>
                    <span style="color: #333;">${props.subsector_description || props.subsector_classification || 'N/A'}</span>
                </div>
                <div class="popup-row" style="margin-bottom: 6px;">
                    <span style="font-weight: bold; color: #666;">Capacity:</span>
                    <span style="color: #333;">${formatCapacity(props.capacity)}</span>
                </div>
                <div class="popup-row" style="margin-bottom: 6px;">
                    <span style="font-weight: bold; color: #666;">Storage Type:</span>
                    <span style="color: #333;">${props.storage_description || props.storage_classification || 'N/A'}</span>
                </div>
                <div class="popup-row" style="margin-bottom: 6px;">
                    <span style="font-weight: bold; color: #666;">Year Announced:</span>
                    <span style="color: #333;">${props.year_announced || 'N/A'}</span>
                </div>
                <div class="popup-row" style="margin-bottom: 6px;">
                    <span style="font-weight: bold; color: #666;">Year Operational:</span>
                    <span style="color: #333;">${props.year_operational || 'N/A'}</span>
                </div>
                ${props.capture_storage_details ? `
                <div class="popup-row" style="margin-bottom: 6px;">
                    <span style="font-weight: bold; color: #666;">Details:</span>
                    <span style="color: #333; font-size: 11px;">${props.capture_storage_details.substring(0, 200)}${props.capture_storage_details.length > 200 ? '...' : ''}</span>
                </div>
                ` : ''}
                ${props.reference ? `
                <div class="popup-row" style="margin-top: 10px;">
                    <a href="${props.reference}" target="_blank" style="color: #2196F3; font-size: 11px;">View Reference →</a>
                </div>
                ` : ''}
                ${this.getGeothermalInfoHTML(e.lngLat.lng, e.lngLat.lat)}
            </div>
        `;

        // Close existing popup
        if (this.activePopup) {
            this.activePopup.remove();
        }

        this.activePopup = new maplibregl.Popup({
            closeButton: true,
            closeOnClick: false,
            maxWidth: '400px'
        })
            .setLngLat(e.lngLat)
            .setHTML(popupHTML)
            .addTo(this.map);
    }

    toggleDatacenters(visible) {
        const layers = ['datacenter-points'];
        layers.forEach(layerId => {
            try {
                if (this.map.getLayer(layerId)) {
                    this.map.setLayoutProperty(layerId, 'visibility', visible ? 'visible' : 'none');
                }
            } catch (error) {
                // Layer might not exist, ignore
            }
        });
        this.reorderLayers();
    }

    toggleCCUS(visible) {
        const layers = ['ccus-points'];
        layers.forEach(layerId => {
            try {
                if (this.map.getLayer(layerId)) {
                    this.map.setLayoutProperty(layerId, 'visibility', visible ? 'visible' : 'none');
                }
            } catch (error) {
                // Layer might not exist, ignore
            }
        });
        this.reorderLayers();
    }

    updateCCUSFilter() {
        if (!this.map.getLayer('ccus-points')) return;

        // Build filter based on selected categories
        const allowedCategories = [];
        
        if (this.layerState.ccusSaline) {
            allowedCategories.push('Dedicated Saline Storage');
            allowedCategories.push('Dedicated Saline Storage & Enhanced Oil Recovery');
        }
        if (this.layerState.ccusEOR) {
            allowedCategories.push('Enhanced Oil Recovery');
            allowedCategories.push('Dedicated Saline Storage & Enhanced Oil Recovery');
        }
        if (this.layerState.ccusUtilization) {
            allowedCategories.push('Utilization');
        }
        if (this.layerState.ccusOther) {
            allowedCategories.push('Other');
            allowedCategories.push('Unavailable');
            allowedCategories.push(null);
            allowedCategories.push('');
        }

        // Remove duplicates
        const uniqueCategories = [...new Set(allowedCategories)];

        // Apply filter
        if (uniqueCategories.length === 0) {
            // Hide all
            this.map.setFilter('ccus-points', ['==', ['get', 'storage_classification'], '__none__']);
        } else {
            this.map.setFilter('ccus-points', [
                'any',
                ['in', ['get', 'storage_classification'], ['literal', uniqueCategories]],
                ...(this.layerState.ccusOther ? [['!', ['has', 'storage_classification']]] : [])
            ]);
        }

        console.log(`🏭 CCUS filter updated: ${uniqueCategories.length} categories visible`);
    }

    showDatacenterPopup(e) {
        const feature = e.features[0];
        const props = feature.properties;

        // Parse JSONB fields if they're strings
        let certifications = [];
        let features = {};

        try {
            certifications = props.certifications ? JSON.parse(props.certifications) : [];
        } catch (e) {
            certifications = [];
        }

        try {
            features = props.features ? JSON.parse(props.features) : {};
        } catch (e) {
            features = {};
        }

        // Build certifications HTML
        let certificationsHTML = '';
        if (certifications && certifications.length > 0) {
            certificationsHTML = `
                <div class="popup-row">
                    <span class="popup-label">Certifications:</span>
                    <span class="popup-value">${certifications.join(', ')}</span>
                </div>
            `;
        }

        // Build features HTML
        let featuresHTML = '';
        const activeFeatures = Object.entries(features).filter(([k, v]) => v).map(([k, v]) => k.replace(/_/g, ' '));
        if (activeFeatures.length > 0) {
            featuresHTML = `
                <div class="popup-row">
                    <span class="popup-label">Features:</span>
                    <span class="popup-value">${activeFeatures.join(', ')}</span>
                </div>
            `;
        }

        const html = `
            <div class="popup-header">🏢 ${props.name || 'Datacenter Facility'}</div>
            <div class="popup-row">
                <span class="popup-label">Operator:</span>
                <span class="popup-value">${props.operator || 'N/A'}</span>
            </div>
            ${props.street_address ? `
            <div class="popup-row">
                <span class="popup-label">Address:</span>
                <span class="popup-value">${props.street_address}</span>
            </div>
            ` : ''}
            <div class="popup-row">
                <span class="popup-label">Location:</span>
                <span class="popup-value">${props.city || 'N/A'}, ${props.state || 'N/A'}</span>
            </div>
            ${props.market_region ? `
            <div class="popup-row">
                <span class="popup-label">Market:</span>
                <span class="popup-value">${props.market_region}</span>
            </div>
            ` : ''}
            ${props.power_capacity_mw ? `
            <div class="popup-row">
                <span class="popup-label">Power Capacity:</span>
                <span class="popup-value">${props.power_capacity_mw} MW</span>
            </div>
            ` : ''}
            ${props.square_footage ? `
            <div class="popup-row">
                <span class="popup-label">Size:</span>
                <span class="popup-value">${props.square_footage.toLocaleString()} sq ft</span>
            </div>
            ` : ''}
            ${props.facility_type ? `
            <div class="popup-row">
                <span class="popup-label">Type:</span>
                <span class="popup-value">${props.facility_type}</span>
            </div>
            ` : ''}
            ${props.phone_number ? `
            <div class="popup-row">
                <span class="popup-label">Phone:</span>
                <span class="popup-value">${props.phone_number}</span>
            </div>
            ` : ''}
            ${props.miles_to_airport ? `
            <div class="popup-row">
                <span class="popup-label">Airport Distance:</span>
                <span class="popup-value">${props.miles_to_airport} miles</span>
            </div>
            ` : ''}
            ${props.nearby_datacenter_count ? `
            <div class="popup-row">
                <span class="popup-label">Nearby Facilities:</span>
                <span class="popup-value">${props.nearby_datacenter_count} within 50 miles</span>
            </div>
            ` : ''}
            ${certificationsHTML}
            ${featuresHTML}
            ${props.has_images || props.has_brochures || props.has_media ? `
            <div class="popup-row">
                <span class="popup-label">Media:</span>
                <span class="popup-value">
                    ${props.has_images ? '📷 Images ' : ''}
                    ${props.has_brochures ? '📄 Brochures ' : ''}
                    ${props.has_media ? '🎥 Media ' : ''}
                </span>
            </div>
            ` : ''}
            ${props.facility_url ? `
            <div class="popup-row" style="margin-top: 8px; padding-top: 8px; border-top: 1px solid #eee;">
                <a href="${props.facility_url}" target="_blank" rel="noopener noreferrer"
                   style="color: #2196F3; text-decoration: none; font-size: 13px; display: flex; align-items: center; gap: 4px;">
                    🔗 View Full Details on datacenters.com
                </a>
            </div>
            ` : ''}
        `;

        new maplibregl.Popup()
            .setLngLat(feature.geometry.coordinates)
            .setHTML(html)
            .addTo(this.map);
    }

    async updateDatacenters() {
        const button = document.getElementById('update-datacenters-btn');
        const icon = button.querySelector('i');

        try {
            // Disable button and show loading state
            button.disabled = true;
            button.classList.add('loading');
            button.innerHTML = '<i class="fas fa-sync-alt"></i> Updating...';

            // Disable datacenter layer toggle during update
            const datacenterToggle = document.getElementById('datacenter-toggle');
            datacenterToggle.disabled = true;

            console.log('🔄 Starting datacenter scraper...');

            // Connect to the scraper endpoint with Server-Sent Events
            const response = await fetch('/api/scrape-datacenters', {
                method: 'POST',
                headers: {
                    'Accept': 'text/event-stream',
                }
            });

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }

            const reader = response.body.getReader();
            const decoder = new TextDecoder();

            while (true) {
                const { value, done } = await reader.read();
                if (done) break;

                const chunk = decoder.decode(value);
                const lines = chunk.split('\n');

                for (const line of lines) {
                    if (line.startsWith('data: ')) {
                        try {
                            const data = JSON.parse(line.substring(6));
                            this.handleDatacenterScraperProgress(data, button);
                        } catch (e) {
                            console.warn('Failed to parse SSE data:', line);
                        }
                    }
                }
            }

        } catch (error) {
            console.error('❌ Error updating datacenters:', error);
            button.innerHTML = '<i class="fas fa-exclamation-triangle"></i> Update Failed';
            setTimeout(() => {
                button.innerHTML = '<i class="fas fa-sync-alt"></i> Update Data Centers';
                button.disabled = false;
                button.classList.remove('loading');
            }, 3000);
        } finally {
            // Re-enable datacenter layer toggle
            const datacenterToggle = document.getElementById('datacenter-toggle');
            datacenterToggle.disabled = false;
        }
    }

    handleDatacenterScraperProgress(data, button) {
        const { status, message, currentPage, totalPages, currentListing, totalListings } = data;

        console.log(`📡 Datacenter scraper progress: ${status} - ${message}`);

        switch (status) {
            case 'starting':
                button.innerHTML = '<i class="fas fa-sync-alt"></i> Starting...';
                break;

            case 'discovering':
                if (currentPage && totalPages) {
                    const progress = Math.round((currentPage / totalPages) * 100);
                    button.innerHTML = `<i class="fas fa-search"></i> Page ${currentPage}/${totalPages} (${progress}%)`;
                } else {
                    button.innerHTML = '<i class="fas fa-search"></i> Discovering...';
                }
                break;

            case 'discovered':
                button.innerHTML = `<i class="fas fa-list"></i> Found ${totalListings} facilities`;
                break;

            case 'scraping':
                if (currentListing && totalListings) {
                    const progress = Math.round((currentListing / totalListings) * 100);
                    button.innerHTML = `<i class="fas fa-cog"></i> ${progress}% (${currentListing}/${totalListings})`;
                } else {
                    button.innerHTML = '<i class="fas fa-cog"></i> Scraping...';
                }
                break;

            case 'complete':
                button.innerHTML = '<i class="fas fa-check"></i> Update Complete';
                console.log('✅ Datacenter scraper completed successfully');

                // Refresh the datacenter data
                setTimeout(async () => {
                    console.log('🔄 Refreshing datacenter data...');
                    await this.loadDatacenters();

                    // Reset button
                    button.innerHTML = '<i class="fas fa-sync-alt"></i> Update Data Centers';
                    button.disabled = false;
                    button.classList.remove('loading');
                }, 1000);
                break;

            case 'error':
                button.innerHTML = '<i class="fas fa-exclamation-triangle"></i> Error';
                console.error('❌ Datacenter scraper error:', message);
                setTimeout(() => {
                    button.innerHTML = '<i class="fas fa-sync-alt"></i> Update Data Centers';
                    button.disabled = false;
                    button.classList.remove('loading');
                }, 3000);
                break;
        }
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
        
        // Convert temperatures to Celsius
        const avgTempC = this.fToC(props.avg_temperature_f);
        const minTempC = this.fToC(props.min_temperature_f);
        const maxTempC = this.fToC(props.max_temperature_f);
        
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
                    <span class="popup-value">${avgTempC}°C</span>
                </div>
                <div class="popup-row">
                    <span class="popup-label">Temperature Range:</span>
                    <span class="popup-value">${minTempC !== null ? minTempC : 'N/A'}°C - ${maxTempC !== null ? maxTempC : 'N/A'}°C</span>
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
                    <span class="popup-value">65 square miles</span>
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
                    <span class="popup-value">65 square miles</span>
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
    const app = new GeospatialApp();
    // Make available globally for debugging
    window.geoApp = app;
    console.log('🔧 Debug: Type window.geoApp.mapManager.forceRefreshEnergyNet() to force refresh EnergyNet data');
});