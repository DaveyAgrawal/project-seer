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
            geothermalPoints: true
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
            maxBounds: this.config.map.maxBounds
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
        const gridDataset = this.datasets.find(d => d.geometry_type === 'POLYGON');
        
        console.log('Available datasets:', this.datasets.length);
        console.log('Transmission dataset:', transmissionDataset ? 'Found' : 'Not found');
        console.log('Geothermal dataset:', geothermalDataset ? 'Found' : 'Not found');
        console.log('Grid dataset:', gridDataset ? 'Found' : 'Not found');
        
        if (geothermalDataset) {
            console.log('Geothermal dataset details:', geothermalDataset);
        }
        if (gridDataset) {
            console.log('Grid dataset details:', gridDataset);
        }
        
        // Add transmission lines source with zoom-banded views (only if dataset exists)
        if (transmissionDataset) {
            this.map.addSource('transmission-lines', {
                type: 'vector',
                tiles: [
                    `${tileserverUrl}/public.${transmissionDataset.table_name}_us_z0_6/{z}/{x}/{y}.mvt`,
                    `${tileserverUrl}/public.${transmissionDataset.table_name}_us_z7_10/{z}/{x}/{y}.mvt`,
                    `${tileserverUrl}/public.${transmissionDataset.table_name}_us_z11_14/{z}/{x}/{y}.mvt`
                ],
                minzoom: 3,
                maxzoom: 14
            });
            console.log('Added transmission lines source');
        }
        
        // Add geothermal sources (only if dataset exists)
        if (geothermalDataset) {
            const tileUrl = `${tileserverUrl}/public.${geothermalDataset.table_name}_us/{z}/{x}/{y}.mvt`;
            console.log('🌡️ Adding geothermal source...');
            console.log('Tile URL:', tileUrl);
            
            try {
                // Use main geothermal view for all zoom levels
                this.map.addSource('geothermal-points', {
                    type: 'vector',
                    tiles: [tileUrl],
                    minzoom: 3,  // Allow lower zoom for testing
                    maxzoom: 14
                });
                console.log('✅ Added geothermal source with 4.2M+ points');
            } catch (error) {
                console.error('❌ Error adding geothermal source:', error);
            }
        } else {
            console.warn('⚠️ No geothermal dataset found');
        }
        
        // Add grid sources (only if dataset exists)
        if (gridDataset) {
            const gridTileUrl = `${tileserverUrl}/public.${gridDataset.table_name}/{z}/{x}/{y}.mvt`;
            console.log('🔲 Adding grid source...');
            console.log('Grid Tile URL:', gridTileUrl);
            
            try {
                this.map.addSource('geothermal-grid', {
                    type: 'vector',
                    tiles: [gridTileUrl],
                    minzoom: 0,
                    maxzoom: 8
                });
                console.log('✅ Added geothermal grid source with 13,696 boxes');
            } catch (error) {
                console.error('❌ Error adding grid source:', error);
            }
        } else {
            console.warn('⚠️ No grid dataset found');
        }
        
        // Update UI to show dataset status
        this.updateDatasetStatus(transmissionDataset, geothermalDataset, gridDataset);
    }

    addDataLayers() {
        // Add transmission lines layers (zoom-banded)
        this.addTransmissionLinesLayer('transmission-lines-z0-6', 'transmission-lines', 0, 6);
        this.addTransmissionLinesLayer('transmission-lines-z7-10', 'transmission-lines', 7, 10);
        this.addTransmissionLinesLayer('transmission-lines-z11-14', 'transmission-lines', 11, 14);
        
        // Add geothermal layer
        this.addGeothermalPointsLayer();
        
        // Add grid layer
        this.addGeothermalGridLayer();
        
        // Debug: Log all map layers and check for errors
        setTimeout(() => {
            console.log('Map layers after adding all layers:', this.map.getStyle().layers.map(l => l.id));
            console.log('Map sources:', Object.keys(this.map.getStyle().sources));
            
            // Check if layers are visible
            const gridLayer = this.map.getLayer('geothermal-grid');
            const pointsLayer = this.map.getLayer('geothermal-points');
            
            console.log('Grid layer exists:', !!gridLayer);
            console.log('Points layer exists:', !!pointsLayer);
            
            if (gridLayer) {
                console.log('Grid layer visibility:', this.map.getLayoutProperty('geothermal-grid', 'visibility'));
                console.log('Grid layer opacity:', this.map.getPaintProperty('geothermal-grid', 'fill-opacity'));
            }
            
            if (pointsLayer) {
                console.log('Points layer visibility:', this.map.getLayoutProperty('geothermal-points', 'visibility'));
                console.log('Points layer opacity:', this.map.getPaintProperty('geothermal-points', 'circle-opacity'));
            }
        }, 2000);
    }

    addTransmissionLinesLayer(layerId, sourceId, minZoom, maxZoom) {
        this.map.addLayer({
            id: layerId,
            type: 'line',
            source: sourceId,
            'source-layer': 'default', // pg_tileserv uses 'default' as layer name
            minzoom: minZoom,
            maxzoom: maxZoom,
            paint: {
                'line-color': [
                    'case',
                    ['==', ['get', 'kv'], null], '#999999',           // Unknown
                    ['<', ['get', 'kv'], 69], '#4CAF50',              // Low voltage - Green
                    ['<', ['get', 'kv'], 138], '#FF9800',             // Med-Low - Orange
                    ['<', ['get', 'kv'], 230], '#2196F3',             // Medium - Blue
                    ['<', ['get', 'kv'], 345], '#9C27B0',             // High - Purple
                    ['<', ['get', 'kv'], 500], '#F44336',             // Extra High - Red
                    '#000000'                                         // Ultra High - Black
                ],
                'line-width': [
                    'interpolate',
                    ['linear'],
                    ['zoom'],
                    0, [
                        'case',
                        ['==', ['get', 'kv'], null], 2.5,
                        ['<', ['get', 'kv'], 69], 2.5,
                        ['<', ['get', 'kv'], 138], 3,
                        ['<', ['get', 'kv'], 230], 3.5,
                        ['<', ['get', 'kv'], 345], 4,
                        ['<', ['get', 'kv'], 500], 4.5,
                        5
                    ],
                    6, [
                        'case',
                        ['==', ['get', 'kv'], null], 3,
                        ['<', ['get', 'kv'], 69], 3,
                        ['<', ['get', 'kv'], 138], 3.5,
                        ['<', ['get', 'kv'], 230], 4,
                        ['<', ['get', 'kv'], 345], 4.5,
                        ['<', ['get', 'kv'], 500], 5,
                        5.5
                    ],
                    14, [
                        'case',
                        ['==', ['get', 'kv'], null], 4,
                        ['<', ['get', 'kv'], 69], 4,
                        ['<', ['get', 'kv'], 138], 5,
                        ['<', ['get', 'kv'], 230], 6,
                        ['<', ['get', 'kv'], 345], 7,
                        ['<', ['get', 'kv'], 500], 8,
                        9
                    ]
                ],
                'line-opacity': 0.8
            }
        });
    }

    // Removed addGeothermalAggregatedLayer function as aggregated source doesn't exist

    addGeothermalPointsLayer() {
        console.log('🔥 Adding geothermal points layer...');
        try {
            this.map.addLayer({
            id: 'geothermal-points',
            type: 'circle',
            source: 'geothermal-points',
            'source-layer': 'default', // pg_tileserv uses 'default' as layer name
            minzoom: 3,  // Lower zoom to make data more visible
            maxzoom: 14,
            paint: {
                // Simplified large circles for maximum visibility  
                'circle-radius': 25,
                'circle-color': [
                    'case',
                    ['==', ['get', 'temperature_f'], null], '#999999',    // Gray for null
                    ['<', ['get', 'temperature_f'], 40], '#999999',       // Gray for cold (ignored)
                    ['<', ['get', 'temperature_f'], 60], '#4CAF50',       // Green - Moderate (40-60°F)
                    ['<', ['get', 'temperature_f'], 80], '#FFEB3B',       // Yellow - Warm (60-80°F)
                    ['<', ['get', 'temperature_f'], 100], '#FF9800',      // Orange - Hot (80-100°F)
                    ['<', ['get', 'temperature_f'], 130], '#F44336',      // Red - Very Hot (100-130°F)
                    ['<', ['get', 'temperature_f'], 160], '#E91E63',      // Hot Pink - Extreme (130-160°F)
                    ['<', ['get', 'temperature_f'], 200], '#9C27B0',      // Purple - Ultra (160-200°F)
                    '#000000'                                             // Black - Maximum (>200°F)
                ],
                'circle-opacity': 1.0,  // Maximum opacity for visibility
                'circle-stroke-color': '#FFFFFF',
                'circle-stroke-width': 3
            },
            filter: ['>=', ['get', 'temperature_f'], 40] // Only show moderate+ temperatures
        });
        console.log('✅ Geothermal layer added successfully');
        } catch (error) {
            console.error('❌ Error adding geothermal layer:', error);
        }
    }

    addGeothermalGridLayer() {
        console.log('🔲 Adding geothermal grid layer...');
        try {
            this.map.addLayer({
                id: 'geothermal-grid',
                type: 'fill',
                source: 'geothermal-grid',
                'source-layer': 'default',
                minzoom: 0,
                maxzoom: 8,
                paint: {
                    'fill-color': [
                        'case',
                        ['==', ['get', 'avg_temperature_f'], null], 'transparent',
                        ['<', ['get', 'avg_temperature_f'], 60], '#4CAF50',       // Green - Moderate (40-60°F)
                        ['<', ['get', 'avg_temperature_f'], 80], '#FFEB3B',       // Yellow - Warm (60-80°F)
                        ['<', ['get', 'avg_temperature_f'], 100], '#FF9800',      // Orange - Hot (80-100°F)
                        ['<', ['get', 'avg_temperature_f'], 130], '#F44336',      // Red - Very Hot (100-130°F)
                        ['<', ['get', 'avg_temperature_f'], 160], '#E91E63',      // Hot Pink - Extreme (130-160°F)
                        ['<', ['get', 'avg_temperature_f'], 200], '#9C27B0',      // Purple - Ultra (160-200°F)
                        '#000000'                                                 // Black - Maximum (>200°F)
                    ],
                    'fill-opacity': [
                        'case',
                        ['boolean', ['feature-state', 'hover'], false],
                        0.9,  // Higher opacity on hover
                        0.6   // Normal opacity
                    ],
                    'fill-outline-color': [
                        'case',
                        ['boolean', ['feature-state', 'hover'], false],
                        '#FFD700',  // Gold outline on hover
                        '#FFFFFF'   // White outline normally
                    ]
                }
            });
            
            // Add grid outline layer for better visibility
            this.map.addLayer({
                id: 'geothermal-grid-outline',
                type: 'line',
                source: 'geothermal-grid',
                'source-layer': 'default',
                minzoom: 0,
                maxzoom: 8,
                paint: {
                    'line-color': [
                        'case',
                        ['boolean', ['feature-state', 'hover'], false],
                        '#FFD700',  // Gold outline on hover
                        '#FFFFFF'   // White outline normally
                    ],
                    'line-width': [
                        'case',
                        ['boolean', ['feature-state', 'hover'], false],
                        2,    // Thicker line on hover
                        0.5   // Thin line normally
                    ],
                    'line-opacity': [
                        'case',
                        ['boolean', ['feature-state', 'hover'], false],
                        1.0,  // Full opacity on hover
                        0.3   // Low opacity normally
                    ]
                }
            });
            
            console.log('✅ Geothermal grid layer added successfully');
        
        // Add a simple debug layer to test if any layers work
        this.map.addLayer({
            id: 'debug-test-layer',
            type: 'fill',
            source: 'geothermal-grid',
            'source-layer': 'default',
            minzoom: 0,
            maxzoom: 22,
            paint: {
                'fill-color': '#FF0000',  // Bright red
                'fill-opacity': 0.8
            }
        });
        console.log('🔴 Added debug red layer for testing');
        
        } catch (error) {
            console.error('❌ Error adding grid layer:', error);
        }
    }

    setupMapInteractions() {
        // Click handlers for popups
        const transmissionLayers = [
            'transmission-lines-z0-6',
            'transmission-lines-z7-10', 
            'transmission-lines-z11-14'
        ];
        
        const geothermalLayers = [
            'geothermal-points'
        ];
        
        const gridLayers = [
            'geothermal-grid'
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

        // Grid box popups
        gridLayers.forEach(layerId => {
            this.map.on('click', layerId, (e) => {
                this.showGridPopup(e);
            });
            
            this.map.on('mouseenter', layerId, (e) => {
                this.map.getCanvas().style.cursor = 'pointer';
                
                // Highlight the hovered grid box
                if (e.features.length > 0) {
                    if (this.hoveredGridId) {
                        this.map.setFeatureState(
                            { source: 'geothermal-grid', sourceLayer: 'default', id: this.hoveredGridId },
                            { hover: false }
                        );
                    }
                    this.hoveredGridId = e.features[0].id;
                    this.map.setFeatureState(
                        { source: 'geothermal-grid', sourceLayer: 'default', id: this.hoveredGridId },
                        { hover: true }
                    );
                }
            });
            
            this.map.on('mouseleave', layerId, () => {
                this.map.getCanvas().style.cursor = '';
                
                // Remove highlight
                if (this.hoveredGridId) {
                    this.map.setFeatureState(
                        { source: 'geothermal-grid', sourceLayer: 'default', id: this.hoveredGridId },
                        { hover: false }
                    );
                    this.hoveredGridId = null;
                }
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

        // Geothermal controls
        document.getElementById('geothermal-toggle').addEventListener('change', (e) => {
            this.toggleGeothermalPoints(e.target.checked);
        });

        // Removed aggregated layer toggle as it doesn't exist

        document.getElementById('geothermal-opacity').addEventListener('input', (e) => {
            const opacity = e.target.value / 100;
            document.getElementById('geothermal-opacity-value').textContent = e.target.value + '%';
            this.updateGeothermalOpacity(opacity);
        });

        document.getElementById('temperature-filter').addEventListener('input', (e) => {
            const minTemp = parseInt(e.target.value);
            document.getElementById('temperature-filter-value').textContent = minTemp + '°F';
            this.updateTemperatureFilter(minTemp);
        });
        
        // Grid controls
        document.getElementById('grid-toggle').addEventListener('change', (e) => {
            this.toggleGrid(e.target.checked);
        });

        document.getElementById('grid-opacity').addEventListener('input', (e) => {
            const opacity = e.target.value / 100;
            document.getElementById('grid-opacity-value').textContent = e.target.value + '%';
            this.updateGridOpacity(opacity);
        });
        
        // Temperature lookup functionality
        document.getElementById('lookup-temp').addEventListener('click', () => {
            this.performTemperatureLookup();
        });
    }

    toggleTransmissionLines(visible) {
        const layers = ['transmission-lines-z0-6', 'transmission-lines-z7-10', 'transmission-lines-z11-14'];
        layers.forEach(layerId => {
            this.map.setLayoutProperty(layerId, 'visibility', visible ? 'visible' : 'none');
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
        const layers = ['transmission-lines-z0-6', 'transmission-lines-z7-10', 'transmission-lines-z11-14'];
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

    toggleGrid(visible) {
        const layers = ['geothermal-grid', 'geothermal-grid-outline'];
        layers.forEach(layerId => {
            this.map.setLayoutProperty(layerId, 'visibility', visible ? 'visible' : 'none');
        });
    }

    updateGridOpacity(opacity) {
        this.map.setPaintProperty('geothermal-grid', 'fill-opacity', opacity);
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