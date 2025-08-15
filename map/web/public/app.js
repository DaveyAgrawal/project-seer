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
            geothermalAggregated: true
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
        
        console.log('Available datasets:', this.datasets.length);
        console.log('Transmission dataset:', transmissionDataset ? 'Found' : 'Not found');
        console.log('Geothermal dataset:', geothermalDataset ? 'Found' : 'Not found');
        
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
            // Aggregated view for low zooms
            this.map.addSource('geothermal-aggregated', {
                type: 'vector',
                tiles: [`${tileserverUrl}/public.${geothermalDataset.table_name}_us_z0_9/{z}/{x}/{y}.mvt`],
                minzoom: 3,
                maxzoom: 9
            });
            
            // Raw points for high zooms
            this.map.addSource('geothermal-points', {
                type: 'vector',
                tiles: [`${tileserverUrl}/public.${geothermalDataset.table_name}_us/{z}/{x}/{y}.mvt`],
                minzoom: 10,
                maxzoom: 14
            });
            console.log('Added geothermal sources');
        }
        
        // Update UI to show dataset status
        this.updateDatasetStatus(transmissionDataset, geothermalDataset);
    }

    addDataLayers() {
        // Add transmission lines layers (zoom-banded)
        this.addTransmissionLinesLayer('transmission-lines-z0-6', 'transmission-lines', 0, 6);
        this.addTransmissionLinesLayer('transmission-lines-z7-10', 'transmission-lines', 7, 10);
        this.addTransmissionLinesLayer('transmission-lines-z11-14', 'transmission-lines', 11, 14);
        
        // Add geothermal layers
        this.addGeothermalAggregatedLayer();
        this.addGeothermalPointsLayer();
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
                    3, [
                        'case',
                        ['==', ['get', 'kv'], null], 1,
                        ['<', ['get', 'kv'], 69], 1,
                        ['<', ['get', 'kv'], 138], 1.5,
                        ['<', ['get', 'kv'], 230], 2,
                        ['<', ['get', 'kv'], 345], 2.5,
                        ['<', ['get', 'kv'], 500], 3,
                        3.5
                    ],
                    14, [
                        'case',
                        ['==', ['get', 'kv'], null], 2,
                        ['<', ['get', 'kv'], 69], 2,
                        ['<', ['get', 'kv'], 138], 3,
                        ['<', ['get', 'kv'], 230], 4,
                        ['<', ['get', 'kv'], 345], 5,
                        ['<', ['get', 'kv'], 500], 6,
                        7
                    ]
                ],
                'line-opacity': 0.8
            }
        });
    }

    addGeothermalAggregatedLayer() {
        this.map.addLayer({
            id: 'geothermal-aggregated',
            type: 'circle',
            source: 'geothermal-aggregated',
            'source-layer': 'default',
            minzoom: 3,
            maxzoom: 9,
            paint: {
                'circle-radius': [
                    'interpolate',
                    ['linear'],
                    ['get', 'point_count'],
                    1, 4,
                    10, 8,
                    50, 12,
                    100, 16,
                    500, 20
                ],
                'circle-color': [
                    'case',
                    ['==', ['get', 'avg_temperature_f'], null], '#999999',
                    ['<', ['get', 'avg_temperature_f'], 150], '#2196F3',    // Cool - Blue
                    ['<', ['get', 'avg_temperature_f'], 200], '#4CAF50',    // Warm - Green  
                    ['<', ['get', 'avg_temperature_f'], 250], '#FF9800',    // Hot - Orange
                    ['<', ['get', 'avg_temperature_f'], 300], '#F44336',    // Very Hot - Red
                    '#9C27B0'                                               // Extreme - Purple
                ],
                'circle-opacity': 0.7,
                'circle-stroke-color': '#fff',
                'circle-stroke-width': 1
            }
        });
    }

    addGeothermalPointsLayer() {
        this.map.addLayer({
            id: 'geothermal-points',
            type: 'circle',
            source: 'geothermal-points',
            'source-layer': 'default',
            minzoom: 10,
            maxzoom: 14,
            paint: {
                'circle-radius': [
                    'interpolate',
                    ['linear'],
                    ['zoom'],
                    10, 3,
                    14, 6
                ],
                'circle-color': [
                    'case',
                    ['==', ['get', 'temperature_f'], null], '#999999',
                    ['<', ['get', 'temperature_f'], 150], '#2196F3',       // Cool - Blue
                    ['<', ['get', 'temperature_f'], 200], '#4CAF50',       // Warm - Green
                    ['<', ['get', 'temperature_f'], 250], '#FF9800',       // Hot - Orange  
                    ['<', ['get', 'temperature_f'], 300], '#F44336',       // Very Hot - Red
                    '#9C27B0'                                              // Extreme - Purple
                ],
                'circle-opacity': 0.7,
                'circle-stroke-color': '#fff',
                'circle-stroke-width': 1
            },
            filter: ['>=', ['get', 'temperature_f'], 0] // Will be updated by temperature filter
        });
    }

    setupMapInteractions() {
        // Click handlers for popups
        const transmissionLayers = [
            'transmission-lines-z0-6',
            'transmission-lines-z7-10', 
            'transmission-lines-z11-14'
        ];
        
        const geothermalLayers = [
            'geothermal-aggregated',
            'geothermal-points'
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

        document.getElementById('geothermal-aggregated').addEventListener('change', (e) => {
            this.toggleGeothermalAggregated(e.target.checked);
        });

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
        const layers = ['geothermal-aggregated', 'geothermal-points'];
        layers.forEach(layerId => {
            this.map.setLayoutProperty(layerId, 'visibility', visible ? 'visible' : 'none');
        });
        
        // Update legend visibility
        document.getElementById('geothermal-legend').style.display = visible ? 'block' : 'none';
    }

    toggleGeothermalAggregated(useAggregated) {
        if (useAggregated) {
            this.map.setLayoutProperty('geothermal-aggregated', 'visibility', 'visible');
            this.map.setLayoutProperty('geothermal-points', 'visibility', 'none');
        } else {
            this.map.setLayoutProperty('geothermal-aggregated', 'visibility', 'none');
            this.map.setLayoutProperty('geothermal-points', 'visibility', 'visible');
        }
    }

    updateTransmissionOpacity(opacity) {
        const layers = ['transmission-lines-z0-6', 'transmission-lines-z7-10', 'transmission-lines-z11-14'];
        layers.forEach(layerId => {
            this.map.setPaintProperty(layerId, 'line-opacity', opacity);
        });
    }

    updateGeothermalOpacity(opacity) {
        const layers = ['geothermal-aggregated', 'geothermal-points'];
        layers.forEach(layerId => {
            this.map.setPaintProperty(layerId, 'circle-opacity', opacity);
        });
    }

    updateTemperatureFilter(minTemp) {
        // Update filter for individual points
        this.map.setFilter('geothermal-points', ['>=', ['get', 'temperature_f'], minTemp]);
        
        // Update filter for aggregated points
        this.map.setFilter('geothermal-aggregated', ['>=', ['get', 'avg_temperature_f'], minTemp]);
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