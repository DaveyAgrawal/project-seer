"""
Sample HTML fixtures for testing parsers.
"""

USA_PAGE_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>Data Centers in USA | DataCenterMap</title>
</head>
<body>
    <div class="main-content">
        <h1>United States Data Centers</h1>
        <div class="state-links">
            <a href="/usa/alabama/">Alabama</a>
            <a href="/usa/alaska/">Alaska</a>
            <a href="/usa/arizona/">Arizona</a>
            <a href="/usa/california/">California</a>
            <a href="/usa/delaware/">Delaware</a>
            <a href="/usa/florida/">Florida</a>
            <a href="/usa/texas/">Texas</a>
        </div>
    </div>
</body>
</html>
"""

DELAWARE_STATE_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>Delaware Data Centers | DataCenterMap</title>
</head>
<body>
    <div class="main-content">
        <h1>Delaware Data Centers</h1>
        <div class="city-links">
            <a href="/usa/delaware/dover/">Dover</a>
            <a href="/usa/delaware/newark/">Newark</a>
            <a href="/usa/delaware/wilmington/">Wilmington</a>
        </div>
    </div>
</body>
</html>
"""

WILMINGTON_CITY_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>Wilmington Delaware Data Centers | DataCenterMap</title>
</head>
<body>
    <div class="main-content">
        <h1>Data Centers in Wilmington, Delaware</h1>
        <div class="facility-links">
            <a href="/datacenters/delaware-data-center-1/">Delaware Data Center 1</a>
            <a href="/facilities/wilmington-tech-hub/">Wilmington Tech Hub</a>
            <a href="/datacenter/east-coast-facility/">East Coast Facility</a>
        </div>
    </div>
</body>
</html>
"""

FACILITY_DETAIL_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>Delaware Data Center 1 | DataCenterMap</title>
    <script>
        var facility_lat = 39.7391;
        var facility_lng = -75.5398;
    </script>
</head>
<body>
    <div class="facility-detail">
        <h1>Delaware Data Center 1</h1>
        <div class="facility-info">
            <div class="address">
                123 Technology Drive, Wilmington, DE 19801
            </div>
            <div class="description">
                State-of-the-art data center facility serving the Delaware region.
            </div>
        </div>
        <div class="map-container">
            <iframe src="https://maps.google.com/maps?q=39.7391,-75.5398&z=15"></iframe>
        </div>
    </div>
</body>
</html>
"""

FACILITY_WITH_DATA_ATTRS_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>Tech Hub Facility | DataCenterMap</title>
</head>
<body>
    <div class="facility-detail">
        <h1 class="facility-name">Wilmington Tech Hub</h1>
        <div class="facility-address">456 Innovation Way, Newark, DE 19702</div>
        <div class="map" data-lat="39.6837" data-lng="-75.7497">
            <div class="marker"></div>
        </div>
    </div>
</body>
</html>
"""

FACILITY_MINIMAL_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>Basic Facility</title>
</head>
<body>
    <h1>East Coast Data Center</h1>
    <p>Located in Delaware</p>
</body>
</html>
"""