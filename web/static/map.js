/* Initialize Leaflet map with physician search results. */

(function () {
    if (!RESULTS || RESULTS.length === 0) return;

    var map = L.map("map");

    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
        attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
        maxZoom: 18,
    }).addTo(map);

    var bounds = L.latLngBounds();

    // Blue marker for user's postal code
    var userMarker = L.marker([USER_LAT, USER_LNG], {
        icon: L.divIcon({
            className: "user-marker",
            html: '<div style="background:#2563eb;width:14px;height:14px;border-radius:50%;border:2px solid white;box-shadow:0 1px 3px rgba(0,0,0,0.3);"></div>',
            iconSize: [14, 14],
            iconAnchor: [7, 7],
        }),
    }).addTo(map);
    userMarker.bindPopup("<strong>Your location</strong>");
    bounds.extend([USER_LAT, USER_LNG]);

    // Numbered red markers for each result, grouped in a cluster layer
    var clusters = L.markerClusterGroup({
        maxClusterRadius: 30,
        spiderfyOnMaxZoom: false,
        showCoverageOnHover: false,
        zoomToBoundsOnClick: false,
    });

    function buildPopupHtml(r) {
        var lines = [];
        lines.push("<strong>" + (r.full_name || "Unknown") + "</strong>");
        if (r.specialties) lines.push(r.specialties);
        var addrParts = [r.street, r.city, r.province, r.postal_code].filter(Boolean);
        if (addrParts.length) lines.push(addrParts.join(", "));
        if (r.phone) lines.push("Phone: " + r.phone);
        if (r.distance_km != null) lines.push("<em>" + r.distance_km + " km away</em>");
        return lines.join("<br>");
    }

    RESULTS.forEach(function (r, i) {
        if (r.lat == null || r.lng == null) return;

        var marker = L.marker([r.lat, r.lng], {
            icon: L.divIcon({
                className: "result-marker",
                html: '<div style="background:#dc2626;color:white;width:24px;height:24px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:12px;font-weight:bold;border:2px solid white;box-shadow:0 1px 3px rgba(0,0,0,0.3);">' + (i + 1) + "</div>",
                iconSize: [24, 24],
                iconAnchor: [12, 12],
            }),
        });

        marker._physicianData = r;
        marker.bindPopup(buildPopupHtml(r));
        clusters.addLayer(marker);
        bounds.extend([r.lat, r.lng]);
    });

    // Click a cluster → show scrollable list of all doctors at that location
    clusters.on("clusterclick", function (e) {
        var childMarkers = e.layer.getAllChildMarkers();
        var items = childMarkers.map(function (m) { return m._physicianData; });
        items.sort(function (a, b) { return (a.full_name || "").localeCompare(b.full_name || ""); });

        var distance = items[0] && items[0].distance_km != null ? items[0].distance_km + ' km' : null;
        var html = '<div style="max-height:300px;overflow-y:auto;min-width:250px;">';
        html += '<strong>' + items.length + ' physicians at this location</strong>';
        if (distance) html += ' <span style="color:#2563eb;">(' + distance + ')</span>';
        html += '<hr style="margin:0.4rem 0;">';
        items.forEach(function (r) {
            html += '<div style="padding:0.3rem 0;border-bottom:1px solid #eee;">';
            html += '<strong>' + (r.full_name || "Unknown") + '</strong>';
            if (r.specialties) html += '<br><span style="font-size:0.85em;color:#555;">' + r.specialties + '</span>';
            html += '</div>';
        });
        html += '</div>';

        L.popup({ maxWidth: 350 })
            .setLatLng(e.layer.getLatLng())
            .setContent(html)
            .openOn(map);
    });

    map.addLayer(clusters);

    map.fitBounds(bounds, { padding: [30, 30] });
})();
