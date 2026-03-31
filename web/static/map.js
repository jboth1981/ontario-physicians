/* Initialize Leaflet map — always visible for location picking + results. */

(function () {
    var map = L.map("map");

    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
        attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
        maxZoom: 18,
    }).addTo(map);

    var userIcon = L.divIcon({
        className: "user-marker",
        html: '<div style="background:#2563eb;width:14px;height:14px;border-radius:50%;border:2px solid white;box-shadow:0 1px 3px rgba(0,0,0,0.3);"></div>',
        iconSize: [14, 14],
        iconAnchor: [7, 7],
    });

    var userMarker = null;
    var hasResults = RESULTS && RESULTS.length > 0;

    if (hasResults) {
        document.getElementById("map").classList.add("map-with-results");
    }

    // Place user marker if we have coordinates
    if (USER_LAT != null && USER_LNG != null) {
        userMarker = L.marker([USER_LAT, USER_LNG], { icon: userIcon }).addTo(map);
        userMarker.bindPopup("<strong>Your location</strong>");
    }

    // Click map to pick location
    map.on("click", function (e) {
        var lat = e.latlng.lat;
        var lng = e.latlng.lng;

        if (userMarker) {
            userMarker.setLatLng(e.latlng);
        } else {
            userMarker = L.marker(e.latlng, { icon: userIcon }).addTo(map);
            userMarker.bindPopup("<strong>Your location</strong>");
        }
        userMarker.openPopup();

        // Fill hidden form fields
        document.getElementById("map-lat").value = lat.toFixed(6);
        document.getElementById("map-lng").value = lng.toFixed(6);

        // Update postal code field to indicate map selection
        var postalInput = document.getElementById("postal-code");
        postalInput.value = "Map location (" + lat.toFixed(3) + ", " + lng.toFixed(3) + ")";
    });

    if (hasResults) {
        // Show results on map
        var bounds = L.latLngBounds();
        if (USER_LAT != null && USER_LNG != null) {
            bounds.extend([USER_LAT, USER_LNG]);
        }

        var clusters = L.markerClusterGroup({
            maxClusterRadius: 30,
            spiderfyOnMaxZoom: false,
            showCoverageOnHover: false,
            zoomToBoundsOnClick: false,
            singleMarkerMode: true,
        });

        var CPSO_URL = "https://register.cpso.on.ca/physician-info/?cpsonum=";

        function doctorCard(r) {
            var html = '<div style="min-width:220px;font-size:0.9em;line-height:1.5;">';
            html += '<strong>' + (r.full_name || "Unknown") + '</strong>';
            if (r.specialties) html += '<br><span style="color:#555;">' + r.specialties + '</span>';
            var addrParts = [r.street, r.city, r.province, r.postal_code].filter(Boolean);
            if (addrParts.length) html += '<br>' + addrParts.join(", ");
            if (r.phone) html += '<br>Phone: ' + r.phone;
            if (r.fax) html += '<br>Fax: ' + r.fax;
            html += '<br>CPSO#: ' + r.cpso_number;
            if (r.distance_km != null) html += '<br><em style="color:#2a5f8f;">' + r.distance_km + ' km</em>';
            html += '<br><a href="' + CPSO_URL + r.cpso_number + '" target="_blank" rel="noopener" style="color:#2a5f8f;">View CPSO Profile</a>';
            html += '</div>';
            return html;
        }

        RESULTS.forEach(function (r, i) {
            if (r.lat == null || r.lng == null) return;

            var marker = L.marker([r.lat, r.lng]);

            marker._physicianData = r;
            marker.bindPopup(doctorCard(r), { maxWidth: 350 });
            clusters.addLayer(marker);
            bounds.extend([r.lat, r.lng]);
        });

        clusters.on("clusterclick", function (e) {
            var childMarkers = e.layer.getAllChildMarkers();
            var items = childMarkers.map(function (m) { return m._physicianData; });
            items.sort(function (a, b) { return (a.full_name || "").localeCompare(b.full_name || ""); });

            var html;
            if (items.length === 1) {
                html = doctorCard(items[0]);
            } else {
                var distance = items[0] && items[0].distance_km != null ? items[0].distance_km + ' km' : null;
                html = '<div style="max-height:400px;overflow-y:auto;min-width:280px;">';
                html += '<strong>' + items.length + ' physicians at this location</strong>';
                if (distance) html += ' <span style="color:#2a5f8f;">(' + distance + ')</span>';
                html += '<hr style="margin:0.4rem 0;">';
                items.forEach(function (r) {
                    html += '<div style="padding:0.4rem 0;border-bottom:1px solid #eee;">';
                    html += doctorCard(r);
                    html += '</div>';
                });
                html += '</div>';
            }

            L.popup({ maxWidth: 350 })
                .setLatLng(e.layer.getLatLng())
                .setContent(html)
                .openOn(map);
        });

        map.addLayer(clusters);
        map.fitBounds(bounds, { padding: [30, 30] });
    } else {
        // No results — show Ontario centered on Toronto, zoomed out
        map.setView([44.0, -79.5], 7);
    }

    // Force Leaflet to recalculate after layout settles (fixes mobile rendering)
    setTimeout(function () { map.invalidateSize(); }, 100);

    // Clear map lat/lng if user types a new location manually
    var postalInput = document.getElementById("postal-code");
    postalInput.addEventListener("input", function () {
        document.getElementById("map-lat").value = "";
        document.getElementById("map-lng").value = "";
    });
})();
