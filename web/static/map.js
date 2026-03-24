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

    // Numbered red markers for each result
    RESULTS.forEach(function (r, i) {
        if (r.lat == null || r.lng == null) return;

        var marker = L.marker([r.lat, r.lng], {
            icon: L.divIcon({
                className: "result-marker",
                html: '<div style="background:#dc2626;color:white;width:24px;height:24px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:12px;font-weight:bold;border:2px solid white;box-shadow:0 1px 3px rgba(0,0,0,0.3);">' + (i + 1) + "</div>",
                iconSize: [24, 24],
                iconAnchor: [12, 12],
            }),
        }).addTo(map);

        var popupLines = [];
        popupLines.push("<strong>" + (r.full_name || "Unknown") + "</strong>");
        if (r.specialties) popupLines.push(r.specialties);
        var addrParts = [r.street, r.city, r.province, r.postal_code].filter(Boolean);
        if (addrParts.length) popupLines.push(addrParts.join(", "));
        if (r.phone) popupLines.push("Phone: " + r.phone);
        if (r.distance_km != null) popupLines.push("<em>" + r.distance_km + " km away</em>");

        marker.bindPopup(popupLines.join("<br>"));
        bounds.extend([r.lat, r.lng]);
    });

    map.fitBounds(bounds, { padding: [30, 30] });
})();
