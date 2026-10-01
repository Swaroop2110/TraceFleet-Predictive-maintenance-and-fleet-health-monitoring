$ts = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
$event = '{"event_id": "test-alert-' + (Get-Random) + '", "vehicle_id": "veh_demo123", "vin": "1HGC1234567890123", "ts": "' + $ts + '", "lat": 21.17, "lon": 72.83, "speed_kmh": 120.0, "acceleration_mps2": -6.0, "brake_force_pct": 90.0, "heading_deg": 270.0, "dtc": ["P0101"], "seq": 1, "schema_version": "1.0"}'

# Use a here-string or echo piped directly into the container
$event | docker compose exec -T kafka sh -c "kafka-console-producer --bootstrap-server localhost:9092 --topic telemetry.raw"

Write-Host "Alert injected successfully! Check your web dashboard." -ForegroundColor Green
