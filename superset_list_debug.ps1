$login = Invoke-RestMethod -Method Post -Uri 'http://localhost:8088/api/v1/security/login' -ContentType 'application/json' -Body (@{username='admin'; password='admin123'; provider='db'} | ConvertTo-Json)
$token = $login.access_token
$hdr = @{Authorization = "Bearer $token"}
Write-Output "TOKEN:" + $token.Substring(0,8)
$datasets = Invoke-RestMethod -Method Get -Uri 'http://localhost:8088/api/v1/dataset/?order_column=id' -Headers $hdr
Write-Output "DATASETS_COUNT:" + $datasets.result.Count
$charts = Invoke-RestMethod -Method Get -Uri 'http://localhost:8088/api/v1/chart/?order_column=id' -Headers $hdr
Write-Output "CHARTS_COUNT:" + $charts.result.Count
$dashboards = Invoke-RestMethod -Method Get -Uri 'http://localhost:8088/api/v1/dashboard/?order_column=id' -Headers $hdr
Write-Output "DASH_COUNT:" + $dashboards.result.Count
