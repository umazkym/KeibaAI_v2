# deep_cleanup_phase2.ps1

Write-Host "Starting Deep Cleanup Phase 2..."

# 1. Move Scripts from keibaai/src to scripts/
$moves = @(
    @{ Src = "keibaai/src/debug_run_parsing_pipeline_local_1000.py"; Dest = "scripts/debug/" },
    @{ Src = "keibaai/src/run_parsing_pipeline_local.py"; Dest = "scripts/pipelines/" },
    @{ Src = "keibaai/src/run_scraping_pipeline_local.py"; Dest = "scripts/pipelines/" },
    @{ Src = "keibaai/src/run_scraping_pipeline_with_args.py"; Dest = "scripts/pipelines/" }
)

foreach ($move in $moves) {
    if (Test-Path $move.Src) {
        if (-not (Test-Path $move.Dest)) {
            New-Item -ItemType Directory -Path $move.Dest -Force | Out-Null
        }
        Move-Item -Path $move.Src -Destination $move.Dest -Force
        Write-Host "Moved: $($move.Src) -> $($move.Dest)"
    }
    else {
        Write-Warning "Source not found: $($move.Src)"
    }
}

# 2. Move Reports from docs/ to docs/reports/
$report_moves = @(
    @{ Src = "docs/FIX_REPORT_20251122.md"; Dest = "docs/reports/" },
    @{ Src = "docs/GEMINI_VERIFICATION_REPORT_20251122.md"; Dest = "docs/reports/" },
    @{ Src = "docs/SIMULATION_ISSUE_ANALYSIS_20251122.md"; Dest = "docs/reports/" },
    @{ Src = "docs/data_structure.txt"; Dest = "docs/archive/reports/" }
)

foreach ($move in $report_moves) {
    if (Test-Path $move.Src) {
        if (-not (Test-Path $move.Dest)) {
            New-Item -ItemType Directory -Path $move.Dest -Force | Out-Null
        }
        Move-Item -Path $move.Src -Destination $move.Dest -Force
        Write-Host "Moved: $($move.Src) -> $($move.Dest)"
    }
    else {
        Write-Warning "Source not found: $($move.Src)"
    }
}

Write-Host "Deep Cleanup Phase 2 completed."
