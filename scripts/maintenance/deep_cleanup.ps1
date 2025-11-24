# deep_cleanup.ps1

Write-Host "Starting Deep Cleanup..."

# 1. Create Archive Directories
$dirs = @(
    "archive/scripts/debug",
    "docs/archive/reports"
)

foreach ($d in $dirs) {
    if (-not (Test-Path $d)) {
        New-Item -ItemType Directory -Path $d -Force | Out-Null
        Write-Host "Created directory: $d"
    }
}

# 2. Archive Debug Scripts (from scripts/debug and scripts/temp)
# Patterns to move to archive/scripts/debug
$archive_patterns = @(
    "debug_0*.py", 
    "debug_output_*.py", 
    "debug_parse_*.py", 
    "investigate_*.py", 
    "reproduce_*.py", 
    "fix_*.py", 
    "test_*.py",
    "simple_*.py",
    "verify_*.py",
    "inspect_*.py",
    "check_*.py",
    "analyze_*.py",
    "calculate_*.py",
    "compare_*.py",
    "evaluate_*.py",
    "diagnose_*.py",
    "direct_*.py",
    "find_*.py",
    "manual_*.py",
    "quick_*.py",
    "resave_*.py",
    "tmp_*.py",
    "transform_*.py",
    "update_*.py",
    "rebuild_*.py",
    "reparse_*.py",
    "apply_*.py",
    "backup_*.py",
    "cleanup_*.py",
    "complete_*.py",
    "deep_*.py",
    "definitive_*.py",
    "implement_*.py"
)

# Source directories to clean
$source_dirs = @("scripts/debug", "scripts/temp", "scripts/analysis")

foreach ($src in $source_dirs) {
    if (Test-Path $src) {
        foreach ($pattern in $archive_patterns) {
            Get-ChildItem -Path $src -Filter $pattern -ErrorAction SilentlyContinue | ForEach-Object {
                $dest = "archive/scripts/debug/"
                Move-Item -Path $_.FullName -Destination $dest -Force
                Write-Host "Archived: $($_.Name) from $src"
            }
        }
    }
}

# 3. Archive Reports
# Move all *_REPORT.md except CURRENT_STATUS_REPORT.md
if (Test-Path "docs/reports") {
    Get-ChildItem -Path "docs/reports" -Filter "*_REPORT.md" | Where-Object { $_.Name -ne "CURRENT_STATUS_REPORT.md" } | ForEach-Object {
        Move-Item -Path $_.FullName -Destination "docs/archive/reports/" -Force
        Write-Host "Archived Report: $($_.Name)"
    }
    
    $misc_reports = @("FILES_TO_REMOVE.md", "IDEA.md", "REVIEW.md", "TROUBLESHOOTING.md", "PROGRESS.md")
    foreach ($f in $misc_reports) {
        if (Test-Path "docs/reports/$f") {
            Move-Item -Path "docs/reports/$f" -Destination "docs/archive/reports/" -Force
            Write-Host "Archived Doc: $f"
        }
    }
}

# 4. Delete Temporary Output
$items_to_delete = @(
    "output_final",
    "test",
    "debug_scraped_data.csv",
    "debug_race_list.html",
    "distance_analysis.txt"
)

foreach ($item in $items_to_delete) {
    if (Test-Path $item) {
        Remove-Item -Path $item -Recurse -Force -ErrorAction SilentlyContinue
        Write-Host "Deleted: $item"
    }
}

Write-Host "Deep cleanup completed."
