# Build DazzleDuck extension natively on Windows using MSVC
# Run from PowerShell: .\scripts\build-windows-native.ps1

param(
    [string]$DuckDBVersion = "v1.4.3",
    [switch]$Clean
)

$ErrorActionPreference = "Stop"

# Add Git to PATH if not already there
$GitPaths = @(
    "C:\Program Files\Git\bin",
    "C:\Program Files\Git\cmd",
    "$env:LOCALAPPDATA\Programs\Git\bin",
    "$env:LOCALAPPDATA\Programs\Git\cmd"
)
foreach ($GitPath in $GitPaths) {
    if (Test-Path $GitPath) {
        $env:PATH = "$GitPath;$env:PATH"
        break
    }
}

# Paths - use native Windows path to avoid UNC issues
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$SourceRootDir = Split-Path -Parent $ScriptDir

# Build in a native Windows directory
$BuildRoot = "C:\duckdb-build"
$BuildDir = Join-Path $BuildRoot "dazzle_duck"
$DuckDBDir = Join-Path $BuildDir "duckdb"
$ExtSourceDir = Join-Path $BuildDir "ext_source"

# VS Build Tools paths
$VSPath = "C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools"
$VCVarsAll = Join-Path $VSPath "VC\Auxiliary\Build\vcvarsall.bat"
$CMakePath = Join-Path $VSPath "Common7\IDE\CommonExtensions\Microsoft\CMake\CMake\bin"
$NinjaPath = Join-Path $VSPath "Common7\IDE\CommonExtensions\Microsoft\CMake\Ninja"

Write-Host "=== DazzleDuck Windows Build ===" -ForegroundColor Cyan
Write-Host "DuckDB Version: $DuckDBVersion"
Write-Host "Build Dir: $BuildDir"
Write-Host "Source Dir: $SourceRootDir"

# Clean if requested
if ($Clean -and (Test-Path $BuildDir)) {
    Write-Host "Cleaning build directory..." -ForegroundColor Yellow
    Remove-Item -Recurse -Force $BuildDir
}

# Create directories
New-Item -ItemType Directory -Force -Path $BuildDir | Out-Null
New-Item -ItemType Directory -Force -Path $ExtSourceDir | Out-Null

# Clone DuckDB if needed
if (-not (Test-Path $DuckDBDir)) {
    Write-Host "Cloning DuckDB $DuckDBVersion..." -ForegroundColor Yellow
    Push-Location $BuildDir
    git clone --depth 1 --branch $DuckDBVersion https://github.com/duckdb/duckdb.git
    Pop-Location
} else {
    Write-Host "Using existing DuckDB at $DuckDBDir" -ForegroundColor Green
}

# Copy extension sources to build directory
Write-Host "Copying extension sources..." -ForegroundColor Yellow
$FilesToCopy = @(
    "CMakeLists.txt",
    "extension_config.cmake",
    "vcpkg.json",
    "version.txt"
)
foreach ($File in $FilesToCopy) {
    $SrcFile = Join-Path $SourceRootDir $File
    if (Test-Path $SrcFile) {
        Copy-Item $SrcFile $ExtSourceDir -Force
    }
}
# Copy src directory
$SrcDir = Join-Path $SourceRootDir "src"
if (Test-Path $SrcDir) {
    Copy-Item -Path $SrcDir -Destination $ExtSourceDir -Recurse -Force
}

# Read version from version.txt
$VersionFile = Join-Path $SourceRootDir "version.txt"
$ExtVersion = (Get-Content $VersionFile -Raw).Trim()
Write-Host "Extension Version: $ExtVersion" -ForegroundColor Cyan

# Create extension_config.cmake that points to our extension
$ExtConfigPath = Join-Path $ExtSourceDir "extension_config.cmake"
# Use forward slashes for CMake compatibility
$ExtSourceDirCMake = $ExtSourceDir -replace '\\', '/'
$ExtConfigContent = @"
# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(dazzle_duck
                      SOURCE_DIR
                      $ExtSourceDirCMake
                      LOAD_TESTS
                      EXTENSION_VERSION "$ExtVersion")
"@
$ExtConfigContent | Out-File -FilePath $ExtConfigPath -Encoding UTF8

# Create a batch file to run the build with VS environment
# Use forward slashes for CMake paths
$ExtConfigPathCMake = $ExtConfigPath -replace '\\', '/'
$DuckDBDirCMake = $DuckDBDir -replace '\\', '/'

$BatchScript = @"
@echo off
call "$VCVarsAll" x64

set PATH=$CMakePath;$NinjaPath;%PATH%

cd /d "$BuildDir"

echo Configuring with CMake...
cmake -G Ninja ^
    -DCMAKE_BUILD_TYPE=Release ^
    -DDUCKDB_EXTENSION_CONFIGS="$ExtConfigPathCMake" ^
    -S "$DuckDBDirCMake" ^
    -B release

if errorlevel 1 exit /b 1

echo Building extension...
ninja -C release dazzle_duck_loadable_extension

if errorlevel 1 exit /b 1

echo Build complete!
"@

$BatchFile = Join-Path $BuildDir "run_build.bat"
$BatchScript | Out-File -FilePath $BatchFile -Encoding ASCII

Write-Host "Running build..." -ForegroundColor Yellow
Push-Location $BuildDir
cmd /c $BatchFile
$BuildExitCode = $LASTEXITCODE
Pop-Location

if ($BuildExitCode -ne 0) {
    Write-Host "Build failed!" -ForegroundColor Red
    exit 1
}

# Copy extension to output (back to WSL path)
$ExtensionPath = Join-Path $BuildDir "release\extension\dazzle_duck\dazzle_duck.duckdb_extension"
if (Test-Path $ExtensionPath) {
    # Create output dir in source
    $OutputDir = Join-Path $SourceRootDir "dist\$DuckDBVersion"
    New-Item -ItemType Directory -Force -Path $OutputDir | Out-Null

    $OutputFile = Join-Path $OutputDir "dazzle_duck.windows_amd64.duckdb_extension"
    Copy-Item $ExtensionPath $OutputFile -Force
    Write-Host "Extension copied to: $OutputFile" -ForegroundColor Green
    Get-Item $OutputFile | Select-Object Name, Length
} else {
    Write-Host "Extension file not found at $ExtensionPath" -ForegroundColor Red
    exit 1
}

Write-Host "=== Build Complete ===" -ForegroundColor Cyan
