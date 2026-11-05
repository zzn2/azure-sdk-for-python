param([switch]$SkipInstall)

$ErrorActionPreference = 'Stop'
$packageRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$source = Join-Path $PSScriptRoot 'specification\ai-foundry\data-plane\Foundry'
$arguments = @(
    'update',
    '--output-dir', $PSScriptRoot,
    '--local-spec-repo', $source,
    '--no-prompt'
)
if ($SkipInstall) {
    $arguments += '--skip-install'
}

& tsp-client @arguments
if ($LASTEXITCODE -ne 0) {
    throw 'Internal Component registration generation failed.'
}

# Keep only the generated operations; reuse the SDK's existing client and helpers.
foreach ($relativePath in @(
    'operations\_pipeline_component_registration_operations.py',
    'aio\operations\_pipeline_component_registration_operations.py'
)) {
    Copy-Item -LiteralPath (Join-Path $PSScriptRoot "output\azure\ai\projects\$relativePath") `
        -Destination (Join-Path $packageRoot "azure\ai\projects\$relativePath")
}
