param(
    [string] $owner,
    [string] $repo,
    [string] $pullRequestId,
    [string] $label
)

$ErrorActionPreference = "Stop"
[Net.ServicePointManager]::Expect100Continue = $true;
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12;

$IGNORED_LABEL = "future";

function Invoke-JsonHttpPost($url, $text) {
    Write-Host "[INFO] POSTing to $url"

    $webRequest = [System.Net.WebRequest]::Create($url)

Write-Host "[INFO] Adding label `"$label`" to pull request #$pullRequestId"
Invoke-JsonHttpPost "https://api.github.com/repos/$owner/$repo/issues/$pullRequestId/labels" "[`"$label`"]"
