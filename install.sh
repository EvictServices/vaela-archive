#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

GO_MIN_MAJOR=1
GO_MIN_MINOR=22

print_step() {
  printf "\n==> %s\n" "$1"
}

have_cmd() {
  command -v "$1" >/dev/null 2>&1
}

version_ge() {
  local current_major="$1"
  local current_minor="$2"
  local req_major="$3"
  local req_minor="$4"

  if (( current_major > req_major )); then
    return 0
  fi

  if (( current_major < req_major )); then
    return 1
  fi

  (( current_minor >= req_minor ))
}

print_step "Validating Go installation"
if ! have_cmd go; then
  echo "Error: Go is not installed or not in PATH."
  echo "Install Go 1.22+ from https://go.dev/dl/ and rerun this script."
  exit 1
fi

go_version_raw="$(go version)"
go_version_token="$(printf "%s" "$go_version_raw" | awk '{print $3}')"
go_version_trimmed="${go_version_token#go}"
go_major="${go_version_trimmed%%.*}"
go_minor_patch="${go_version_trimmed#*.}"
go_minor="${go_minor_patch%%[^0-9]*}"

if [[ -z "$go_major" || -z "$go_minor" ]]; then
  echo "Error: Could not parse Go version from: $go_version_raw"
  exit 1
fi

if ! version_ge "$go_major" "$go_minor" "$GO_MIN_MAJOR" "$GO_MIN_MINOR"; then
  echo "Error: Go ${GO_MIN_MAJOR}.${GO_MIN_MINOR}+ is required. Found: ${go_version_trimmed}"
  exit 1
fi

echo "Go version detected: ${go_version_trimmed}"

print_step "Bootstrapping Go module"
if [[ ! -f "go.mod" ]]; then
  echo "No go.mod found. Initializing module as 'cluster'."
  go mod init cluster
else
  echo "go.mod already exists."
fi

print_step "Resolving dependencies"
go mod tidy

print_step "Building binary"
go build -o cluster .

print_step "Checking Redis tooling (optional)"
if have_cmd redis-cli; then
  if redis-cli ping >/dev/null 2>&1; then
    echo "Redis is reachable via redis-cli."
  else
    echo "redis-cli is installed, but Redis does not appear reachable right now."
    echo "Start Redis before running the service (example: redis-server)."
  fi
else
  echo "redis-cli not found. Install Redis locally if you need to run this service on this machine."
  echo "macOS (Homebrew): brew install redis"
fi

print_step "Done"
echo "Install complete."
echo "Run the service with: ./cluster"
