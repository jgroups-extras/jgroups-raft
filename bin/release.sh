#!/bin/bash

# ==============================================================================
# JGROUPS RAFT RELEASE AUTOMATION
# ==============================================================================
# This script automates the Maven release process:
# 1. Sets release version (POMs + CLI Scripts) -> Commits
# 2. Builds and Deploys to Maven Central
# 3. Tags the release
# 4. Sets next snapshot version (POMs only) -> Commits
# 5. Pushes to remote
# ==============================================================================

set -e

# --- Constants & Config ---
CLI_WRAPPER="bin/raft"
INSTALL_SCRIPT="bin/install.sh"

# Colors for UI
BLUE='\033[0;34m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# --- Helper Functions ---

log_info() { echo -e "${BLUE}[INFO] $1${NC}"; }
log_step() { echo -e "\n${GREEN}=== $1 ===${NC}"; }
log_warn() { echo -e "${YELLOW}[WARN] $1${NC}"; }
log_err()  { echo -e "${RED}[ERROR] $1${NC}"; }

usage() {
  echo "Usage: $0 -releaseVersion <version> -nextVersion <version>"
  echo "  -releaseVersion   The version to be released (e.g., 2.0.0.Final, 2.0.0.Alpha1)."
  echo "  -nextVersion      The next development snapshot version (e.g., 2.1.0-SNAPSHOT)."
  exit 1
}

# Updates the version strings inside the CLI shell scripts
update_cli_scripts() {
  local new_version="$1"
  log_info "Updating CLI scripts to version: $new_version"

  if [ -f "$CLI_WRAPPER" ]; then
    sed -i "s/^VERSION=\".*\"/VERSION=\"$new_version\"/" "$CLI_WRAPPER"
    echo " > Updated $CLI_WRAPPER"
  else
    log_warn "$CLI_WRAPPER not found."
  fi

  if [ -f "$INSTALL_SCRIPT" ]; then
    sed -i "s/^DEFAULT_VERSION=\".*\"/DEFAULT_VERSION=\"$new_version\"/" "$INSTALL_SCRIPT"
    echo " > Updated $INSTALL_SCRIPT"
  else
    log_warn "$INSTALL_SCRIPT not found."
  fi
}

set_maven_version() {
  local version="$1"
  log_info "Updating POM versions to: $version"
  mvn versions:set -DnewVersion="$version" -DprocessAllModules=true -DgenerateBackupPoms=false -q
}

git_commit() {
  local message="$1"
  shift
  local files=("$@") # Remaining arguments are files to add

  log_info "Committing changes..."
  git add "${files[@]}"
  git commit -m "$message" --no-verify
}

# --- Main Logic ---

# 1. Parse Arguments
while [[ $# -gt 0 ]]; do
  key="$1"
  case $key in
    -releaseVersion) releaseVersion="$2"; shift; shift ;;
    -nextVersion)    nextVersion="$2";    shift; shift ;;
    *) usage ;;
  esac
done

if [ -z "$releaseVersion" ] || [ -z "$nextVersion" ]; then
  log_err "Both -releaseVersion and -nextVersion are required."
  usage
fi

# 2. Confirmation
echo "--------------------------------------------------------"
echo " Maven Release Configuration"
echo "--------------------------------------------------------"
echo " Release Version:  $releaseVersion"
echo " Next Dev Version: $nextVersion"
echo "--------------------------------------------------------"
read -p "Do you want to continue with the release? [y/N] " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
  log_info "Release cancelled by user."
  exit 0
fi

# 3. Prepare Release (POMs + Scripts)
log_step "Step 1/5: Setting Release Version"
set_maven_version "$releaseVersion"
update_cli_scripts "$releaseVersion"

# Commit POMs AND Binaries (so the tag contains the correct CLI version)
git_commit "Releasing version $releaseVersion" "pom.xml" "$CLI_WRAPPER" "$INSTALL_SCRIPT"

# 4. Build and Deploy
log_step "Step 2/5: Building and Deploying"
log_info "Building and deploying artifacts..."
mvn -DskipTests clean install deploy -Prelease

# 5. Tagging
log_step "Step 3/5: Tagging Release"
prefix=$(mvn help:evaluate -Dexpression=project.artifactId -q -DforceStdout)
TAG_NAME="$prefix-$releaseVersion"
git tag "$TAG_NAME"
log_info "Created git tag: $TAG_NAME"

# 6. Prepare Next Snapshot (POMs ONLY)
log_step "Step 4/5: Setting Next Development Version"
set_maven_version "$nextVersion"
# NOTE: We DO NOT update CLI scripts here. They remain pinned to the stable release.

# Commit ONLY POMs
git_commit "Next version $nextVersion" "pom.xml"

# 7. Push
log_step "Step 5/5: Pushing to Remote"
git push
git push --tags

log_step "Release Process Completed Successfully!"
echo " > Released: $releaseVersion"
echo " > Next Dev: $nextVersion"
