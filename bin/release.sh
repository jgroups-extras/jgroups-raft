#!/bin/bash

# A script to automate the release process.
#
# This script will:
# 1.  Update the project version to the release version.
# 2.  Commit the version change.
# 3.  Build and install the project artifacts.
# 4.  Deploy the artifacts to the configured repository.
# 5.  Create a Git tag for the release.
# 6.  Update the project version to the next development version.
# 7.  Commit the new version change.
# 8.  Push the commits and tags to the remote repository.

# Exit immediately if a command exits with a non-zero status.
set -e

# --- Function to display script usage ---
usage() {
  echo "Usage: $0 -releaseVersion <version> -nextVersion <version>"
  echo "  -releaseVersion   The version to be released (e.g., 1.0.0)."
  echo "  -nextVersion      The next development snapshot version (e.g., 1.1.0-SNAPSHOT)."
  exit 1
}

# --- Parse Command Line Arguments ---
while [[ $# -gt 0 ]]; do
  key="$1"
  case $key in
  -releaseVersion)
    releaseVersion="$2"
    shift
    shift
    ;;
  -nextVersion)
    nextVersion="$2"
    shift
    shift
    ;;
  *) # unknown option
    usage
    ;;
  esac
done

if [ -z "$releaseVersion" ] || [ -z "$nextVersion" ]; then
  echo "Error: Both -releaseVersion and -nextVersion are required."
  usage
fi

echo "--------------------------------------------------------"
echo " Maven Release Script"
echo "--------------------------------------------------------"
echo "Releasing version:   $releaseVersion"
echo "Next dev version:    $nextVersion"
echo "--------------------------------------------------------"

read -p "Do you want to continue with the release? [y/N] " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
  echo "Release cancelled by user."
  exit 0
fi

echo "Updating POM files to release version: $releaseVersion"
mvn versions:set -DnewVersion="$releaseVersion" -DprocessAllModules=true -DgenerateBackupPoms=false

echo "Committing release version $releaseVersion"
git add 'pom.xml'
git commit -m "Releasing version $releaseVersion" --no-verify

echo "Building project artifacts for $releaseVersion"
mvn -DskipTests clean install -Prelease

echo "Starting deployment of $releaseVersion"
mvn -DskipTests deploy -Prelease
echo "Deployment finished successfully."

prefix=$(mvn help:evaluate -Dexpression=project.artifactId -q -DforceStdout)
echo "Creating Git tag as $prefix-$releaseVersion"
git tag "$prefix-$releaseVersion"
echo "Tag '$prefix-$releaseVersion' created."

echo "Updating POM files to next development version: $nextVersion..."
mvn versions:set -DnewVersion="$nextVersion" -DprocessAllModules=true -DgenerateBackupPoms=false

echo "Committing next development version to $nextVersion"
git add 'pom.xml'
git commit -m "Next version $nextVersion" --no-verify

echo "Pushing commits and tags to remote repository"
git push
git push --tags

echo "--------------------------------------------------------"
echo " Release process completed successfully!"
echo " You have released: $releaseVersion"
echo " Next development iteration in: $nextVersion"
echo "--------------------------------------------------------"
