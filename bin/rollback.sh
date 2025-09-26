#!/bin/bash

# A script to roll back a failed or incorrect release by reverting the release commit.
#
# This script will:
# 1.  Delete the remote Git tag for the release.
# 2.  Delete the local Git tag.
# 3.  Create a new commit that reverts the "Releasing version" commit, preserving history.
# 4.  Push the new revert commit to the remote repository.
#

# Exit immediately if a command exits with a non-zero status.
set -e

# --- Function to display script usage ---
usage() {
  echo "Usage: $0 -releaseVersion <version>"
  echo "  -releaseVersion   The version of the release to roll back (e.g., 1.0.0)."
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
  *) # unknown option
    usage
    ;;
  esac
done

# --- Validate Inputs ---
if [ -z "$releaseVersion" ]; then
  echo "Error: -releaseVersion is required."
  usage
fi

# --- Safety Check ---
echo "Performing safety check on last two commits..."
LAST_COMMIT_MSG=$(git log -1 --pretty=%B)
SECOND_LAST_COMMIT_MSG=$(git log -1 --skip=1 --pretty=%B)

# Check if the last two commits seem to be from the release script.
# This is a safeguard to prevent accidental rollbacks of other work.
if [[ "$LAST_COMMIT_MSG" != "Next version "* || "$SECOND_LAST_COMMIT_MSG" != "Releasing version $releaseVersion"* ]]; then
  echo "--------------------------------------------------------"
  echo " ERROR: Safety check failed!"
  echo " The last two commits do not match the expected release pattern."
  echo "--------------------------------------------------------"
  echo "Expected HEAD:      'Next version ...'"
  echo "Actual HEAD:        '$LAST_COMMIT_MSG'"
  echo ""
  echo "Expected HEAD~1:    'Releasing version $releaseVersion'"
  echo "Actual HEAD~1:      '$SECOND_LAST_COMMIT_MSG'"
  echo ""
  echo "Rollback aborted to prevent data loss."
  exit 1
fi
echo "Safety check passed."

# --- User Confirmation ---
echo "--------------------------------------------------------"
echo " Maven Release Rollback Script"
echo "--------------------------------------------------------"
echo "This will UNDO the release for version: $releaseVersion"
echo "This operation will:"
echo "  1. Delete the remote tag."
echo "  2. Delete the local tag."
echo "  3. Create a new commit to revert the 'Releasing version' changes."
echo "  4. Push the revert commit to the remote repository."
echo "--------------------------------------------------------"

read -p "Are you sure you want to continue? [y/N] " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
  echo "Rollback cancelled by user."
  exit 0
fi

echo "Getting project artifactId..."
prefix=$(mvn help:evaluate -Dexpression=project.artifactId -q -DforceStdout)
tagName="$prefix-$releaseVersion"

echo "Deleting remote tag '$tagName'..."
# The || true prevents the script from exiting if the tag doesn't exist remotely
git push origin --delete "$tagName" || echo "Warning: Remote tag could not be deleted (it may not exist)."

echo "Deleting local tag '$tagName'..."
git tag -d "$tagName"

echo "Reverting the release commit..."
# This command identifies the release commit (HEAD~1) and creates a new commit
# that undoes its changes, using the default "Revert..." commit message.
git revert --no-edit HEAD~1

# --- 5. Push changes ---
echo "Pushing revert commit to remote repository..."
git push

echo "--------------------------------------------------------"
echo " Rollback process completed successfully!"
echo " The 'Releasing version' commit has been reverted and the tag has been removed."
echo "--------------------------------------------------------"

