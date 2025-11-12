#!/bin/bash

# Utility functions for publishing snapshots to Maven
# Simplified version for S3-based publishing using standardized publish-snapshot.sh

set -e

# Main function to handle snapshot publishing
publish_snapshots_and_update_metadata() {
  local current_version="$1"
  local commit_id="$2"

  echo "Publishing snapshots for version: ${current_version}, commit: ${commit_id}"

  # Validate that SNAPSHOT_REPO_URL is set (should be exported from GitHub Actions workflow)
  if [ -z "$SNAPSHOT_REPO_URL" ]; then
    echo "Error: SNAPSHOT_REPO_URL environment variable is not set"
    echo "This should be set by the GitHub Actions workflow from secrets.MAVEN_SNAPSHOTS_S3_REPO"
    exit 1
  fi

  echo "Using snapshot repository: ${SNAPSHOT_REPO_URL}"

  # Prepare publish-snapshot.sh from opensearch-build repository
  echo "Preparing publish-snapshot.sh script..."
  mkdir -p build/resources/publish/
  cp build/publish/publish-snapshot.sh build/resources/publish/
  chmod +x build/resources/publish/publish-snapshot.sh

  # Copy artifacts from local Maven repository
  echo "Copying artifacts from local Maven repository..."
  cd build/resources/publish/
  cp -a "$HOME"/.m2/repository/* ./

  # List opensearch artifacts for verification
  echo "Verifying OpenSearch artifacts:"
  find org/opensearch/ -name "*.jar" -o -name "*.pom" | head -20 || echo "No artifacts found"

  # Call the standardized publish-snapshot.sh script
  # This script handles S3 publishing via maven-s3-wagon when SNAPSHOT_REPO_URL points to S3
  echo "Publishing snapshots using publish-snapshot.sh..."
  ./publish-snapshot.sh ./

  echo "Snapshot publishing completed successfully"
  echo "Artifacts published to: ${SNAPSHOT_REPO_URL}"
}
