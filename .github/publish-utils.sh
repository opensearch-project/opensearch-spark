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

  # Update maven-metadata.xml with commitId for each artifact
  echo "Updating maven-metadata.xml files with commit ID: ${commit_id}"

  # Remove trailing slash from SNAPSHOT_REPO_URL if present
  local repo_url="${SNAPSHOT_REPO_URL%/}"

  local artifacts=("opensearch-spark-standalone_2.12" "opensearch-spark-ppl_2.12" "opensearch-spark-sql-application_2.12")

  for artifact in "${artifacts[@]}"; do
    local metadata_s3_path="org/opensearch/${artifact}/${current_version}/maven-metadata.xml"
    local local_metadata="/tmp/maven-metadata-${artifact}.xml"

    echo "Processing metadata for ${artifact}..."

    # Download current metadata from S3
    if ! aws s3 cp "${repo_url}/${metadata_s3_path}" "${local_metadata}"; then
      echo "Warning: Could not download metadata for ${artifact}, skipping..."
      continue
    fi

    # Verify download succeeded
    if [ -f "${local_metadata}" ]; then
      echo "Successfully downloaded metadata, size: $(wc -c < "${local_metadata}") bytes"
    else
      echo "Error: Metadata file not found after download for ${artifact}, skipping..."
      continue
    fi

    # Use xmlstarlet to properly inject commitId into versioning section
    if command -v xmlstarlet &> /dev/null; then
      xmlstarlet ed -L -s "/metadata/versioning" -t elem -n "commitId" -v "${commit_id}" "${local_metadata}"
    else
      # Fallback to sed if xmlstarlet not available
      sed -i "s|</versioning>|  <commitId>${commit_id}</commitId>\n  </versioning>|" "${local_metadata}"
    fi

    # Re-upload modified metadata
    if ! aws s3 cp "${local_metadata}" "${repo_url}/${metadata_s3_path}"; then
      echo "Error: Failed to upload modified metadata for ${artifact}. Permission denied or path not writable."
      continue
    fi

    # Generate and upload checksums
    sha256sum "${local_metadata}" | awk '{print $1}' > "${local_metadata}.sha256"
    sha512sum "${local_metadata}" | awk '{print $1}' > "${local_metadata}.sha512"

    if ! aws s3 cp "${local_metadata}.sha256" "${repo_url}/${metadata_s3_path}.sha256"; then
      echo "Warning: Failed to upload sha256 checksum for ${artifact}"
    fi
    if ! aws s3 cp "${local_metadata}.sha512" "${repo_url}/${metadata_s3_path}.sha512"; then
      echo "Warning: Failed to upload sha512 checksum for ${artifact}"
    fi

    echo "Successfully updated metadata for ${artifact} with commitId"
  done

  echo "Snapshot publishing completed successfully"
  echo "Artifacts published to: ${SNAPSHOT_REPO_URL}"
}
