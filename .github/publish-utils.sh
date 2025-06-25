#!/bin/bash

# Utility functions for publishing snapshots to Maven

set -e

# Function to execute curl commands with retry and error handling
execute_curl_with_retry() {
  local url="$1"
  local method="$2"
  local output_file="$3"
  local upload_file="$4"
  local max_retries=3
  local retry_count=0
  local sleep_time=10

  while [ $retry_count -lt $max_retries ]; do
    echo "Attempting curl request to ${url} (attempt $((retry_count + 1))/${max_retries})"

    local curl_cmd="curl -s -u \"${SONATYPE_USERNAME}:${SONATYPE_PASSWORD}\""

    case "$method" in
      "GET")
        if [ -n "$output_file" ]; then
          curl_cmd="$curl_cmd -o \"$output_file\""
        fi
        curl_cmd="$curl_cmd \"$url\""
        ;;
      "PUT")
        curl_cmd="$curl_cmd --upload-file \"$upload_file\" \"$url\""
        ;;
      "HEAD")
        curl_cmd="$curl_cmd -I \"$url\""
        ;;
    esac

    echo "Executing: $curl_cmd"
    if eval $curl_cmd; then
      local http_code=$(curl -s -o /dev/null -w "%{http_code}" -u "${SONATYPE_USERNAME}:${SONATYPE_PASSWORD}" "$url")
      if [[ "$http_code" =~ ^[23] ]]; then
        echo "Request successful (HTTP $http_code)"
        return 0
      else
        echo "Request failed with HTTP code: $http_code"
      fi
    else
      echo "Curl command failed"
    fi

    retry_count=$((retry_count + 1))
    if [ $retry_count -lt $max_retries ]; then
      echo "Retrying in ${sleep_time} seconds..."
      sleep $sleep_time
      sleep_time=$((sleep_time * 2))  # Exponential backoff
    fi
  done

  echo "All retry attempts failed for ${url}"
  return 1
}

# Function to extract JAR version from metadata
extract_jar_version() {
  local project="$1"
  local metadata_file="$2"

  echo "Extracting JAR version for ${project} from ${metadata_file}"

  if [ ! -s "$metadata_file" ]; then
    echo "Warning: Metadata file is empty or does not exist for ${project}"
    return 1
  fi

  local latest_jar_version
  latest_jar_version=$(xmlstarlet sel -t -v "//snapshotVersion[extension='jar' and not(classifier)]/value" "$metadata_file" | head -1)

  if [ -n "$latest_jar_version" ]; then
    echo "Latest jar version for ${project}: ${latest_jar_version}"
    ACTUAL_VERSIONS["${project}"]="${latest_jar_version}"
    return 0
  else
    echo "Warning: Could not find JAR version in metadata for ${project}"
    return 1
  fi
}

# Function to process metadata for a single project
process_project_metadata() {
  local project="$1"
  local current_version="$2"
  local commit_id="$3"

  echo "Processing metadata for ${project}"

  # Create a temporary metadata file with commit ID
  TEMP_DIR=$(mktemp -d)
  METADATA_FILE="${TEMP_DIR}/maven-metadata.xml"

  # Download the current metadata from the repository
  META_URL="https://aws.oss.sonatype.org/content/repositories/snapshots/org/opensearch/${project}/${current_version}/maven-metadata.xml"
  echo "Downloading metadata from ${META_URL}"

  # Wait a bit to ensure the metadata file is available after publishing
  sleep 10

  # Download metadata with retry logic
  if execute_curl_with_retry "$META_URL" "GET" "$METADATA_FILE"; then
    echo "Successfully downloaded metadata file"

    # Extract JAR version using the abstracted function
    if extract_jar_version "$project" "$METADATA_FILE"; then
      # Modify the metadata to include commit ID
      cp "${METADATA_FILE}" "${METADATA_FILE}.bak"

      awk -v commit="${commit_id}" '
        /<versioning>/ {
          print $0
          print "  <commitId>" commit "</commitId>"
          next
        }
        {print}
      ' "${METADATA_FILE}.bak" > "${METADATA_FILE}"

      echo "Modified metadata content:"
      cat "${METADATA_FILE}"

      # Upload the modified metadata back
      echo "Uploading modified metadata to ${META_URL}"
      if ! execute_curl_with_retry "$META_URL" "PUT" "" "$METADATA_FILE"; then
        echo "Failed to upload modified metadata for ${project}"
      fi
    else
      echo "Failed to extract JAR version for ${project}, skipping metadata modification"
    fi
  else
    echo "Failed to download metadata for ${project} after all retries, skipping"
  fi

  # Clean up
  rm -rf "${TEMP_DIR}"
}

# Function to create/update commit ID to version mapping for a specific project
update_commit_mapping_for_project() {
  local project="$1"
  local current_version="$2"
  local commit_id="$3"
  local snapshot_repo_url="$4"

  echo "Creating/updating commit ID to version mapping for ${project}..."

  MAPPING_DIR=$(mktemp -d)

  # Generate artifact-specific commit history filename
  local commit_map_filename="commit-history-${project}.json"
  MAPPING_FILE="${MAPPING_DIR}/${commit_map_filename}"

  # Define the URL for the mapping file in the artifact directory
  MAPPING_URL="${snapshot_repo_url}org/opensearch/${project}/${commit_map_filename}"

  # Try to download existing mapping file if it exists
  if execute_curl_with_retry "$MAPPING_URL" "GET" "$MAPPING_FILE"; then
    echo "Downloaded existing commit history file for ${project}"
  else
    echo "No existing commit history file found for ${project}, creating new one"
    echo '{"mappings":[]}' > "${MAPPING_FILE}"
  fi

  # Get the actual artifact version for this project
  local artifact_version="${ACTUAL_VERSIONS[$project]}"
  if [ -z "$artifact_version" ]; then
    echo "Warning: No artifact version found for ${project}, using base version"
    artifact_version="$current_version"
  fi

  # Add new mapping entry
  TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

  # Use temporary file for JSON manipulation
  TEMP_JSON="${MAPPING_DIR}/temp.json"

  # Use jq to add the new mapping or update existing one
  cat "${MAPPING_FILE}" | jq --arg commit "$commit_id" \
                              --arg timestamp "$TIMESTAMP" \
                              --arg project "$project" \
                              --arg base_version "$current_version" \
                              --arg artifact_version "$artifact_version" '
  # Look for an existing entry with this commit ID
  if (.mappings | map(select(.commit_id == $commit)) | length) == 0 then
    # No entry exists, add a new one
    .mappings += [{"commit_id": $commit, "timestamp": $timestamp, "artifacts": {($project): {"base_version": $base_version, "artifact_version": $artifact_version}}}]
  else
    # Update the existing entry
    .mappings = [.mappings[] | if .commit_id == $commit then
      # Update timestamp and merge artifacts
      . + {"timestamp": $timestamp, "artifacts": (.artifacts + {($project): {"base_version": $base_version, "artifact_version": $artifact_version}})}
    else . end]
  end
  ' > "${TEMP_JSON}"

  mv "${TEMP_JSON}" "${MAPPING_FILE}"

  # Sort mappings by timestamp (newest first) for easier lookup
  cat "${MAPPING_FILE}" | jq '.mappings |= sort_by(.timestamp) | .mappings |= reverse' > "${TEMP_JSON}"
  mv "${TEMP_JSON}" "${MAPPING_FILE}"

  # Print the updated mapping for debugging
  echo "Updated commit history file content for ${project}:"
  cat "${MAPPING_FILE}"

  # Upload the mapping file
  echo "Uploading commit history file to ${MAPPING_URL}"
  if execute_curl_with_retry "$MAPPING_URL" "PUT" "" "$MAPPING_FILE"; then
    echo "Successfully uploaded commit history file for ${project}"
  else
    echo "Failed to upload commit history file for ${project}"
    exit 1
  fi

  # Clean up
  rm -rf "${MAPPING_DIR}"
}

# Main function to handle snapshot publishing and metadata updates
publish_snapshots_and_update_metadata() {
  local current_version="$1"
  local commit_id="$2"

  # Get credentials to upload files directly
  export SONATYPE_USERNAME=$(aws secretsmanager get-secret-value --secret-id maven-snapshots-username --query SecretString --output text)
  export SONATYPE_PASSWORD=$(aws secretsmanager get-secret-value --secret-id maven-snapshots-password --query SecretString --output text)
  echo "::add-mask::$SONATYPE_USERNAME"
  echo "::add-mask::$SONATYPE_PASSWORD"
  export SNAPSHOT_REPO_URL="https://aws.oss.sonatype.org/content/repositories/snapshots/"

  # Publish snapshots to maven
  cd build/resources/publish/
  cp -a $HOME/.m2/repository/* ./
  ./publish-snapshot.sh ./

  echo "Snapshot publishing completed. Now uploading commit ID metadata..."

  # Define the Spark projects
  PROJECTS=("opensearch-spark-standalone_2.12" "opensearch-spark-ppl_2.12" "opensearch-spark-sql-application_2.12")

  # Create a dictionary to store the actual artifact versions
  declare -A ACTUAL_VERSIONS

  # Process metadata for each project
  for PROJECT in "${PROJECTS[@]}"; do
    process_project_metadata "$PROJECT" "$current_version" "$commit_id"
  done

  # Print all collected actual versions
  echo "Collected actual artifact versions:"
  for project in "${!ACTUAL_VERSIONS[@]}"; do
    echo "$project: ${ACTUAL_VERSIONS[$project]}"
  done

  # Create/update commit ID to version mapping for each project
  for PROJECT in "${PROJECTS[@]}"; do
    update_commit_mapping_for_project "$PROJECT" "$current_version" "$commit_id" "$SNAPSHOT_REPO_URL"
  done

  echo "All commit mapping files updated successfully"
}