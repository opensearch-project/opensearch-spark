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

# Function to process metadata for a single project with commit history
process_project_metadata() {
  local project="$1"
  local current_version="$2"
  local commit_id="$3"

  echo "Processing metadata for ${project}"

  # Create a temporary metadata file
  TEMP_DIR=$(mktemp -d)
  METADATA_FILE="${TEMP_DIR}/maven-metadata.xml"
  UPDATED_METADATA="${TEMP_DIR}/maven-metadata-updated.xml"

  # Download the current metadata from the repository
  META_URL="https://central.sonatype.com/repository/maven-snapshots/org/opensearch/${project}/${current_version}/maven-metadata.xml"
  echo "Downloading metadata from ${META_URL}"

  # Wait a bit to ensure the metadata file is available after publishing
  sleep 10

  # Download metadata with retry logic
  if execute_curl_with_retry "$META_URL" "GET" "$METADATA_FILE"; then
    echo "Successfully downloaded metadata file"

    # Extract JAR version using the abstracted function
    if extract_jar_version "$project" "$METADATA_FILE"; then
      local artifact_version="${ACTUAL_VERSIONS[$project]}"
      local timestamp=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
      
      # Check if commitHistory section already exists
      if grep -q "<commitHistory>" "$METADATA_FILE"; then
        echo "Updating existing commit history in metadata"
        
        # Create a new mapping entry
        local new_mapping="    <mapping>\n      <commitId>${commit_id}</commitId>\n      <timestamp>${timestamp}</timestamp>\n      <baseVersion>${current_version}</baseVersion>\n      <artifactVersion>${artifact_version}</artifactVersion>\n    </mapping>"
        
        # Insert new mapping at the beginning of commitHistory section
        awk -v new_mapping="${new_mapping}" '
          /<commitHistory>/ {
            print $0
            print new_mapping
            next
          }
          {print}
        ' "${METADATA_FILE}" > "${UPDATED_METADATA}"
      else
        echo "Adding new commit history section to metadata"
        
        # Add commitHistory section before closing </metadata> tag
        awk -v commit="${commit_id}" -v ts="${timestamp}" -v base_ver="${current_version}" -v artifact_ver="${artifact_version}" '
          /<\/metadata>/ {
            print "  <commitHistory>"
            print "    <mapping>"
            print "      <commitId>" commit "</commitId>"
            print "      <timestamp>" ts "</timestamp>"
            print "      <baseVersion>" base_ver "</baseVersion>"
            print "      <artifactVersion>" artifact_ver "</artifactVersion>"
            print "    </mapping>"
            print "  </commitHistory>"
            print $0
            next
          }
          {print}
        ' "${METADATA_FILE}" > "${UPDATED_METADATA}"
      fi

      echo "Modified metadata content:"
      cat "${UPDATED_METADATA}"

      # Upload the modified metadata back
      echo "Uploading modified metadata to ${META_URL}"
      if execute_curl_with_retry "$META_URL" "PUT" "" "$UPDATED_METADATA"; then
        echo "Successfully updated metadata with commit history for ${project}"
      else
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

# Function to retrieve commit history from maven-metadata.xml
retrieve_commit_history() {
  local project="$1"
  local current_version="$2"
  
  echo "Retrieving commit history for ${project}..."
  
  TEMP_DIR=$(mktemp -d)
  METADATA_FILE="${TEMP_DIR}/maven-metadata.xml"
  
  # Download the metadata
  META_URL="https://central.sonatype.com/repository/maven-snapshots/org/opensearch/${project}/${current_version}/maven-metadata.xml"
  
  if execute_curl_with_retry "$META_URL" "GET" "$METADATA_FILE"; then
    if grep -q "<commitHistory>" "$METADATA_FILE"; then
      echo "Commit history found in metadata:"
      # Extract commit history section
      xmlstarlet sel -t -m "//commitHistory/mapping" \
        -v "concat('Commit: ', commitId, ' | Timestamp: ', timestamp, ' | Base: ', baseVersion, ' | Artifact: ', artifactVersion)" \
        -n "$METADATA_FILE" 2>/dev/null || \
      awk '/<commitHistory>/,/<\/commitHistory>/' "$METADATA_FILE"
    else
      echo "No commit history found in metadata for ${project}"
    fi
  else
    echo "Failed to retrieve metadata for ${project}"
  fi
  
  # Clean up
  rm -rf "${TEMP_DIR}"
}

# Main function to handle snapshot publishing and metadata updates
publish_snapshots_and_update_metadata() {
  local current_version="$1"
  local commit_id="$2"

  # Credentials are loaded from 1Password via environment variables
  if [ -z "$SONATYPE_USERNAME" ] || [ -z "$SONATYPE_PASSWORD" ]; then
    echo "Error: SONATYPE_USERNAME or SONATYPE_PASSWORD not set"
    exit 1
  fi
  echo "::add-mask::$SONATYPE_USERNAME"
  echo "::add-mask::$SONATYPE_PASSWORD"
  export SNAPSHOT_REPO_URL="https://central.sonatype.com/repository/maven-snapshots/"

  mkdir -p build/resources/publish/

  # Copy the publish script from the opensearch-build repo
  cp build/publish/publish-snapshot.sh build/resources/publish/
  chmod +x build/resources/publish/publish-snapshot.sh

  # Continue with the original flow
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

  # Optionally retrieve and display commit history for verification
  echo "Verifying commit history in metadata files:"
  for PROJECT in "${PROJECTS[@]}"; do
    retrieve_commit_history "$PROJECT" "$current_version"
  done

  echo "All metadata files updated with commit history successfully"
}