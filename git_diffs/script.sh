#!/usr/bin/env bash

# Between last commit and base commit 

set -euo pipefail

# ===== CONFIG =====
FOLDER_1="pg_parquet/"
BASE_COMMIT_1="6416a908576cc145b9c37cf6ce8611b28fa7c0fa"

FOLDER_2="postgres/"
BASE_COMMIT_2="fc3ee63198c9614d431156335967e64e050275ed"

OUTPUT_DIR="git_diffs"
# ==================

process_folder() {
  local folder="$1"
  local base_commit="$2"

  echo "Processing $folder (base: $base_commit)"

  git diff --name-only "$base_commit"..HEAD -- "$folder" | \
  while read -r file; do

    diff_content=$(git diff "$base_commit"..HEAD -- "$file")

    # skip empty diffs
    if [ -z "$diff_content" ]; then
      continue
    fi

    # safe filename
    safe_name=$(echo "$file" | sed 's|/|__|g')
    output_file="$OUTPUT_DIR/${safe_name}.diff"

    echo "Saving $output_file"

    printf "%s\n" "$diff_content" > "$output_file"

  done
}

# process both independently
process_folder "$FOLDER_1" "$BASE_COMMIT_1"
process_folder "$FOLDER_2" "$BASE_COMMIT_2"

echo "Done. Diffs stored in $OUTPUT_DIR/"