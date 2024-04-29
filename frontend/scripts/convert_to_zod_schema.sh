#!/bin/bash

# get relative path of the directory of this script
script_dir=$(dirname "$0")
# convert it to absoluate path
script_dir=$(cd "$script_dir" && pwd)

default_input_dir="$script_dir/../../backend/schemas"
default_output_dir="$script_dir/../src/schems"

# Directory containing the JSON files
input_directory=${1:-$default_input_dir}
# Directory where the processed files will be saved
output_directory=${2:-$default_output_dir}

# Create the output directory if it doesn't exist
mkdir -p "$output_directory"

json_to_zod() {
  npx --yes json-refs resolve "$1" | \
  npx --yes json-schema-to-zod | \
  npx --yes prettier --parser typescript
}

# Loop through all .json files in the input directory
for json_file in "$input_directory"/*.json
do
    # Extract the filename without the directory path and extension
    base_filename=$(basename "$json_file" .json)

    # Define the output filename with a new extension and output directory
    output_file="$output_directory/$base_filename.ts"

    # Process the JSON file and save the output
    # Replace 'cmd' with the actual command you want to use for processing
    json_to_zod "$json_file" > "$output_file"
done

echo "Processing complete. Files saved in $output_directory"
