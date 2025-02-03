#!/bin/sh

# Ensure the S3 bucket name is provided as an environment variable
if [ -z "$S3_BUCKET" ]; then
  echo "Error: S3_BUCKET environment variable is not set."
  exit 1
fi

# Ensure the file list is passed as an argument
if [ -z "$FILE_LIST" ]; then
  echo "Error: FILE_LIST environment variable is not set."
  exit 1
fi

# Ensure the output directory is passed as an argument
if [ -z "$S3_OUTPUT_DIRECTORY" ]; then
  echo "Error: S3_OUTPUT_DIRECTORY environment variable is not set."
  exit 1
fi

echo "S3_BUCKET is set to: $S3_BUCKET"
echo "FILE_LIST is set to: $FILE_LIST"
echo "S3_OUTPUT_DIRECTORY is set to: $S3_OUTPUT_DIRECTORY"

# Local directory to store downloaded files
LOCAL_DIRECTORY="/app/GNorm2/input"

# Convert JSON string to a bash array using Python
files=$(python3 -c "import sys, json; files = json.loads(sys.argv[1]); print(' '.join(files))" "$FILE_LIST")

if [ -z "$files" ]; then
  echo "No files to process in FILE_LIST."
  exit 0
fi

echo "Files to process: $files"

# Download specific files from S3 to the local directory
echo "Downloading specified files from S3..."
for file in $files; do
  aws s3 cp "s3://${S3_BUCKET}/${file}" "$LOCAL_DIRECTORY"

  if [ $? -ne 0 ]; then
    echo "Error: Failed to download file $file from S3."
    exit 1
  fi
done

# Change directory to /app/GNorm2 to ensure relative paths work
cd /app/GNorm2

# Define input and output directories for GNorm2 processing
INPUT="input"
OUTPUT="output"

# Step 1: SR Processing
echo "Running SR processing with GNormPlus..."
java -Xmx60G -Xms30G -jar GNormPlus.jar ${INPUT} tmp_SR setup.SR.txt

# Step 2: GNR+SA Processing
echo "Running GeneNER and Species Assignment..."
python GeneNER_SpeAss_run.py -i tmp_SR -r tmp_GNR -a tmp_SA -n gnorm_trained_models/geneNER/GeneNER-Bioformer.h5 -s gnorm_trained_models/SpeAss/SpeAss-Bioformer.h5

# Step 3: GN Processing
echo "Running GN processing with GNormPlus..."
java -Xmx60G -Xms30G -jar GNormPlus.jar tmp_SA ${OUTPUT} setup.GN.txt

# Clean up temporary files
echo "Cleaning up temporary files..."
rm -rf tmp_SR/*
rm -rf tmp_GNR/*
rm -rf tmp_SA/*

# Copy processed files from output directory to S3 output directory
echo "Uploading processed files to S3 output directory..."
aws s3 cp "/app/GNorm2/${OUTPUT}/" "s3://${S3_BUCKET}/${S3_OUTPUT_DIRECTORY}" --recursive

if [ $? -ne 0 ]; then
  echo "Error: Failed to upload processed files to S3."
  exit 1
fi

echo "Script executed successfully."

# Exit with success code
exit 0
