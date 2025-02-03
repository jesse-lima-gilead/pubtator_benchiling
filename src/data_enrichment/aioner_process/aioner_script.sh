#!/bin/bash

# Ensure the S3 bucket name is provided as an environment variable
if [ -z "$S3_BUCKET" ]; then
  echo "Error: S3_BUCKET environment variable is not set."
  exit 1
fi

echo "S3_BUCKET is set to: $S3_BUCKET"

# Ensure the file list is passed as an argument
if [ -z "$FILE_LIST" ]; then
  echo "Error: FILE_LIST environment variable is not set."
  exit 1
fi

echo "FILE_LIST is set to: $FILE_LIST"

# Ensure the output directory is passed as an argument
if [ -z "$S3_OUTPUT_DIRECTORY" ]; then
  echo "Error: S3_OUTPUT_DIRECTORY environment variable is not set."
  exit 1
fi

echo "S3_OUTPUT_DIRECTORY is set to: $S3_OUTPUT_DIRECTORY"

# Convert JSON string to a bash array using Python
files=$(python3 -c "import sys, json; files = json.loads(sys.argv[1]); print(' '.join(files))" "$FILE_LIST")

# Local directories for input and output
LOCAL_INPUT_DIRECTORY="/app/AIONER/example/input"
LOCAL_OUTPUT_DIRECTORY="/app/AIONER/example/output/"

# Copy all files from S3 to local directory
echo "Copying files from S3 to local directory..."
for file in $files; do
  aws s3 cp "s3://${S3_BUCKET}/${file}" "$LOCAL_INPUT_DIRECTORY"

  if [ $? -ne 0 ]; then
    echo "Error: Failed to copy file $file from S3."
    exit 1
  fi
done

# The source code directory is named "src" under /app
cd /app/AIONER/src

# Run the Python program with specified arguments for all files
echo "Running AIONER_Run.py script..."
python AIONER_Run.py \
  -i ../example/input/ \
  -m ../pretrained_models/AIONER/Bioformer-softmax-AIONER.h5 \
  -v ../vocab/AIO_label.vocab \
  -e ALL \
  -o ../example/output/

if [ $? -ne 0 ]; then
  echo "Error: Failed to run AIONER_Run.py script."
  exit 1
fi

# Copy processed files from output directory to S3
echo "Copying processed files from local output directory to S3..."
aws s3 cp "$LOCAL_OUTPUT_DIRECTORY" "s3://${S3_BUCKET}/${S3_OUTPUT_DIRECTORY}" --recursive

if [ $? -ne 0 ]; then
  echo "Error: Failed to copy processed files to S3."
  exit 1
fi

echo "Script executed successfully."
exit 0
