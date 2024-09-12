#!/bin/bash

###### ToDo
# Connect to AWS

###### Run the Data Ingestion Pipeline
python src/data_ingestion/ingest_pubmed_articles.py

# Check if Python script executed successfully
if [ $? -eq 0 ]; then
    echo "Data Ingestion Pipeline executed successfully."
else
    echo "Data Ingestion Pipeline failed."
    exit 1
fi

####### Run the AIONER container

# Run the container
docker run -it \
  --name my-aioner-container \
  -v $(pwd)/data/bioc_xml:/app/example/input \
  -v $(pwd)/data/bioformer_annotated:/app/example/output/bioformer_annotated \
  -v $(pwd)/data/pubmedbert_annotated:/app/example/output/pubmedbert_annotated \
  my-aioner

# Run AIONER Pipeline with Bioformer-Softmax in Container
docker exec -it my-aioner-container \
python AIONER/src/AIONER_Run.py \
  -i ../example/input/ \
  -m ../pretrained_models/AIONER/Bioformer-softmax-AIONER.h5 \
  -v ../vocab/AIO_label.vocab \
  -e ALL \
  -o ../example/output/bioformer_annotated
# Check if Pipeline executed successfully
if [ $? -eq 0 ]; then
    echo "AIONER Pipeline with Bioformer-Softmax executed successfully."
else
    echo "AIONER Pipeline with Bioformer-Softmax Pipeline failed."
    exit 1
fi


# Run AIONER Pipeline with PubmedBERT-CRF in Container
docker exec -it my-aioner-container \
python AIONER/src/AIONER_Run.py \
  -i ../example/input/ \
  -m ../pretrained_models/AIONER/PubmedBERT-CRF-AIONER.h5 \
  -v ../vocab/AIO_label.vocab \
  -e ALL \
  -o ../example/output/pubmedbert_annotated
# Check if Pipeline executed successfully
if [ $? -eq 0 ]; then
    echo "AIONER Pipeline with PubmedBERT-CRF executed successfully."
else
    echo "AIONER Pipeline with PubmedBERT-CRF Pipeline failed."
    exit 1
fi

######### Run GNorm2

# Run the container
docker run -it \
  --name my-gnorm2-container \
  -v $(pwd)/data/bioformer_annotated:/app/input/bioformer_annotated \
  -v $(pwd)/data/pubmedbert_annotated:/app/input/pubmedbert_annotated \
  -v $(pwd)/data/bioformer_annotated_normalized:/app/output/bioformer_annotated_normalized \
  -v $(pwd)/data/pubmedbert_annotated_normalized:/app/output/pubmedbert_annotated_normalized \
  my-gnorm2

# Run Gnorm2 Pipeline on AIONER Output
docker exec -it my-gnorm2-container \
sh ./Gnorm2.sh input/bioformer_annotated output/bioformer_annotated_normalized
# Check if Pipeline executed successfully
if [ $? -eq 0 ]; then
    echo "GNorm2 Pipeline with Bioformer-Softmax executed successfully."
else
    echo "GNorm2 Pipeline with Bioformer-Softmax Pipeline failed."
    exit 1
fi

docker exec -it my-gnorm2-container \
sh ./Gnorm2.sh input/pubmedbert_annotated output/pubmedbert_annotated_normalized
if [ $? -eq 0 ]; then
    echo "GNorm2 Pipeline with PubmedBERT-CRF executed successfully."
else
    echo "GNorm2 Pipeline with PubmedBERT-CRF Pipeline failed."
    exit 1
fi

######### Run Data Migration Util to copy processed files to S3


######### Run the Data Processing Pipeline - Chunking + Embedding + Indexing
