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

####### Run AIONER

# Run AIONER Pipeline with Bioformer-Softmax
python AIONER/src/AIONER_Run.py -i ../../data/bioc_xml/ -m AIONER/pretrained_models/AIONER/Bioformer-softmax-AIONER.h5 -v ../vocab/AIO_label.vocab -e ALL -o ../../data/bioformer_annotated
# Check if Pipeline executed successfully
if [ $? -eq 0 ]; then
    echo "AIONER Pipeline with Bioformer-Softmax executed successfully."
else
    echo "AIONER Pipeline with Bioformer-Softmax Pipeline failed."
    exit 1
fi


# Run AIONER Pipeline with PubmedBERT-CRF
python AIONER/src/AIONER_Run.py -i ../../data/bioc_xml/ -m AIONER/pretrained_models/AIONER/PubmedBERT-CRF-AIONER.h5 -v ../vocab/AIO_label.vocab -e ALL -o ../../data/pubmedbert_annotated
# Check if Pipeline executed successfully
if [ $? -eq 0 ]; then
    echo "AIONER Pipeline with PubmedBERT-CRF executed successfully."
else
    echo "AIONER Pipeline with PubmedBERT-CRF Pipeline failed."
    exit 1
fi

######### Run GNorm2

# Run Gnorm2 Pipeline on AIONER Output
sh Gnorm2/Gnorm2.sh data/bioformer_annotated data/bioformer_annotated_normalized
# Check if Pipeline executed successfully
if [ $? -eq 0 ]; then
    echo "GNorm2 Pipeline with Bioformer-Softmax executed successfully."
else
    echo "GNorm2 Pipeline with Bioformer-Softmax Pipeline failed."
    exit 1
fi

sh Gnorm2/Gnorm2.sh data/pubmedbert_annotated data/pubmedbert_annotated_normalized
if [ $? -eq 0 ]; then
    echo "GNorm2 Pipeline with PubmedBERT-CRF executed successfully."
else
    echo "GNorm2 Pipeline with PubmedBERT-CRF Pipeline failed."
    exit 1
fi

######### Run Data Migration Util to copy processed files to S3


######### Run the Data Processing Pipeline - Chunking + Indexing
