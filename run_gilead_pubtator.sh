#!/bin/bash

###### ToDo
# Connect to AWS

# Get the BioC PMC Articles from AWS in EC2
aws s3 cp s3://gilead-pubtator/bioc_full_text_articles /home/ubuntu/gilead_setup/data/pmc_articles --recursive


####### Run the Data Ingestion Pipeline to fetch PMC articles in S3
#python src/data_ingestion/ingest_pubmed_articles.py
#
## Check if Python script executed successfully
#if [ $? -eq 0 ]; then
#    echo "Data Ingestion Pipeline executed successfully."
#else
#    echo "Data Ingestion Pipeline failed."
#    exit 1
#fi

###### Run AWS CLI command to fetch articles in S3 to EC2 data/pmc_articles folder
#aws s3 cp s3://gilead-pubtator/pmc_articles /home/ubuntu/gilead_setup/data/pmc_articles --recursive
#
## Check if Python script executed successfully
#if [ $? -eq 0 ]; then
#    echo "Data Ingested inside EC2 successfully."
#else
#    echo "Data Ingestion Pipeline failed."
#    exit 1
#fi


####### Run the AIONER container

# Run the container
sudo docker run -it \
  --name my-aioner-container \
  -v /home/ubuntu/gilead_setup/data/pmc_articles:/app/example/input \
  -v /home/ubuntu/gilead_setup/data/aioner_annotated/bioformer_annotated:/app/example/output/bioformer_annotated \
  -v /home/ubuntu/gilead_setup/data/aioner_annotated/pubmedbert_annotated:/app/example/output/pubmedbert_annotated \
  my-aioner

# Run AIONER Pipeline with Bioformer-Softmax in Container
sudo docker exec -it my-aioner-container bash -c "\
cd src && \
python AIONER_Run.py \
  -i ../example/input/ \
  -m ../pretrained_models/AIONER/Bioformer-softmax-AIONER.h5 \
  -v ../vocab/AIO_label.vocab \
  -e ALL \
  -o ../example/output/bioformer_annotated"
# Check if Pipeline executed successfully
if [ $? -eq 0 ]; then
    echo "AIONER Pipeline with Bioformer-Softmax executed successfully."
else
    echo "AIONER Pipeline with Bioformer-Softmax Pipeline failed."
    exit 1
fi


# Run AIONER Pipeline with PubmedBERT-CRF in Container
sudo docker exec -it my-aioner-container bash -c "\
cd src && \
python AIONER_Run.py \
  -i ../example/input/ \
  -m ../pretrained_models/AIONER/PubmedBERT-CRF-AIONER.h5 \
  -v ../vocab/AIO_label.vocab \
  -e ALL \
  -o ../example/output/pubmedbert_annotated"

# Check if Pipeline executed successfully
if [ $? -eq 0 ]; then
    echo "AIONER Pipeline with PubmedBERT-CRF executed successfully."
else
    echo "AIONER Pipeline with PubmedBERT-CRF Pipeline failed."
    exit 1
fi

######### Run GNorm2

# Run the container
sudo docker run -it \
  --name my-gnorm2-container \
  -v /home/ubuntu/gilead_setup/data/aioner_annotated/bioformer_annotated:/app/input/bioformer_annotated \
  -v /home/ubuntu/gilead_setup/data/aioner_annotated/pubmedbert_annotated:/app/input/pubmedbert_annotated \
  -v /home/ubuntu/gilead_setup/data/gnorm2_annotated/bioformer_annotated:/app/output/bioformer_annotated \
  -v /home/ubuntu/gilead_setup/data/gnorm2_annotated/pubmedbert_annotated:/app/output/pubmedbert_annotated \
  my-gnorm2

# Run Gnorm2 Pipeline on AIONER Bioformer Output
sudo docker exec -dit my-gnorm2-container bash -c "\
python GeneNER_SpeAss_run.py \
  -i input/bioformer_annotated \
  -r output/bioformer_annotated/ner_tagged \
  -a output/bioformer_annotated/sa_tagged \
  -n gnorm_trained_models/geneNER/GeneNER-Bioformer.h5 \
  -s gnorm_trained_models/SpeAss/SpeAss-Bioformer.h5
"

# Check if Pipeline executed successfully
if [ $? -eq 0 ]; then
    echo "GNorm2 Pipeline with Bioformer-Softmax executed successfully."
else
    echo "GNorm2 Pipeline with Bioformer-Softmax Pipeline failed."
    exit 1
fi

# Run Gnorm2 Pipeline on AIONER Pubmedbert Output
sudo docker exec -dit my-gnorm2-container bash -c "\
python GeneNER_SpeAss_run.py \
  -i input/pubmedbert_annotated \
  -r output/pubmedbert_annotated/ner_tagged \
  -a output/pubmedbert_annotated/sa_tagged \
  -n gnorm_trained_models/geneNER/GeneNER-Bioformer.h5 \
  -s gnorm_trained_models/SpeAss/SpeAss-Bioformer.h5
"

# Check if Pipeline executed successfully
if [ $? -eq 0 ]; then
    echo "GNorm2 Pipeline with PubmedBERT-CRF executed successfully."
else
    echo "GNorm2 Pipeline with PubmedBERT-CRF Pipeline failed."
    exit 1
fi

######### Run Data Migration Util to copy processed files to S3

aws s3 cp /home/ubuntu/gilead_setup/data/aioner_annotated s3://gilead-pubtator/aioner_annotated --recursive
aws s3 cp /home/ubuntu/gilead_setup/data/gnorm2_annotated s3://gilead-pubtator/gnorm2_annotated --recursive


######### Run Data Migration Util to copy processed files from S3 to local

aws s3 cp s3://gilead-pubtator/aioner_annotated /Users/ishaanbhatnagar/Workspace/Gilead/ResearchAssistant/GileadPubtator/data/aioner_annotated --recursive
aws s3 cp s3://gilead-pubtator/gnorm2_annotated /Users/ishaanbhatnagar/Workspace/Gilead/ResearchAssistant/GileadPubtator/data/gnorm2_annotated --recursive

aws s3 cp s3://gilead-pubtator/gnorm2_annotated /Users/ishaanbhatnagar/Workspace/Gilead/ResearchAssistant/gilead_pubtator_results/gnorm2_annotated --recursive

######### Run the Data Processing Pipeline - Chunking + Embedding + Indexing
