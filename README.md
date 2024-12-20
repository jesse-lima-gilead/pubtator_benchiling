# Pubtator Search System

## Description

This project is designed to search entities and relationships within biomedical publications. It processes PubMed Central (PMC) articles through a series of automated pipelines to ingest, preprocess, and retrieve data efficiently. The final output provides insightful information tailored to user queries using advanced entity recognition and vector-based search. The goal is to generate contextually enriched embeddings and annotations for efficient metadata-based retrieval and search.

---

## Table of Contents
- [Processes Overview](#processes-overview)
- [Directory Structure](#directory-structure)
- [How to Run](#how-to-run)
- [Technologies Used](#technologies-used)

---

## Processes Overview

The project workflow is divided into four key processes:

### 1. **Data Ingestion**
- Extract full-text articles from PubMed Central (PMC) via API and store them in the S3 `pubtator-poc` bucket under the `pmc_full_text_articles` directory.
- Create summaries of articles using the Bedrock Claude model (anthropic.claude-v2:1) to provide context for downstream tasks. The summaries are stored in the `articles_directory` and range between 50-80 words.
- Extract article metadata, including `pmid`, `pmcid`, `doi`, `publisher-id`, `title`, `keywords`, `authors`, `journal data`, `funding`, and `publication_date`, and store it in a vector database for metadata-based filtering.
- Convert PMC full-text articles into BioC format, ensuring the removal of excessive newlines and trimming of leading and trailing whitespaces within each line. Maintain the original text structure to support subsequent processing. Save the converted BioC articles in the `bioc_full_text_articles` directory.

**Steps:**
PMC Article Extraction → Article Summarization → Metadata Extraction → BioC Conversion

### 2. **Data Enrichment**
- Process BioC XML files using the following models:
  - **AIONER**: Biomedical Named Entity Recognition (BioNER) to identify biomedical entities. The Aioner ECS task reads articles from the `bioc_full_text_articles` directory, processes them, and stores the annotated files in the `aioner_annotated` directory.
  - **GNorm2**: Gene name recognition and normalization. The GNorm2 task reads the Aioner-processed files from the `aioner_annotated` directory, processes them, and writes the output files to the `gnorm2_annotated` directory.
- Both AIONER and GNorm2 are executed as AWS Fargate containers.

**Steps:**
AIONER Processing → GNorm2 Processing

### 3. **Data Processing**
- Apply sliding window chunking to passage contents from GNorm2 outputs in the `gnorm2_annotated` directory.
- Enhance chunks with contextual information by prepending summaries and annotations.
- Generate embeddings using the `NeuML/pubmedbert-base-embeddings` model for the enhanced chunks.
- Store the chunks and their embeddings in the Qdrant vector database, along with the chunk metadata.

**Steps:**
Chunking → Prepending Summary → Prepending Annotations → Generate Embeddings → Store in Qdrant DB

### 4. **Data Retrieval**
- Accept user queries and apply optional filters.
- Perform similarity searches on the vector database using query embeddings.
- Filter and retrieve chunks that match the query criteria, based on metadata.
- Retrieve the top 3 articles with up to 5 chunks from each article and output the results as JSON files.

**Steps:**
1. `get_user_query_embeddings` → Retrieve the embeddings for the user query.
2. `retrieve_chunks` → Retrieve chunks from the vector database using the query embeddings.
3. `filter_article_ids_by_metadata` → Filter articles based on metadata criteria (e.g., journal, year).
4. `get_intersection_article_ids` → Get the final set of article IDs that match both the query and metadata filters.
5. `parse_and_store_results` → Format and store the final results as JSON files.

---

## Directory Structure

The important directories to be known:

### **data/**
- `articles_metadata/`
  - `metadata/`: Metadata for articles.
  - `summary/`: Summaries of articles.
- `indexing/`
  - `chunks/`: Chunked article text for embedding or retrieval.
  - `embeddings/`: Precomputed embeddings for efficient similarity searches.
- `ner_processed/`
  - `aioner_annotated/`: Annotations processed by AIONER.
  - `gnorm2_annotated/`: Annotations processed by GNorm2.
- `results/`: JSON output files for user queries.
- `staging/`
  - `bioc_xml/`: BioC format XML files.
  - `pmc_xml/`: Raw PMC XML files before annotation processing.

### **data_enrichment/**
- `aioner_process/`
  - `aioner_ecs_task_runner.py`: Script for running AIONER fargate containers.
  - `Dockerfile/`: File used to create the AIONER Docker Image in the ECR.
- `gnorm2_process/`
  - `gnorm2_ecs_task_runner/`: Script for running GNorm2 fargate containers.
  - `Dockerfile/`: File used to create the GNorm2 Docker Image in the ECR.

### **data_ingestion/**
- `orchestrator.py`: Orchestrates the data ingestion pipeline.
- `pmc_articles_extractor.py`: Extracts PubMed Central articles.
- `fetch_metadata.py`: Extracts and stores article metadata in vector db.
- `pmc_to_bioc_converter.py`: Converts PMC XML files to BioC XML format.

### **data_processing/**
- `Chunking/`: Scripts for chunking articles using a sliding window.
- `Embedding/`: Scripts for embedding chunks using PubMedBERT.
- `Merging/`: Scripts for merging annotations with chunks.
- `Retrieval/`: Scripts for query-based retrieval tasks.
- `orchestrator.py`: Orchestrates the data processing pipeline.

---

## How to Run

### Prerequisites
- Install dependencies using [Poetry](https://python-poetry.org/).
- Docker setup for local databases (Qdrant and PostgreSQL).

### Steps
1. **Setup**:
   - Clone the repository and install dependencies:
     ```bash
     poetry install
     ```
   - Start Qdrant and PostgreSQL locally:
     ```bash
     docker-compose up
     ```

2. **Data Ingestion**:
   - Run the ingestion pipeline:
     ```bash
     python src/data_ingestion/orchestrator.py
     ```
   - Provide specific article IDs or queries (e.g., "lung cancer").

3. **Annotation**:
   - In AWS ECS, under `pubtator-fargate-cluster`, execute:
     - `aioner-ncbi-task`
     - `gnorm2-ncbi-task`

4. **Data Processing**:
   - Run the data processing pipeline:
     ```bash
     python src/data_processing/orchestrator.py
     ```

5. **Query Retrieval**:
   - Pass user queries with filters to the retrieval script:
     ```bash
     python src/data_processing/Retrieval/retriever_handler.py
     ```
   - **Example Query:**
     ```python
     user_query = "Effect of PM2.5 in EGFR mutation in lung cancer"
     metadata_filters = {
        "journal": "Nature",
        "year": "2023"
     }
     ```
   - Results are stored as JSON files in the `results/` directory.

---

## Technologies Used
- **Programming Language**: Python
- **Vector Database**: Qdrant
- **Relational Database**: PostgreSQL
- **Large Language Model**: Claude (via AWS Bedrock)
- **Container Management**: AWS Fargate
- **Package Management**: Poetry
- **Storage**: Amazon S3

---
