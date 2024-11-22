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
- Extract full-text articles from PubMed Central (PMC) via API.
- Create summaries of articles using Claude to provide context for downstream tasks.
- Extract article metadata and store it in a vector database for metadata-based filtering.
- Convert PMC full-text articles to BioC format, which is required for subsequent processing.

**Steps:**
1. **PMC Article Extraction** → Summarize the article using Claude.
2. **Metadata Extraction** → Store metadata in a vector database.
3. **BioC Conversion** → Convert PMC XML files into BioC XML format.

### 2. **Data Preprocessing**
- Process BioC XML files using:
  - **AIONER**: Biomedical Named Entity Recognition (BioNER) to recognize biomedical entities.
  - **GNorm2**: Gene name recognition and normalization.
- Both AIONER and GNorm2 are executed as AWS Fargate containers.

### 3. **Data Processing**
- Use sliding window chunking on passage contents from GNorm2 outputs.
- Enhance chunks with contextual information by prepending summaries and annotations.
- Generate embeddings using the `NeuML/pubmedbert-base-embeddings` model.
- Store chunks and embeddings in Qdrant vector database along with chunk metadata.

### 4. **Data Retrieval**
- Accept user queries and apply optional filters.
- Perform similarity searches on the vector database using query embeddings.
- Filter and retrieve chunks that match the query criteria, based on metadata.
- Retrieve the top 3 articles with up to 5 chunks from each article and output the results as JSON files.

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
   - Results are stored as JSON files in the `results/` directory.

---

## Technologies Used
- **Programming Language**: Python
- **Vector Database**: Qdrant
- **Relational Database**: PostgreSQL
- **Large Language Model**: Claude (via AWS Bedrock)
- **Container Management**: AWS Fargate
- **Package Management**: Poetry

---




To calculate accuracy metrics using the annotation counts, you can design some logical metrics that measure the quality of annotations based on the special cases you are tracking. Here are a few metrics you can consider:

### 1. **Correct Annotations (CA)**
This metric will count all the annotations that do not fall into the error categories (`incorrect_text`, `wrong_species_geneid`, `partial_annotation`, `combined_case`). You can calculate it as:
\[ \text{CA} = \text{Total Annotations} - (\text{incorrect_text} + \text{wrong_species_geneid} + \text{partial_annotation} + \text{combined_case}) \]

### 2. **Annotation Accuracy (AA)**
This metric reflects the proportion of correct annotations out of the total number of annotations:
\[ \text{AA} = \frac{\text{CA}}{\text{Total Annotations}} \times 100 \]

### 3. **Error Rate (ER)**
This metric reflects the proportion of erroneous annotations:
\[ \text{ER} = \frac{\text{incorrect_text} + \text{wrong_species_geneid} + \text{partial_annotation} + \text{combined_case}}{\text{Total Annotations}} \times 100 \]

### 4. **Missed Annotation Rate (MAR)**
This metric indicates the proportion of missed annotations (i.e., words starting with `?`) relative to the total number of annotations and text:
\[ \text{MAR} = \frac{\text{missed_annotations}}{\text{Total Annotations} + \text{missed_annotations}} \times 100 \]

### 5. **Precision (P)**
You could consider precision as a measure of the proportion of annotations that were identified correctly out of the total annotations, excluding missed ones:
\[ \text{P} = \frac{\text{CA}}{\text{CA} + \text{missed_annotations}} \times 100 \]

### 6. **Recall (R)**
Recall would reflect how many correct annotations were identified compared to the total annotations that should have been identified (including missed annotations):
\[ \text{R} = \frac{\text{CA}}{\text{Total Annotations} + \text{missed_annotations}} \times 100 \]

### Example Calculation
Based on your report:

```text
annotation_count: 46
incorrect_text: 9
wrong_species_geneid: 0
partial_annotation: 0
combined_case: 0
missed_annotations: 0
```

Here’s how the metrics would look:
1. **Correct Annotations (CA)**:
\[ 46 - (9 + 0 + 0 + 0) = 37 \]
2. **Annotation Accuracy (AA)**:
\[ \frac{37}{46} \times 100 = 80.43\% \]
3. **Error Rate (ER)**:
\[ \frac{9}{46} \times 100 = 19.57\% \]
4. **Missed Annotation Rate (MAR)**:
\[ \frac{0}{46 + 0} \times 100 = 0\% \]
5. **Precision (P)**:
\[ \frac{37}{37 + 0} \times 100 = 100\% \]
6. **Recall (R)**:
\[ \frac{37}{46 + 0} \times 100 = 80.43\% \]

### Python Code to Compute Metrics

Here’s a Python function to compute and print these metrics:

```python
def calculate_metrics(annotation_count, incorrect_text, wrong_species_geneid, partial_annotation, combined_case, missed_annotations):
    # Calculate Correct Annotations (CA)
    correct_annotations = annotation_count - (incorrect_text + wrong_species_geneid + partial_annotation + combined_case)

    # Calculate metrics
    annotation_accuracy = (correct_annotations / annotation_count) * 100
    error_rate = ((incorrect_text + wrong_species_geneid + partial_annotation + combined_case) / annotation_count) * 100
    missed_annotation_rate = (missed_annotations / (annotation_count + missed_annotations)) * 100 if missed_annotations > 0 else 0
    precision = (correct_annotations / (correct_annotations + missed_annotations)) * 100 if correct_annotations + missed_annotations > 0 else 0
    recall = (correct_annotations / (annotation_count + missed_annotations)) * 100 if annotation_count + missed_annotations > 0 else 0

    # Print the metrics
    print(f"Correct Annotations (CA): {correct_annotations}")
    print(f"Annotation Accuracy (AA): {annotation_accuracy:.2f}%")
    print(f"Error Rate (ER): {error_rate:.2f}%")
    print(f"Missed Annotation Rate (MAR): {missed_annotation_rate:.2f}%")
    print(f"Precision (P): {precision:.2f}%")
    print(f"Recall (R): {recall:.2f}%")

# Example usage with your data
calculate_metrics(
    annotation_count=46,
    incorrect_text=9,
    wrong_species_geneid=0,
    partial_annotation=0,
    combined_case=0,
    missed_annotations=0
)
```

This will print the accuracy metrics based on the data you have. You can adjust the input values as needed for other XML files.
