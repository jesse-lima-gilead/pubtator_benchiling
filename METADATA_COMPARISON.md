# Metadata Comparison: PostgreSQL vs OpenSearch

## Overview
This document compares the metadata fields stored in PostgreSQL (relational database) versus OpenSearch (vector search database).

---

## PostgreSQL Metadata

### Document Table (`document`)
| Field Name | Type | Description | Notes |
|------------|------|-------------|-------|
| `document_grsar_id` | String (PK) | Unique document identifier | Primary key |
| `document_name` | String | Original filename | Not nullable |
| `workflow_id` | String (FK) | Workflow identifier | Foreign key to workflow table |
| `document_type` | String | File extension (e.g., .pdf, .docx) | Extracted from filename |
| `document_grsar_name` | String | Safe filename | Sanitized version |
| `source` | String | Data source (pmc, ct, apollo, etc.) | Not nullable |
| `source_path` | String | File path location | |
| `created_dt` | TIMESTAMP | Document creation timestamp | |
| `last_update_dt` | TIMESTAMP | Last update timestamp | |
| `document_file_size_in_bytes` | Integer | File size in bytes | |
| `starfish_document_valid_to` | Integer | Starfish integration field | Optional |
| `starfish_volume_display_name` | String | Starfish integration field | Optional |
| `starfish_file_extension_type` | String | Starfish integration field | Optional |
| `starfish_mt` | Integer | Starfish integration field | Optional |
| `starfish_ct` | Integer | Starfish integration field | Optional |
| `starfish_file_name` | String | Starfish integration field | Optional |
| `starfish_size_unit` | String | Starfish integration field | Optional |
| `starfish_file_size` | Integer | Starfish integration field | Optional |
| `starfish_gid` | Integer | Starfish integration field | Optional |
| `starfish_full_path` | String | Starfish integration field | Optional |
| `starfish_volume` | String | Starfish integration field | Optional |
| `starfish_uid` | Integer | Starfish integration field | Optional |
| `starfish_document_valid_from` | Float | Starfish integration field | Optional |
| `starfish_object_id` | Integer | Starfish integration field | Optional |

### Chunk Table (`chunk`)
| Field Name | Type | Description | Notes |
|------------|------|-------------|-------|
| `chunk_id` | UUID (PK) | Unique chunk identifier | Primary key, auto-generated |
| `document_grsar_id` | String (FK) | Reference to parent document | Foreign key to document table |
| `workflow_id` | String (FK) | Workflow identifier | Foreign key to workflow table |
| `chunk_sequence` | Integer | Order of chunk in document | Not nullable |
| `chunk_type` | String | Type of chunk (e.g., "article_chunk") | Not nullable |
| `vector_field_name` | String | Vector field name ('vector' or 'smiles_vector') | Not nullable |
| `chunk_annotations_count` | Integer | Count of annotations in chunk | |
| `source` | String | Data source | Not nullable |
| `chunk_creation_dt` | TIMESTAMP | Chunk creation timestamp | Not nullable |
| `chunk_creation_ds` | TIMESTAMP | Chunk creation date | Not nullable |

---

## OpenSearch Schema (Chunk Payload)

### Core Chunk Fields
| Field Name | Type | Description | PostgreSQL Equivalent |
|------------|------|-------------|----------------------|
| `chunk_id` | String | Unique chunk identifier | ✅ `chunk.chunk_id` |
| `chunk_name` | String | Name of the chunk | ❌ Not in PostgreSQL |
| `chunk_text` | String | Raw chunk text content | ❌ Not in PostgreSQL |
| `chunk_length` | Integer | Length of chunk text | ❌ Not in PostgreSQL |
| `token_count` | Integer | Number of tokens in chunk | ❌ Not in PostgreSQL |
| `chunk_annotations_count` | Integer | Count of annotations | ✅ `chunk.chunk_annotations_count` |
| `chunk_processing_date` | String (ISO date) | Date chunk was processed | ❌ Not in PostgreSQL |
| `processing_ts` | String (ISO datetime) | Processing timestamp | Similar to `chunk.chunk_creation_dt` |
| `chunk_type` | String | Type of chunk | ✅ `chunk.chunk_type` |
| `merged_text` | String | Text used for embedding generation | ❌ Not in PostgreSQL |
| `article_summary` | String | Article summary (if available) | ❌ Not in PostgreSQL |
| `slide_ids` | Array | Slide IDs (for PPTX files) | ❌ Not in PostgreSQL |
| `section_title` | String | Section title (for DOCX files) | ❌ Not in PostgreSQL |

### Annotation Fields (Biomedical Entities)
| Field Name | Type | Description | PostgreSQL Equivalent |
|------------|------|-------------|----------------------|
| `genes` | Array[String] | List of gene names found in chunk | ❌ Not in PostgreSQL |
| `species` | Array[String] | List of species names | ❌ Not in PostgreSQL |
| `strains` | Array[String] | List of strain names | ❌ Not in PostgreSQL |
| `genus` | Array[String] | List of genus names | ❌ Not in PostgreSQL |
| `cell_lines` | Array[String] | List of cell line names | ❌ Not in PostgreSQL |
| `diseases` | Array[String] | List of disease names | ❌ Not in PostgreSQL |
| `chemicals` | Array[String] | List of chemical names | ❌ Not in PostgreSQL |
| `variants` | Array[String] | List of variant names | ❌ Not in PostgreSQL |
| `keywords` | Array[String] | Extracted keywords | ❌ Not in PostgreSQL |
| `gene_ids` | Array[String] | NCBI Gene IDs | ❌ Not in PostgreSQL |
| `species_ids` | Array[String] | NCBI Taxonomy IDs | ❌ Not in PostgreSQL |
| `strain_ids` | Array[String] | Strain identifiers | ❌ Not in PostgreSQL |
| `genus_ids` | Array[String] | Genus identifiers | ❌ Not in PostgreSQL |
| `cell_line_ids` | Array[String] | Cell line identifiers | ❌ Not in PostgreSQL |
| `disease_ids` | Array[String] | Disease identifiers | ❌ Not in PostgreSQL |
| `chemical_ids` | Array[String] | Chemical identifiers | ❌ Not in PostgreSQL |
| `variant_ids` | Array[String] | Variant identifiers | ❌ Not in PostgreSQL |

### Document Reference Fields
| Field Name | Type | Description | PostgreSQL Equivalent |
|------------|------|-------------|----------------------|
| `article_id` | String | Article/document identifier | Similar to `document.document_grsar_id` |
| `source` | String | Data source | ✅ `chunk.source` |
| `workflow_id` | String | Workflow identifier | ✅ `chunk.workflow_id` |

### Source-Specific Article Metadata
*All fields from the article metadata JSON are merged into the OpenSearch payload (varies by source)*

#### PMC/PubMed Metadata
| Field Name | Type | Description | PostgreSQL Equivalent |
|------------|------|-------------|----------------------|
| `pmid` | String | PubMed ID | ❌ Not in PostgreSQL |
| `pmcid` | String | PubMed Central ID | ❌ Not in PostgreSQL |
| `doi` | String | Digital Object Identifier | ❌ Not in PostgreSQL |
| `publisher-id` | String | Publisher identifier | ❌ Not in PostgreSQL |
| `title` | String | Article title | ❌ Not in PostgreSQL |
| `article_type` | String | Type of article | ❌ Not in PostgreSQL |
| `keywords_from_source` | String | Keywords from source | ❌ Not in PostgreSQL |
| `authors` | Array[String] | List of author names | ❌ Not in PostgreSQL |
| `journal` | String | Journal name | ❌ Not in PostgreSQL |
| `publication_date` | Object | Publication date (day, month, year) | ❌ Not in PostgreSQL |
| `license` | String | Article license | ❌ Not in PostgreSQL |
| `references_count` | Integer | Number of references | ❌ Not in PostgreSQL |
| `references` | Array | List of references | ❌ Not in PostgreSQL |
| `competing_interests` | String | Competing interests statement | ❌ Not in PostgreSQL |

#### Clinical Trials Metadata
| Field Name | Type | Description | PostgreSQL Equivalent |
|------------|------|-------------|----------------------|
| `nct_id` | String | Clinical trial identifier | ❌ Not in PostgreSQL |
| `title` | String | Study title | ❌ Not in PostgreSQL |
| `study_url` | String | Study URL | ❌ Not in PostgreSQL |
| `acronym` | String | Study acronym | ❌ Not in PostgreSQL |
| `study_status` | String | Current study status | ❌ Not in PostgreSQL |
| `study_results` | String | Study results availability | ❌ Not in PostgreSQL |
| `conditions` | Array[String] | Medical conditions studied | ❌ Not in PostgreSQL |
| `interventions` | Array[String] | Interventions tested | ❌ Not in PostgreSQL |
| `sponsor` | String | Study sponsor | ❌ Not in PostgreSQL |
| `collaborators` | Array[String] | Collaborating organizations | ❌ Not in PostgreSQL |
| `sex` | String | Gender eligibility | ❌ Not in PostgreSQL |
| `age` | String | Age eligibility | ❌ Not in PostgreSQL |
| `phases` | Array[String] | Clinical trial phases | ❌ Not in PostgreSQL |
| `enrollment` | Integer | Enrollment number | ❌ Not in PostgreSQL |
| `funder_type` | String | Funder type | ❌ Not in PostgreSQL |
| `study_type` | String | Study type | ❌ Not in PostgreSQL |
| `study_design` | Array[String] | Study design | ❌ Not in PostgreSQL |
| `other_ids` | Array[String] | Other identifiers | ❌ Not in PostgreSQL |
| `start_date` | Object | Study start date | ❌ Not in PostgreSQL |
| `primary_completion_date` | Object | Primary completion date | ❌ Not in PostgreSQL |
| `completion_date` | Object | Completion date | ❌ Not in PostgreSQL |
| `first_posted` | Object | First posted date | ❌ Not in PostgreSQL |
| `results_first_posted` | Object | Results first posted date | ❌ Not in PostgreSQL |
| `last_update_posted` | Object | Last update posted date | ❌ Not in PostgreSQL |
| `locations` | Array[String] | Study locations | ❌ Not in PostgreSQL |
| `study_documents` | Array[String] | Study documents | ❌ Not in PostgreSQL |

#### Apollo Metadata
| Field Name | Type | Description | PostgreSQL Equivalent |
|------------|------|-------------|----------------------|
| `article_id` | String | Article UUID | Similar to `document.document_grsar_id` |
| `original_filename` | String | Original filename | Similar to `document.document_name` |
| `safe_fileName` | String | Safe filename | Similar to `document.document_grsar_name` |
| `title` | String | Full path as title | ❌ Not in PostgreSQL |
| `extension` | String | File extension | Similar to `document.document_type` |
| `full_path` | String | Full file path | Similar to `document.source_path` |
| `team` | String | Project/team name | ❌ Not in PostgreSQL |
| `sub_team` | String | Functional area | ❌ Not in PostgreSQL |
| `sub_context` | String | Sub-context | ❌ Not in PostgreSQL |
| `experiment_id_all` | Array[String] | Experiment IDs | ❌ Not in PostgreSQL |
| `compound_id_all` | Array[String] | Compound IDs | ❌ Not in PostgreSQL |
| `species_extracted` | String | Extracted species | ❌ Not in PostgreSQL |
| `assay_protocol` | String | Assay protocol | ❌ Not in PostgreSQL |
| `assay_protocol_all` | Array[String] | All assay protocols | ❌ Not in PostgreSQL |
| `meeting_report_type` | String | Meeting report type | ❌ Not in PostgreSQL |
| `meeting_report_subtype` | String | Meeting report subtype | ❌ Not in PostgreSQL |
| `meeting_report_type_all` | Array[String] | All meeting report types | ❌ Not in PostgreSQL |
| `author_owner` | String | Author/owner | ❌ Not in PostgreSQL |
| `created_date` | String | Created date | Similar to `document.created_dt` |
| `primary_date_is_year_only` | Boolean | Year-only flag | ❌ Not in PostgreSQL |
| `primary_date_from_month_year` | Boolean | Month-year flag | ❌ Not in PostgreSQL |
| `year` | String | Year extracted | ❌ Not in PostgreSQL |
| `file_category` | String | File category | ❌ Not in PostgreSQL |
| `article_type` | String | Article type | ❌ Not in PostgreSQL |
| `is_temp_file` | Boolean | Temporary file flag | ❌ Not in PostgreSQL |

### Vector Embeddings
| Field Name | Type | Description | PostgreSQL Equivalent |
|------------|------|-------------|----------------------|
| `embeddings` | Array[Float] | Vector embeddings for similarity search | ❌ Not in PostgreSQL (only field name stored) |

---

## Key Differences Summary

### PostgreSQL
- **Purpose**: Relational tracking and referential integrity
- **Structure**: Normalized relational schema with foreign keys
- **Content**: Minimal metadata for document/chunk management
- **Fields**: ~9 core document fields + ~9 core chunk fields + optional Starfish fields
- **No content**: Does not store chunk text, annotations, or article metadata
- **No embeddings**: Only stores vector field name, not actual embeddings

### OpenSearch
- **Purpose**: Searchable metadata for retrieval and filtering
- **Structure**: Denormalized document payload with nested/array fields
- **Content**: Rich metadata including full article details, annotations, and embeddings
- **Fields**: 20+ core chunk fields + 15+ annotation fields + source-specific article metadata (varies by source)
- **Full content**: Stores chunk text, merged text, and all article metadata
- **Embeddings**: Stores actual vector embeddings for similarity search

---

## Legend
- ✅ = Field exists in PostgreSQL
- ❌ = Field does NOT exist in PostgreSQL
- Similar = Related field exists but may differ in format/structure
