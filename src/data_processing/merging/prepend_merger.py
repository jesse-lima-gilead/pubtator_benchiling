class PrependMerger:
    def merge(self, text: str, annotations: list) -> str:
        """
        Merges annotations into the text by appending them at the end of the text.

        Args:
        - chunk: a dictionary containing 'text' and 'annotations'

        Returns:
        - Merged text with annotations appended at the end.
        """

        # Start with the original text
        merged_text = ""

        # Append the annotations section
        if annotations:
            # Deduplicating annotations using a set to track unique combinations
            unique_entries = set()
            distinct_annotations = []

            for ann in annotations:
                unique_key = (
                    ann["text"],
                    ann["type"],
                    ann["ncbi_label"],
                    ann["ncbi_id"],
                )
                if unique_key not in unique_entries:
                    unique_entries.add(unique_key)
                    distinct_annotations.append(ann)

            merged_text += "Annotations:\n"
            for ann in distinct_annotations:
                # Extract common fields
                annotation_text = ann["text"]
                annotation_type = ann["type"]
                annotation_label = ann["ncbi_label"]
                annotation_id = ann["ncbi_id"]
                # annotation_offset = ann["offset"]
                # annotation_length = ann["length"]

                # Format the annotation information to be appended
                annotation_block = (
                    f"Text - {annotation_text}"
                    f"\n"
                    f"Type - {annotation_type}"
                    f"\n"
                    f"{annotation_label} - {annotation_id}"
                    # f"\n"
                    # f"Text Offset - {annotation_offset}"
                    # f"\n"
                    # f"Text Length - {annotation_length}"
                )
                merged_text += f"{annotation_block}\n"

        merged_text += f"Chunk Text:\n{text}"

        return merged_text


# Example usage

chunk = [
    {
        "chunk_sequence": "1",
        "merged_text": "      Introduction      Lung cancer is the most commonly diagnosed cancer worldwide and the leading cause of cancer mortality1. Although tobacco smoking remains the predominant risk factor for lung cancer, clinical observations and epidemiological studies have consistently shown that individuals with airflow limitation, particularly those with chronic obstructive pulmonary disease (COPD), have a significantly higher risk of developing lung cancer2–7. Several lines of evidence suggest that biological processes resulting in pulmonary impairment warrant consideration as independent lung cancer risk factors, including observations that previous lung diseases influence lung cancer risk independently of tobacco use6,8–10, and overlap in genetic susceptibility loci for lung cancer and chronic obstructive pulmonary disease (COPD) on 4q24 (FAM13A), 4q31 (HHIP), 5q.32 (HTR4), the 6p21 region, and 15q25 (CHRNA3/CHRNA5)11–14. Inflammation and oxidative stress have been proposed as key mechanisms promoting lung carcinogenesis in individuals affected by COPD or other non-neoplastic lung pathologies9,11,15.      Despite an accumulation of observational findings, previous epidemiological studies have been unable to conclusively establish a causal link between indicators of impaired pulmonary function and lung cancer risk due to the interrelated nature of these conditions7. Lung cancer and obstructive pulmonary disease share multiple etiological factors, such as cigarette smoking, occupational inhalation hazards, and air pollution, and 50–70% of lung cancer patients present with co-existing COPD or airflow obstruction6. Furthermore, reverse causality remains a concern since pulmonary symptoms may be early manifestations of lung cancer or acquired lung diseases in patients whose immune system has already been compromised by undiagnosed cancer.      Disentangling the role of pulmonary impairment in lung cancer development is important from an etiological perspective, for refining disease susceptibility mechanisms, and for informing precision prevention and risk stratification strategies. In this study we comprehensively assess the shared genetic basis of impaired lung function and lung cancer risk by conducting genome-wide association analyses in the UK Biobank cohort to identify genetic determinants of three pulmonary phenotypes, forced expiratory volume in 1s (FEV1), forced vital capacity (FVC), and FEV1/FVC. We examine the genetic correlation between pulmonary function phenotypes and lung cancer, followed by Mendelian randomization (MR) using novel genetic instruments to formally test the causal relevance of impaired pulmonary function, using the largest available dataset of 29,266 lung cancer cases and 56,450 controls from the OncoArray lung cancer collaboration16.    \n\nAnnotations:\nText - tobacco\nType - Species\nNCBI Taxonomy - 4097\nText Offset - 137\nText Length - 7\n\nText - tobacco\nType - Species\nNCBI Taxonomy - 4097\nText Offset - 707\nText Length - 7\n\nText - patients\nType - Species\nNCBI Taxonomy - 9606\nText Offset - 1567\nText Length - 8\n\nText - patients\nType - Species\nNCBI Taxonomy - 9606\nText Offset - 1777\nText Length - 8\n\n",
        "chunk_text": "      Introduction      Lung cancer is the most commonly diagnosed cancer worldwide and the leading cause of cancer mortality1. Although tobacco smoking remains the predominant risk factor for lung cancer, clinical observations and epidemiological studies have consistently shown that individuals with airflow limitation, particularly those with chronic obstructive pulmonary disease (COPD), have a significantly higher risk of developing lung cancer2–7. Several lines of evidence suggest that biological processes resulting in pulmonary impairment warrant consideration as independent lung cancer risk factors, including observations that previous lung diseases influence lung cancer risk independently of tobacco use6,8–10, and overlap in genetic susceptibility loci for lung cancer and chronic obstructive pulmonary disease (COPD) on 4q24 (FAM13A), 4q31 (HHIP), 5q.32 (HTR4), the 6p21 region, and 15q25 (CHRNA3/CHRNA5)11–14. Inflammation and oxidative stress have been proposed as key mechanisms promoting lung carcinogenesis in individuals affected by COPD or other non-neoplastic lung pathologies9,11,15.      Despite an accumulation of observational findings, previous epidemiological studies have been unable to conclusively establish a causal link between indicators of impaired pulmonary function and lung cancer risk due to the interrelated nature of these conditions7. Lung cancer and obstructive pulmonary disease share multiple etiological factors, such as cigarette smoking, occupational inhalation hazards, and air pollution, and 50–70% of lung cancer patients present with co-existing COPD or airflow obstruction6. Furthermore, reverse causality remains a concern since pulmonary symptoms may be early manifestations of lung cancer or acquired lung diseases in patients whose immune system has already been compromised by undiagnosed cancer.      Disentangling the role of pulmonary impairment in lung cancer development is important from an etiological perspective, for refining disease susceptibility mechanisms, and for informing precision prevention and risk stratification strategies. In this study we comprehensively assess the shared genetic basis of impaired lung function and lung cancer risk by conducting genome-wide association analyses in the UK Biobank cohort to identify genetic determinants of three pulmonary phenotypes, forced expiratory volume in 1s (FEV1), forced vital capacity (FVC), and FEV1/FVC. We examine the genetic correlation between pulmonary function phenotypes and lung cancer, followed by Mendelian randomization (MR) using novel genetic instruments to formally test the causal relevance of impaired pulmonary function, using the largest available dataset of 29,266 lung cancer cases and 56,450 controls from the OncoArray lung cancer collaboration16.    ",
        "chunk_annotations": [
            {
                "id": "0",
                "text": "tobacco",
                "type": "Species",
                "ncbi_label": "NCBI Taxonomy",
                "ncbi_id": "4097",
                "offset": 137,
                "length": 7,
            },
            {
                "id": "1",
                "text": "tobacco",
                "type": "Species",
                "ncbi_label": "NCBI Taxonomy",
                "ncbi_id": "4097",
                "offset": 707,
                "length": 7,
            },
            {
                "id": "2",
                "text": "patients",
                "type": "Species",
                "ncbi_label": "NCBI Taxonomy",
                "ncbi_id": "9606",
                "offset": 1567,
                "length": 8,
            },
            {
                "id": "3",
                "text": "patients",
                "type": "Species",
                "ncbi_label": "NCBI Taxonomy",
                "ncbi_id": "9606",
                "offset": 1777,
                "length": 8,
            },
        ],
        "payload": {
            "chunk_id": "8306be57-e1e6-4120-96eb-4c384062c1dc",
            "chunk_name": "PMC_6946810_chunk_1",
            "chunk_length": 2804,
            "token_count": 372,
            "chunk_annotations_count": 4,
            "chunk_annotations_ids": ["0", "1", "2", "3"],
            "chunk_annotations_types": ["Species"],
            "chunk_offset": 0,
            "chunk_infons": {"type": "Introduction"},
            "chunker_type": "annotation_aware",
            "merger_type": "append",
            "aioner_model": "bioformer",
            "gnorm2_model": "bioformer",
            "article_id": "PMC_6946810",
        },
    },
    {
        "chunk_sequence": "2",
        "merged_text": "      Introduction      Lung cancer is the most commonly diagnosed cancer worldwide and the leading cause of cancer mortality1. Although tobacco smoking remains the predominant risk factor for lung cancer, clinical observations and epidemiological studies have consistently shown that individuals with airflow limitation, particularly those with chronic obstructive pulmonary disease (COPD), have a significantly higher risk of developing lung cancer2–7. Several lines of evidence suggest that biological processes resulting in pulmonary impairment warrant consideration as independent lung cancer risk factors, including observations that previous lung diseases influence lung cancer risk independently of tobacco use6,8–10, and overlap in genetic susceptibility loci for lung cancer and chronic obstructive pulmonary disease (COPD) on 4q24 (FAM13A), 4q31 (HHIP), 5q.32 (HTR4), the 6p21 region, and 15q25 (CHRNA3/CHRNA5)11–14. Inflammation and oxidative stress have been proposed as key mechanisms promoting lung carcinogenesis in individuals affected by COPD or other non-neoplastic lung pathologies9,11,15.      Despite an accumulation of observational findings, previous epidemiological studies have been unable to conclusively establish a causal link between indicators of impaired pulmonary function and lung cancer risk due to the interrelated nature of these conditions7. Lung cancer and obstructive pulmonary disease share multiple etiological factors, such as cigarette smoking, occupational inhalation hazards, and air pollution, and 50–70% of lung cancer patients present with co-existing COPD or airflow obstruction6. Furthermore, reverse causality remains a concern since pulmonary symptoms may be early manifestations of lung cancer or acquired lung diseases in patients whose immune system has already been compromised by undiagnosed cancer.      Disentangling the role of pulmonary impairment in lung cancer development is important from an etiological perspective, for refining disease susceptibility mechanisms, and for informing precision prevention and risk stratification strategies. In this study we comprehensively assess the shared genetic basis of impaired lung function and lung cancer risk by conducting genome-wide association analyses in the UK Biobank cohort to identify genetic determinants of three pulmonary phenotypes, forced expiratory volume in 1s (FEV1), forced vital capacity (FVC), and FEV1/FVC. We examine the genetic correlation between pulmonary function phenotypes and lung cancer, followed by Mendelian randomization (MR) using novel genetic instruments to formally test the causal relevance of impaired pulmonary function, using the largest available dataset of 29,266 lung cancer cases and 56,450 controls from the OncoArray lung cancer collaboration16.    \n\nAnnotations:\nText - FAM13A\nType - Gene\nNCBI Gene - 10144\nText Offset - 843\nText Length - 6\n\nText - HHIP\nType - Gene\nNCBI Gene - 64399\nText Offset - 858\nText Length - 4\n\nText - HTR4\nType - Gene\nNCBI Gene - 3360\nText Offset - 872\nText Length - 4\n\nText - CHRNA3\nType - Gene\nNCBI Gene - 1136\nText Offset - 907\nText Length - 6\n\nText - CHRNA5\nType - Gene\nNCBI Gene - 1138\nText Offset - 914\nText Length - 6\n\n",
        "chunk_text": "      Introduction      Lung cancer is the most commonly diagnosed cancer worldwide and the leading cause of cancer mortality1. Although tobacco smoking remains the predominant risk factor for lung cancer, clinical observations and epidemiological studies have consistently shown that individuals with airflow limitation, particularly those with chronic obstructive pulmonary disease (COPD), have a significantly higher risk of developing lung cancer2–7. Several lines of evidence suggest that biological processes resulting in pulmonary impairment warrant consideration as independent lung cancer risk factors, including observations that previous lung diseases influence lung cancer risk independently of tobacco use6,8–10, and overlap in genetic susceptibility loci for lung cancer and chronic obstructive pulmonary disease (COPD) on 4q24 (FAM13A), 4q31 (HHIP), 5q.32 (HTR4), the 6p21 region, and 15q25 (CHRNA3/CHRNA5)11–14. Inflammation and oxidative stress have been proposed as key mechanisms promoting lung carcinogenesis in individuals affected by COPD or other non-neoplastic lung pathologies9,11,15.      Despite an accumulation of observational findings, previous epidemiological studies have been unable to conclusively establish a causal link between indicators of impaired pulmonary function and lung cancer risk due to the interrelated nature of these conditions7. Lung cancer and obstructive pulmonary disease share multiple etiological factors, such as cigarette smoking, occupational inhalation hazards, and air pollution, and 50–70% of lung cancer patients present with co-existing COPD or airflow obstruction6. Furthermore, reverse causality remains a concern since pulmonary symptoms may be early manifestations of lung cancer or acquired lung diseases in patients whose immune system has already been compromised by undiagnosed cancer.      Disentangling the role of pulmonary impairment in lung cancer development is important from an etiological perspective, for refining disease susceptibility mechanisms, and for informing precision prevention and risk stratification strategies. In this study we comprehensively assess the shared genetic basis of impaired lung function and lung cancer risk by conducting genome-wide association analyses in the UK Biobank cohort to identify genetic determinants of three pulmonary phenotypes, forced expiratory volume in 1s (FEV1), forced vital capacity (FVC), and FEV1/FVC. We examine the genetic correlation between pulmonary function phenotypes and lung cancer, followed by Mendelian randomization (MR) using novel genetic instruments to formally test the causal relevance of impaired pulmonary function, using the largest available dataset of 29,266 lung cancer cases and 56,450 controls from the OncoArray lung cancer collaboration16.    ",
        "chunk_annotations": [
            {
                "id": "4",
                "text": "FAM13A",
                "type": "Gene",
                "ncbi_label": "NCBI Gene",
                "ncbi_id": "10144",
                "offset": 843,
                "length": 6,
            },
            {
                "id": "5",
                "text": "HHIP",
                "type": "Gene",
                "ncbi_label": "NCBI Gene",
                "ncbi_id": "64399",
                "offset": 858,
                "length": 4,
            },
            {
                "id": "6",
                "text": "HTR4",
                "type": "Gene",
                "ncbi_label": "NCBI Gene",
                "ncbi_id": "3360",
                "offset": 872,
                "length": 4,
            },
            {
                "id": "7",
                "text": "CHRNA3",
                "type": "Gene",
                "ncbi_label": "NCBI Gene",
                "ncbi_id": "1136",
                "offset": 907,
                "length": 6,
            },
            {
                "id": "8",
                "text": "CHRNA5",
                "type": "Gene",
                "ncbi_label": "NCBI Gene",
                "ncbi_id": "1138",
                "offset": 914,
                "length": 6,
            },
            {
                "id": "9",
                "text": "patients",
                "type": "Species",
                "ncbi_label": "NCBI Taxonomy",
                "ncbi_id": "9606",
                "offset": 1777,
                "length": 8,
            },
        ],
        "payload": {
            "chunk_id": "9506dbfd-0cad-4e42-bc06-389a12f9e4c8",
            "chunk_name": "PMC_6946810_chunk_2",
            "chunk_length": 2804,
            "token_count": 372,
            "chunk_annotations_count": 5,
            "chunk_annotations_ids": ["4", "5", "6", "7", "8"],
            "chunk_annotations_types": ["Gene"],
            "chunk_offset": 0,
            "chunk_infons": {"type": "Introduction"},
            "chunker_type": "annotation_aware",
            "merger_type": "append",
            "aioner_model": "bioformer",
            "gnorm2_model": "bioformer",
            "article_id": "PMC_6946810",
        },
    },
]


# if __name__ == "__main__":
#     # Instantiate the merger and merge the annotations into the text
#     merger = PrependMerger()
#     text = chunk[1]["chunk_text"]
#     annotations = chunk[1]["chunk_annotations"]
#     merged_text = merger.merge(text, annotations)
#     print(merged_text)
