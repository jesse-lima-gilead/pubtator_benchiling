class AppendMerger:
    def merge(self, text: str, annotations: list) -> str:
        """
        Merges annotations into the text by appending them at the end of the text.

        Args:
        - chunk: a dictionary containing 'text' and 'annotations'

        Returns:
        - Merged text with annotations appended at the end.
        """

        # Start with the original text
        merged_text = text.strip()

        # Append the annotations section
        if annotations:
            merged_text += "\n\nAnnotations:\n"
            for ann in annotations:
                # Extract common fields
                annotation_text = ann["text"]
                annotation_type = ann["type"]
                annotation_offset = ann["offset"]
                annotation_length = ann["length"]

                # Determine whether it's a Gene or Species and extract relevant NCBI ID
                if annotation_type == "Gene":
                    ncbi_id = ann["infons"].get("NCBI Gene", "N/A")
                    ncbi_label = "NCBI Gene"
                elif annotation_type == "Species":
                    ncbi_id = ann["infons"].get("NCBI Taxonomy", "N/A")
                    ncbi_label = "NCBI Taxonomy"
                else:
                    ncbi_id = "N/A"
                    ncbi_label = "NCBI ID"

                # Format the annotation information to be appended
                annotation_block = (
                    f"Text - {annotation_text}"
                    f"\n"
                    f"Type - {annotation_type}"
                    f"\n"
                    f"{ncbi_label} - {ncbi_id}"
                    f"\n"
                    f"Text Offset - {annotation_offset}"
                    f"\n"
                    f"Text Length - {annotation_length}"
                )
                merged_text += f"{annotation_block}\n\n"

        return merged_text.strip()


# Example usage

chunk = {
    "chunk_sequence": "1",
    "chunk_text": "Introduction      Lung cancer is the most commonly diagnosed cancer worldwide and the leading cause of cancer mortality1. Although tobacco smoking remains the predominant risk factor for lung cancer, clinical observations and epidemiological studies have consistently shown that individuals with airflow limitation, particularly those with chronic obstructive pulmonary disease (COPD), have a significantly higher risk of developing lung cancer2–7. Several lines of evidence suggest that biological processes resulting in pulmonary impairment warrant consideration as independent lung cancer risk factors, including observations that previous lung diseases influence lung cancer risk independently of tobacco use6,8–10, and overlap in genetic susceptibility loci for lung cancer and chronic obstructive pulmonary disease (COPD) on 4q24 (FAM13A), 4q31 (HHIP), 5q.32 (HTR4), the 6p21 region, and 15q25 (CHRNA3/CHRNA5)11–14. Inflammation and oxidative stress have been proposed as key mechanisms promoting lung carcinogenesis in individuals affected by COPD or other non-neoplastic lung pathologies9,11,15.      Despite an accumulation of observational findings, previous epidemiological studies have been unable to conclusively establish a causal link between indicators of impaired pulmonary function and lung cancer risk due to the interrelated nature of these conditions7. Lung cancer and obstructive pulmonary disease share multiple etiological factors, such as cigarette smoking, occupational inhalation hazards, and air pollution, and 50–70% of lung cancer patients present with co-existing COPD or airflow obstruction6. Furthermore, reverse causality remains a concern since pulmonary symptoms may be early manifestations of lung cancer or acquired lung diseases in patients whose immune system has already been compromised by undiagnosed cancer.      Disentangling the role of pulmonary impairment in lung cancer development is important from an etiological perspective, for refining disease susceptibility mechanisms, and for informing precision prevention and risk stratification strategies. In this study we comprehensively assess the shared genetic basis of impaired lung function and lung cancer risk by conducting genome-wide association analyses in the UK Biobank cohort to identify genetic determinants of three pulmonary phenotypes, forced expiratory volume in 1s (FEV1), forced vital capacity (FVC), and FEV1/FVC. We examine the genetic correlation between pulmonary function phenotypes and lung cancer, followed by Mendelian randomization (MR) using novel genetic instruments to formally test the causal relevance of impaired pulmonary function, using the largest available dataset of 29,266 lung cancer cases and 56,450 controls from the OncoArray lung cancer collaboration16.",
    "chunk_annotations": [
        {
            "id": "0",
            "text": "tobacco",
            "offset": 137,
            "length": 7,
            "type": "Species",
            "infons": {"NCBI Taxonomy": "4097", "type": "Species"},
        },
        {
            "id": "1",
            "text": "tobacco",
            "offset": 707,
            "length": 7,
            "type": "Species",
            "infons": {"NCBI Taxonomy": "4097", "type": "Species"},
        },
        {
            "id": "2",
            "text": "patients",
            "offset": 1567,
            "length": 8,
            "type": "Species",
            "infons": {"NCBI Taxonomy": "9606", "type": "Species"},
        },
        {
            "id": "3",
            "text": "patients",
            "offset": 1777,
            "length": 8,
            "type": "Species",
            "infons": {"NCBI Taxonomy": "9606", "type": "Species"},
        },
    ],
}

if __name__ == "__main__":
    # Instantiate the merger and merge the annotations into the text
    merger = AppendMerger()
    text = chunk["chunk_text"]
    annotations = chunk["chunk_annotations"]
    merged_text = merger.merge(text, annotations)
    print(merged_text)
