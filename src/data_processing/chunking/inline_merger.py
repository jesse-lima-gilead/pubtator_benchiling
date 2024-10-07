class InlineMerger:
    def merge(self, text: str, annotations: list) -> str:
        """
        Merges annotations inline with the text based on their offset and length.

        Args:
        - chunk: a dictionary containing 'text' and 'annotations'

        Returns:
        - Merged text with annotations placed inline.
        """

        # Sort annotations by the offset to avoid disrupting other offsets
        annotations_sorted = sorted(annotations, key=lambda ann: ann["offset"])

        # Offset adjustment to account for the increasing length of the inserted annotations
        offset_adjustment = 0

        # Track already annotated words in this chunk to avoid duplicates
        annotated_offsets = set()

        for ann in annotations_sorted:
            # Extract annotation details
            annotation_text = ann["text"]
            annotation_type = ann["type"]
            annotation_offset = ann["offset"] + offset_adjustment
            annotation_length = ann["length"]

            # Skip if the annotation overlaps with an already processed one
            if annotation_offset in annotated_offsets:
                continue

            # Add annotation to processed list
            annotated_offsets.update(
                range(annotation_offset, annotation_offset + annotation_length)
            )

            # Determine whether it's a Gene, Species, or other
            if annotation_type == "Gene":
                ncbi_id = ann["infons"].get("NCBI Gene", "N/A")
                annotation_label = f"(Gene NCBI Gene - {ncbi_id})"
            elif annotation_type == "Species":
                ncbi_id = ann["infons"].get("NCBI Taxonomy", "N/A")
                annotation_label = f"(Species NCBI Taxonomy - {ncbi_id})"
            else:
                annotation_label = f"({annotation_type} NCBI ID - N/A)"

            # Insert the annotation inline into the text
            annotated_text = f"{annotation_text} {annotation_label}"
            text = (
                text[:annotation_offset]
                + annotated_text
                + text[annotation_offset + annotation_length :]
            )

            # Update the offset adjustment to account for the added length
            offset_adjustment += len(annotated_text) - annotation_length

        return text


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
    merger = InlineMerger()
    text = chunk["chunk_text"]
    annotations = chunk["chunk_annotations"]
    merged_text = merger.merge(text, annotations)
    print(merged_text)
