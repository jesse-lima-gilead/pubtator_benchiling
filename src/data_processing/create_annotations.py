import os
import xml.etree.ElementTree as ET

import bioc
import stanza


class BioCNERAnnotator:
    def __init__(self, input_dir, output_dir):
        self.input_dir = input_dir
        self.output_dir = output_dir

        # Initialize Stanza NER models
        self.ner_models = {
            "linnaeus": stanza.Pipeline(
                "en", package="craft", processors={"ner": "linnaeus"}
            ),
            "bc5cdr": stanza.Pipeline(
                "en", package="craft", processors={"ner": "bc5cdr"}
            ),
            "ncbi_disease": stanza.Pipeline(
                "en", package="craft", processors={"ner": "ncbi_disease"}
            ),
            "bionlp13cg": stanza.Pipeline(
                "en", package="craft", processors={"ner": "bionlp13cg"}
            ),
            "jnlpba": stanza.Pipeline(
                "en", package="craft", processors={"ner": "jnlpba"}
            ),
        }

    def load_bioc_collection(self, xml_file):
        with open(xml_file, "r", encoding="utf-8") as f:
            bioc_data = f.read()
        return bioc.loads(bioc_data)

    def apply_ner(self, bioc_collection):
        # Process each document and passage in the BioC collection
        for document in bioc_collection.documents:
            for passage in document.passages:
                if passage.text:
                    text = passage.text

                    # Apply each NER model
                    for model_name, nlp in self.ner_models.items():
                        doc = nlp(text)

                        # Add annotations to BioCPassage
                        for ent in doc.ents:
                            annotation = bioc.BioCAnnotation()
                            annotation.id = (
                                f"{model_name}_{ent.start_char}_{ent.end_char}"
                            )
                            annotation.text = ent.text
                            annotation.add_location(
                                bioc.BioCLocation(ent.start_char, len(ent.text))
                            )
                            annotation.infons["type"] = ent.type
                            annotation.infons["model"] = model_name

                            # Add annotation to the passage
                            passage.annotations.append(annotation)

        return bioc_collection

    def save_annotated_bioc(self, bioc_collection, output_file):
        bioc_xml = bioc.dumps(bioc_collection, pretty_print=True)
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(bioc_xml)

    def process_files(self):
        # Ensure output directory exists
        os.makedirs(self.output_dir, exist_ok=True)

        # Process each BioC XML file in the input directory
        for file_name in os.listdir(self.input_dir):
            if file_name.endswith(".xml"):
                xml_file = os.path.join(self.input_dir, file_name)
                bioc_collection = self.load_bioc_collection(xml_file)
                annotated_collection = self.apply_ner(bioc_collection)

                # Save the annotated BioC XML file
                output_file = os.path.join(self.output_dir, file_name)
                self.save_annotated_bioc(annotated_collection, output_file)
                print(f"Processed and saved: {output_file}")


if __name__ == "__main__":
    print("Execution Started")
    # Usage Example
    input_dir = "../../data/bioc_xml"
    output_dir = "../../data/annotated_bioc_xml"
    annotator = BioCNERAnnotator(input_dir, output_dir)
    annotator.process_files()
    print("Execution Completed")
