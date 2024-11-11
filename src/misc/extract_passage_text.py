import json

def extract_text(file_path):
    with open(file_path, "r") as file:
        data = json.load(file)
        passages = ""
        for chunk in data:
            passages += chunk["chunk_text"].strip() + "\n\n"
        return passages

if __name__ == "__main__":
    file_path = "../../test_data/chunks_11_oct/passage_prepend_bioformer_PMC_7614604.json"
    passages = extract_text(file_path)
    output_file = "../../test_data/PMC_7614604_chunks/PMC_7614604_passages.txt"
    with open(output_file, "w") as file:
        file.write(passages)
