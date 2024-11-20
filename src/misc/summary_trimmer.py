import os


def trim_summaries(folder_path: str):
    for file in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file)
        print(f"Processing {file_path}")
        if file_path.endswith(".txt"):
            with open(file_path, "r") as f:
                lines = f.readlines()
                if len(lines) > 1:
                    trimmed_summary = lines[-1]
                else:
                    trimmed_summary = lines[0]
            with open(file_path, "w") as f:
                f.write(trimmed_summary)


if __name__ == "__main__":
    summary_path = "../../data/articles_metadata/summary2"
    trim_summaries(summary_path)
