import pandas as pd


def run():
    # Load the data from CSV files
    results_without_annotations = pd.read_csv(
        "../../../data/results/baseline_results.csv", header=0
    )
    results_with_annotations = pd.read_csv(
        "../../../data/results/processed_results.csv", header=0
    )

    # Merge data on User Query and Article ID
    merged_data = pd.merge(
        results_with_annotations,
        results_without_annotations,
        on=["User Query", "Article ID"],
        suffixes=("_WithAnnotations", "_WithoutAnnotations"),
        how="outer",  # Ensures only matching rows are included
    )

    # Save the merged data to a new CSV file
    result_file_path = "../../../data/results/comparison_results.csv"
    merged_data.to_csv(result_file_path, index=False)

    print("Merged data has been saved to 'merged_output.csv'")


if __name__ == "__main__":
    run()
