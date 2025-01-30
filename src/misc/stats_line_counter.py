import os


def count_lines_in_file(file_path):
    """Count the number of lines in a file."""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return sum(1 for _ in f)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return 0  # If the file can't be read, count as 0


def count_lines_in_src(src_directory):
    """Recursively count lines in all files within src directory."""
    total_lines = 0
    file_line_counts = {}

    for root, _, files in os.walk(src_directory):
        for file in files:
            file_path = os.path.join(root, file)
            line_count = count_lines_in_file(file_path)
            file_line_counts[file_path] = line_count
            total_lines += line_count

    return total_lines, file_line_counts


if __name__ == "__main__":
    # Specify the path to your src directory
    src_directory_path = "../../src"

    # Get the total line count and individual file line counts
    total_lines, file_line_counts = count_lines_in_src(src_directory_path)

    # Print the total line count
    print(f"Total lines in all files: {total_lines}")

    # Print line counts for each file
    for file, lines in file_line_counts.items():
        print(f"{file}: {lines} lines")
