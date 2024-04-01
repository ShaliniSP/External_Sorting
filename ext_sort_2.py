import csv
import os
import sys


def sort_and_write_chunk(chunk, run_id):
    """
    Sorts a chunk of data in-memory by the first column and writes it to a temporary file.

    :param chunk: A list of data rows to be sorted.
    :param run_id: Identifier for the run, used for naming the temporary file.
    """
    chunk.sort(key=lambda x: x[0])  # Sort the chunk by the first column
    temp_file_name = f'temp_run_{run_id}.csv'
    with open(temp_file_name, 'w', newline='') as temp_file:
        writer = csv.writer(temp_file)
        writer.writerows(chunk)


def merge_runs(run_files, output_filename):
    """
    Merges sorted files (runs) into a single sorted output file.

    :param run_files: List of filenames representing sorted runs to be merged.
    :param output_filename: Filename for the merged, sorted output.
    """
    # Open file handlers for each run file
    run_handlers = [open(file, 'r') for file in run_files]
    # Open output file handler
    output_file = open(output_filename, 'w', newline='')
    output_writer = csv.writer(output_file)

    # Initialize list to hold current lines from each run
    run_lines = [next(csv.reader(run_file), None) for run_file in run_handlers]

    while any(run_lines):
        # Get the minimum line among the non-empty lines
        min_line = min((line for line in run_lines if line is not None), key=lambda x: x[0])
        output_writer.writerow(min_line)

        # Update the current line for the run from which the minimum line was obtained
        try:
            index = run_lines.index(min_line)
            run_lines[index] = next(csv.reader(run_handlers[index]), None)
        except StopIteration:
            # If end of file is reached, set line to None
            run_lines[index] = None
            # Close the file handler
            run_handlers[index].close()

    # Close all file handlers
    for run_file in run_handlers:
        run_file.close()
    output_file.close()


def external_sort(input_filename, output_filename):
    """
    The external sort process: chunking, sorting, and merging.

    :param input_filename: Name of the file with data to sort.
    :param output_filename: Name of the file where sorted data will be written.
    """
    # Step 1: Divide the input into chunks, sort each chunk, and write to temporary files
    with open(input_filename, 'r') as f:
        reader = csv.reader(f)
        temp_files = []
        chunk_size = 2  # Adjusted chunk size
        chunk_id = 0

        while True:
            chunk = []
            for _ in range(chunk_size):
                try:
                    chunk.append(next(reader))
                except StopIteration:
                    break
            if not chunk:
                break
            sort_and_write_chunk(chunk, chunk_id)
            temp_files.append(f'temp_run_{chunk_id}.csv')
            chunk_id += 1

    # Step 2: Merge sorted chunks until only one remains
    while len(temp_files) > 1:
        merge_pass_files = []

        # Merge runs in pairs
        for i in range(0, len(temp_files), 2):
            output_filename = f'merged_{i}_{i + 1}.csv' if i + 1 < len(temp_files) else f'merged_{i}.csv'
            merge_runs(temp_files[i:i + 2], output_filename)
            merge_pass_files.append(output_filename)

        # Replace temp_files with merge_pass_files
        temp_files = merge_pass_files

    # Rename the final merged file to output_filename
    os.rename(temp_files[0], output_filename)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 ext_sort.py input.csv output.csv")
    else:
        input_filename = sys.argv[1]
        output_filename = sys.argv[2]
        external_sort(input_filename, output_filename)
