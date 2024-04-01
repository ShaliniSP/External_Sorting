import csv
import os


def sort_and_write_chunk(chunk, run_id):
    """
    Sorts a chunk of data in-memory by the first column and writes it to a temporary file.

    :param chunk: A list of data rows to be sorted.
    :param run_id: Identifier for the run, used for naming the temporary file.
    """
    chunk.sort(key=lambda x: x[0])
    with open(f'temp_{run_id}.csv', 'w') as f:
        writer = csv.writer(f)
        for row in chunk:
            writer.writerow(row)


def merge_runs(run_files, output_filename):
    """
    Merges sorted files (runs) into a single sorted output file.

    :param run_files: List of filenames representing sorted runs to be merged.
    :param output_filename: Filename for the merged, sorted output.
    """
    if len(run_files) == 1:
        os.rename(run_files[0], output_filename)
        return



def external_sort(input_filename, output_filename):
    """
    the external sort process: chunking, sorting, and merging.

    :param input_filename: Name of the file with data to sort.
    :param output_filename: Name of the file where sorted data will be written.
    """
    with open(input_filename, 'r') as f:
        reader = csv.reader(f)
        run_id = 0
        temp_files = []
        while True:
            chunk = []

            try:
                row1 = next(reader)
                chunk.append(row1)
            except StopIteration:
                break

            try:
                row2 = next(reader)
                chunk.append(row2)
            except StopIteration:
                break

            finally:
                sort_and_write_chunk(chunk, run_id)
                temp_files.append(f'temp_run_{run_id}.csv')
                run_id += 1

    merge_runs(temp_files, output_filename)

if __name__ == "__main__":
    import sys

    if len(sys.argv) != 3:
        print("Usage: python3 ext_sort.py input.csv output.csv")
    else:
        input_filename = sys.argv[1]
        output_filename = sys.argv[2]
        external_sort(input_filename, output_filename)
