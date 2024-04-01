import csv
import os


def sort_and_write_chunk(chunk, run_id):
    """
    Sorts a chunk of data in-memory by the first column and writes it to a temporary file.

    :param chunk: A list of data rows to be sorted.
    :param run_id: Identifier for the run, used for naming the temporary file.
    """
    chunk.sort(key=lambda x: x[0])
    with open(f'temp_{run_id}.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        for row in chunk:
            writer.writerow(row)


def merge_runs(run_files, output_filename):
    """
    Merges sorted files (runs) into a single sorted output file based on the first column.

    :param run_files: List of filenames representing sorted runs to be merged.
    :param output_filename: Filename for the merged, sorted output.
    """
    if len(run_files) == 1:
        os.rename(run_files[0], output_filename)
        return

    run_id = len(run_files)

    while len(run_files) > 1:
        temp_files = []
        for i in range(0, len(run_files), 2):
            file_1 = run_files[i]
            file_2 = run_files[i + 1]

            with open(file_1, 'r') as f1, open(file_2, 'r') as f2, open(f'temp_{run_id}.csv', 'w', newline='') as f:
                reader_1 = csv.reader(f1)
                reader_2 = csv.reader(f2)
                writer = csv.writer(f)

                row_1 = next(reader_1, None)
                row_2 = next(reader_2, None)

                while row_1 and row_2:
                    if row_1[0] < row_2[0]:
                        writer.writerow(row_1)
                        row_1 = next(reader_1, None)
                    else:
                        writer.writerow(row_2)
                        row_2 = next(reader_2, None)

                while row_1:
                    writer.writerow(row_1)
                    row_1 = next(reader_1, None)

                while row_2:
                    writer.writerow(row_2)
                    row_2 = next(reader_2, None)

            os.remove(file_1)
            os.remove(file_2)

            temp_files.append(f'temp_{run_id}.csv')
            run_id += 1

        run_files = temp_files[:]
    if os.path.exists(output_filename):
        os.remove(output_filename)
    os.rename(run_files[0], output_filename)


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
                temp_files.append(f'temp_{run_id}.csv')
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
