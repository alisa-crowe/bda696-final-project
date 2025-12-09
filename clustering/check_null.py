import os
import pandas as pd

# Get directory of this script
base_dir = os.path.dirname(os.path.abspath(__file__))
output_file = os.path.join(base_dir, "null_report_master.txt")

# Function to format section headers in the report
def write_header(report, title):
    report.write("\n" + "=" * 60 + "\n")
    report.write(f"{title}\n")
    report.write("=" * 60 + "\n\n")

# Open the output file for writing
with open(output_file, "w") as report:

    write_header(report, "NULL VALUE ANALYSIS REPORT")

    for file in os.listdir(base_dir):
        if file.lower().endswith(".csv"):   # only process CSVs

            file_path = os.path.join(base_dir, file)
            report.write(f"\nAnalyzing file: {file}\n")
            report.write("-" * 60 + "\n")

            try:
                # Read the CSV file
                df = pd.read_csv(file_path)

                # Overall null summary
                total_nulls = df.isnull().sum().sum()
                total_cells = df.shape[0] * df.shape[1]
                percent_null = (total_nulls / total_cells) * 100

                report.write(f"Total Rows: {df.shape[0]}\n")
                report.write(f"Total Columns: {df.shape[1]}\n")
                report.write(f"Total Null Values: {total_nulls}\n")
                report.write(f"Percentage Null: {percent_null:.2f}%\n\n")

                # Column-by-column null counts
                write_header(report, "Null Values by Column")

                null_by_column = df.isnull().sum()
                percent_by_column = (null_by_column / len(df)) * 100

                for col in df.columns:
                    report.write(
                        f"{col}: {null_by_column[col]} nulls "
                        f"({percent_by_column[col]:.2f}%)\n"
                    )

                # Identify rows with ANY nulls
                rows_with_nulls = df[df.isnull().any(axis=1)]
                report.write("\n")
                write_header(report, "Rows Containing Null Values")

                report.write(f"Total rows with at least one null: {len(rows_with_nulls)}\n")

                # Optionally print a small preview
                if len(rows_with_nulls) > 0:
                    report.write("\nExample rows with null values:\n")
                    report.write(rows_with_nulls.head().to_string())
                    report.write("\n")

            except Exception as e:
                report.write(f"Error reading {file}: {e}\n")

print(f"\nNull value report saved to: {output_file}")
