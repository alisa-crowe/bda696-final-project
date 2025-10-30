import os
import pandas as pd

base_dir = os.path.dirname(os.path.abspath(__file__))
output_file = os.path.join(base_dir, "null_report.txt")

# Open the output file for writing
with open(output_file, "w") as report:
    for file in os.listdir(base_dir):
        if file.endswith(".csv"):
            file_path = os.path.join(base_dir, file)
            try:
                # Read the CSV file
                df = pd.read_csv(file_path)
                
                # Count total null values
                null_count = df.isnull().sum().sum()
                
                # Write result to file
                report.write(f"{file}: {null_count} null values\n")
            except Exception as e:
                report.write(f"Error reading {file}: {e}\n")

print(f"Null value report saved to: {output_file}")