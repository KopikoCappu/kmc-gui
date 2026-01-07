KMC Batch Processing - Command Line Version

Author: Minh Tran
Language: Python 3
Purpose: Simplify running KMC k-mer counting and analysis in batch mode on HPC systems or local machines. Ideal for testing datasets without manually searching for files.

Overview

This Python script provides an easy way to run KMC and KMC_tools on multiple FASTA files in batch mode. It is designed for high-performance computing (HPC) environments such as HiperGator, but works on any machine with Python 3 and the KMC binaries.

Key features:

Automatically detects FASTA files (*.fasta, *.fa, *.fna) in an input folder.

Processes files individually and outputs KMC databases.

Generates combined outputs:

overlap_merge: union of all k-mer databases.

combination_raw: concatenated dump files for easy inspection.

binary_existence: table of k-mer presence/absence across files.

Optional limit to process only the first N files for testing.

Works in interactive mode or with command-line arguments.

Cleans up temporary folders automatically.

Prerequisites

Python 3.6+

KMC and KMC_tools installed and accessible on your system

Input folder with FASTA files (*.fasta, *.fa, or *.fna)

For HPC environments, ensure your compute node has enough RAM and CPU threads for your datasets.

Installation

Clone this repository or download kmc_batch_cli.py.

Make sure KMC executables are installed:

/path/to/kmc
/path/to/kmc_tools


Install Python dependencies (if not already available):

pip install pathlib argparse

Usage
1. Interactive Mode

Simply run the script without arguments:

python3 kmc_batch_cli.py


The script will prompt for:

KMC executable path

KMC_tools executable path

Input folder (FASTA files)

Output folder

Working directory for temporary files

K-mer length (default: 21)

RAM in GB (default: 4)

Threads (default: 4)

Optional file limit (process only first N files)

2. Command-Line Arguments Mode
python3 kmc_batch_cli.py \
  --kmc /path/to/kmc \
  --kmc-tools /path/to/kmc_tools \
  --input /path/to/fasta_folder \
  --output /path/to/output_folder \
  --workdir /path/to/temp_folder \
  --k 21 \
  --ram 16 \
  --threads 8 \
  --limit 50


Arguments:

Argument	Description
--kmc	Path to KMC executable
--kmc-tools	Path to KMC_tools executable
--input	Input folder containing FASTA files
--output	Output folder for results
--workdir	Working directory for temporary files
--k	K-mer length (default: 21)
--ram	RAM in GB for KMC (default: 4)
--threads	Number of threads (default: 4)
--limit	Process only first N files (optional)
--interactive	Force interactive mode
Output Structure

The script creates the following output folders in your specified output directory:

output_folder/
├── overlap_merge/
│   └── overlap_merge_dump.txt      # union of all KMC databases
├── combination_raw/
│   └── combination_raw.txt         # concatenated dumps of all databases
├── binary_existence/
│   └── binary_existence.txt        # k-mer presence/absence across files


Temporary folders for individual files are automatically deleted after processing.

Example

Interactive setup:

KMC Batch Processing - Interactive Setup
Path to KMC executable: /home/user/kmc
Path to KMC_tools executable: /home/user/kmc_tools
Input folder containing FASTA files: /home/user/fasta_data
Output folder for results: /home/user/kmc_output
Working directory for temporary files: /home/user/kmc_temp
K-mer length [default: 21]:
RAM in GB [default: 4]:
Number of threads [default: 4]:
Process only first N files (leave empty for all):


Command-line setup:

python3 kmc_batch_cli.py \
  --kmc /home/user/kmc \
  --kmc-tools /home/user/kmc_tools \
  --input /home/user/fasta_data \
  --output /home/user/kmc_output \
  --workdir /home/user/kmc_temp \
  --k 21 \
  --ram 8 \
  --threads 4

Notes

For testing large datasets, you can use the --limit option to process a subset.

Ensure that your working directory has enough space for temporary KMC databases.

Designed for Linux-based HPC systems but fully compatible with Windows paths when using the command-line mode.

License

MIT License — free to use, modify, and redistribute.

Acknowledgements

This script was created to help researchers and developers quickly batch-process FASTA files using KMC, without manually managing files or outputs.
