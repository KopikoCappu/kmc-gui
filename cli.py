#!/usr/bin/env python3
"""
KMC Batch Processing - Command Line Version
Designed for HPC environments like HiperGator
"""

import subprocess
import argparse
from pathlib import Path
import sys

def log(message):
    """Print log message with flush for real-time output"""
    print(message, flush=True)

def run_kmc_batch(kmc_exe, kmc_tools_exe, input_folder, output_folder, work_dir, k, m, t, file_limit=None):
    """Run KMC batch processing"""
    
    import shutil
    
    # Get all FASTA files
    fasta_files = []
    for ext in ['*.fasta', '*.fa', '*.fna']:
        fasta_files.extend(Path(input_folder).glob(ext))
    
    if not fasta_files:
        log("ERROR: No FASTA files found in input folder!")
        return False
    
    # Sort files for consistent ordering
    fasta_files = sorted(fasta_files)
    
    total_files = len(fasta_files)
    
    # Apply file limit if specified
    if file_limit is not None and file_limit > 0:
        fasta_files = fasta_files[:file_limit]
        log(f"Found {total_files} FASTA files, processing first {len(fasta_files)} files")
    else:
        log(f"Found {len(fasta_files)} FASTA files to process")
    
    log("=" * 60)
    
    # Process each file
    processed_dbs = []
    errors = []
    individual_folders = []  # Track all individual folders for cleanup
    
    for i, fasta_file in enumerate(fasta_files, 1):
        file_name = fasta_file.stem
        log(f"\n[{i}/{len(fasta_files)}] Processing: {fasta_file.name}")
        
        # Create output subfolder
        file_output_dir = Path(output_folder) / file_name
        file_output_dir.mkdir(parents=True, exist_ok=True)
        individual_folders.append(file_output_dir)  # Track for cleanup
        
        output_db = str(file_output_dir / file_name)
        
        # Run KMC
        log(f"  Running KMC...")
        kmc_cmd = [
            kmc_exe,
            f"-k{k}", f"-m{m}", f"-t{t}",
            "-fa", str(fasta_file), output_db, work_dir
        ]
        
        try:
            result = subprocess.run(kmc_cmd, check=True, capture_output=True, text=True)
            log(f"  ✓ KMC completed")
            
            # Run kmc_tools dump
            log(f"  Running kmc_tools dump...")
            dump_file = str(file_output_dir / f"{file_name}_dump.txt")
            dump_cmd = [kmc_tools_exe, "transform", output_db, "dump", dump_file]
            
            subprocess.run(dump_cmd, check=True, capture_output=True, text=True)
            log(f"  ✓ Dump completed: {file_name}_dump.txt")
            
            processed_dbs.append(output_db)
            
        except subprocess.CalledProcessError as e:
            error_msg = f"  ✗ ERROR processing {fasta_file.name}: {str(e)}"
            if e.stderr:
                error_msg += f"\n    stderr: {e.stderr}"
            log(error_msg)
            errors.append(error_msg)
    
    # Create overlap_merge
    if processed_dbs:
        log("\n" + "=" * 60)
        log("Creating overlap_merge (union of all databases)...")
        
        overlap_dir = Path(output_folder) / "overlap_merge"
        overlap_dir.mkdir(parents=True, exist_ok=True)
        overlap_db = str(overlap_dir / "overlap_merge")
        
        try:
            # Build union command
            union_cmd = [kmc_tools_exe, "simple"] + processed_dbs
            for i in range(len(processed_dbs) - 1):
                union_cmd.append("union")
            union_cmd.append(overlap_db)
            
            subprocess.run(union_cmd, check=True, capture_output=True, text=True)
            log("  ✓ Union completed")
            
            # Dump merged database
            overlap_dump = str(overlap_dir / "overlap_merge_dump.txt")
            dump_cmd = [kmc_tools_exe, "transform", overlap_db, "dump", overlap_dump]
            subprocess.run(dump_cmd, check=True, capture_output=True, text=True)
            log(f"  ✓ Dump completed: overlap_merge_dump.txt")
            
        except subprocess.CalledProcessError as e:
            error_msg = f"  ✗ ERROR creating overlap_merge: {str(e)}"
            if e.stderr:
                error_msg += f"\n    stderr: {e.stderr}"
            log(error_msg)
            errors.append(error_msg)
    else:
        log("\n" + "=" * 60)
        log("WARNING: No databases were successfully processed, skipping overlap_merge")
    
    # Create combination_raw (concatenated dumps)
    if processed_dbs:
        log("\nCreating combination_raw (concatenated text dumps)...")
        combo_dir = Path(output_folder) / "combination_raw"
        combo_dir.mkdir(parents=True, exist_ok=True)
        combo_file = combo_dir / "combination_raw.txt"
        
        try:
            with open(combo_file, 'w') as outfile:
                for db_path in processed_dbs:
                    db_dir = Path(db_path).parent
                    dump_file = db_dir / f"{Path(db_path).name}_dump.txt"
                    if dump_file.exists():
                        with open(dump_file, 'r') as infile:
                            outfile.write(f"# === {dump_file.parent.name} ===\n")
                            outfile.write(infile.read())
                            outfile.write("\n")
            log(f"  ✓ Concatenation completed: combination_raw.txt")
        except Exception as e:
            error_msg = f"  ✗ ERROR creating combination_raw: {str(e)}"
            log(error_msg)
            errors.append(error_msg)
    else:
        log("\nWARNING: No databases were successfully processed, skipping combination_raw")
    
    # Create binary_existence (count files where each k-mer appears)
    if processed_dbs:
        log("\nCreating binary_existence (presence/absence across files)...")
        binary_dir = Path(output_folder) / "binary_existence"
        binary_dir.mkdir(parents=True, exist_ok=True)
        binary_file = binary_dir / "binary_existence.txt"
        
        try:
            kmer_file_count = {}  # k-mer -> count of files it appears in
            
            for db_path in processed_dbs:
                db_dir = Path(db_path).parent
                dump_file = db_dir / f"{Path(db_path).name}_dump.txt"
                if dump_file.exists():
                    kmers_in_this_file = set()
                    with open(dump_file, 'r') as infile:
                        for line in infile:
                            line = line.strip()
                            if line and not line.startswith('#'):
                                parts = line.split()
                                if len(parts) >= 1:
                                    kmer = parts[0]
                                    kmers_in_this_file.add(kmer)
                    
                    # Add 1 to count for each unique k-mer seen in this file
                    for kmer in kmers_in_this_file:
                        kmer_file_count[kmer] = kmer_file_count.get(kmer, 0) + 1
            
            # Write results sorted by k-mer
            with open(binary_file, 'w') as outfile:
                outfile.write("# k-mer\tfile_count\n")
                for kmer in sorted(kmer_file_count.keys()):
                    outfile.write(f"{kmer}\t{kmer_file_count[kmer]}\n")
            
            log(f"  ✓ Binary existence completed: binary_existence.txt")
            log(f"  Total unique k-mers across all files: {len(kmer_file_count)}")
        except Exception as e:
            error_msg = f"  ✗ ERROR creating binary_existence: {str(e)}"
            log(error_msg)
            errors.append(error_msg)
    else:
        log("\nWARNING: No databases were successfully processed, skipping binary_existence")
    
    # Clean up individual file folders
    log("\nCleaning up individual file folders...")
    import shutil
    deleted_count = 0
    for db_path in processed_dbs:
        db_dir = Path(db_path).parent
        try:
            shutil.rmtree(db_dir)
            deleted_count += 1
        except Exception as e:
            log(f"  Warning: Could not delete {db_dir}: {str(e)}")
    
    log(f"  ✓ Deleted {deleted_count} individual file folders")
    
    # Summary
    log("\n" + "=" * 60)
    log(f"PROCESSING COMPLETE!")
    log(f"Successfully processed: {len(processed_dbs)}/{len(fasta_files)} files")
    log(f"\nFinal outputs:")
    log(f"  - overlap_merge/overlap_merge_dump.txt")
    log(f"  - combination_raw/combination_raw.txt")
    log(f"  - binary_existence/binary_existence.txt")
    if errors:
        log(f"\nErrors encountered: {len(errors)}")
        log("\nError details:")
        for error in errors:
            log(error)
    
    return len(errors) == 0

def get_input(prompt, default=None, validate_path=False, check_exists=False, is_dir=False):
    """Get input from user with optional default and validation"""
    if default:
        prompt = f"{prompt} [default: {default}]: "
    else:
        prompt = f"{prompt}: "
    
    while True:
        value = input(prompt).strip()
        
        # Remove surrounding quotes (both single and double)
        if value and len(value) >= 2:
            if (value[0] == '"' and value[-1] == '"') or (value[0] == "'" and value[-1] == "'"):
                value = value[1:-1]
        
        # Use default if no input provided
        if not value and default:
            value = default
        
        if not value:
            print("  Error: This field is required!")
            continue
        
        # Validate path if requested
        if validate_path:
            path = Path(value)
            if check_exists:
                if is_dir and not path.is_dir():
                    print(f"  Error: Directory not found: {value}")
                    continue
                elif not is_dir and not path.exists():
                    print(f"  Error: File not found: {value}")
                    continue
        
        return value

def main():
    parser = argparse.ArgumentParser(
        description='KMC Batch Processing for HPC environments',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Usage modes:
  1. Interactive mode (prompts for inputs):
     python3 kmc_batch_cli.py
  
  2. Command-line arguments mode:
     python3 kmc_batch_cli.py \\
       --kmc /path/to/kmc \\
       --kmc-tools /path/to/kmc_tools \\
       --input /blue/project/fasta_files \\
       --output /blue/project/output \\
       --workdir /blue/project/temp \\
       --k 21 --ram 16 --threads 8
  
  3. Process only first N files (for testing):
     python3 kmc_batch_cli.py \\
       --kmc /path/to/kmc \\
       --kmc-tools /path/to/kmc_tools \\
       --input /blue/project/fasta_files \\
       --output /blue/project/output \\
       --workdir /blue/project/temp \\
       --limit 50
        """
    )
    
    parser.add_argument('--kmc', help='Path to KMC executable')
    parser.add_argument('--kmc-tools', help='Path to KMC_tools executable')
    parser.add_argument('--input', help='Input folder containing FASTA files')
    parser.add_argument('--output', help='Output folder for results')
    parser.add_argument('--workdir', help='Working directory for temporary files')
    parser.add_argument('--k', type=int, help='K-mer length (default: 21)')
    parser.add_argument('--ram', type=int, help='RAM in GB (default: 4)')
    parser.add_argument('--threads', type=int, help='Number of threads (default: 4)')
    parser.add_argument('--limit', type=int, help='Process only first N files (useful for testing)')
    parser.add_argument('--interactive', action='store_true', help='Force interactive mode')
    
    args = parser.parse_args()
    
    # Check if we should use interactive mode
    use_interactive = args.interactive or not any([args.kmc, args.kmc_tools, args.input, args.output, args.workdir])
    
    if use_interactive:
        print("=" * 60)
        print("KMC Batch Processing - Interactive Setup")
        print("=" * 60)
        print()
        
        # Get all inputs interactively
        kmc_exe = get_input(
            "Path to KMC executable",
            validate_path=True,
            check_exists=True,
            is_dir=False
        )
        
        kmc_tools_exe = get_input(
            "Path to KMC_tools executable",
            validate_path=True,
            check_exists=True,
            is_dir=False
        )
        
        input_folder = get_input(
            "Input folder containing FASTA files",
            validate_path=True,
            check_exists=True,
            is_dir=True
        )
        
        output_folder = get_input(
            "Output folder for results"
        )
        
        work_dir = get_input(
            "Working directory for temporary files"
        )
        
        k = int(get_input("K-mer length", default="21"))
        ram = int(get_input("RAM in GB", default="4"))
        threads = int(get_input("Number of threads", default="4"))
        
        # Ask about file limit
        limit_input = get_input("Process only first N files (leave empty for all)", default="None")
        file_limit = int(limit_input) if limit_input and limit_input.isdigit() else None
        
        print()
        print("=" * 60)
        print("Configuration Summary:")
        print("=" * 60)
        print(f"KMC executable:     {kmc_exe}")
        print(f"KMC_tools executable: {kmc_tools_exe}")
        print(f"Input folder:       {input_folder}")
        print(f"Output folder:      {output_folder}")
        print(f"Working directory:  {work_dir}")
        print(f"K-mer length:       {k}")
        print(f"RAM:                {ram} GB")
        print(f"Threads:            {threads}")
        print(f"File limit:         {file_limit if file_limit else 'All files'}")
        print("=" * 60)
        
        confirm = input("\nProceed with these settings? (yes/no) [yes]: ").strip().lower()
        if confirm and confirm not in ['yes', 'y']:
            print("Aborted by user.")
            sys.exit(0)
        print()
        
    else:
        # Use command-line arguments
        kmc_exe = args.kmc
        kmc_tools_exe = args.kmc_tools
        input_folder = args.input
        output_folder = args.output
        work_dir = args.workdir
        k = args.k if args.k else 21
        ram = args.ram if args.ram else 4
        threads = args.threads if args.threads else 4
        file_limit = args.limit
        
        # Validate paths
        if not Path(kmc_exe).exists():
            log(f"ERROR: KMC executable not found: {kmc_exe}")
            sys.exit(1)
        
        if not Path(kmc_tools_exe).exists():
            log(f"ERROR: KMC_tools executable not found: {kmc_tools_exe}")
            sys.exit(1)
        
        if not Path(input_folder).is_dir():
            log(f"ERROR: Input folder not found: {input_folder}")
            sys.exit(1)
    
    # Create output and work directories if they don't exist
    Path(output_folder).mkdir(parents=True, exist_ok=True)
    Path(work_dir).mkdir(parents=True, exist_ok=True)
    
    log("KMC Batch Processing - Starting")
    log("=" * 60)
    log(f"KMC executable: {kmc_exe}")
    log(f"KMC_tools executable: {kmc_tools_exe}")
    log(f"Input folder: {input_folder}")
    log(f"Output folder: {output_folder}")
    log(f"Working directory: {work_dir}")
    log(f"K-mer length: {k}")
    log(f"RAM: {ram} GB")
    log(f"Threads: {threads}")
    log(f"File limit: {file_limit if file_limit else 'None (process all files)'}")
    log("=" * 60)
    
    success = run_kmc_batch(
        kmc_exe, kmc_tools_exe, input_folder, 
        output_folder, work_dir, 
        k, ram, threads, file_limit
    )
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
    
    # /home/minhtran1/kmc_minh/kmc

    # /home/minhtran1/kmc_minh/kmc_tools

    # /orange/simone.marini/m.sy/Merck/BVBRCFASTA

    # /home/minhtran1/kmc_minh/output_folder

    # /home/minhtran1/kmc_minh/working_folder

    # "C:\Users\Minh Tran\Desktop\kmc_test\kmc.exe"

    # "C:\Users\Minh Tran\Desktop\kmc_test\kmc_tools.exe"

    # "C:\Users\Minh Tran\Desktop\kmc_test\fasta_folder"

    # "C:\Users\Minh Tran\Desktop\kmc_test\output_folder"

    # "C:\Users\Minh Tran\Desktop\kmc_test\test_folder"