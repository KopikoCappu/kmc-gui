import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext
import subprocess
import os
from pathlib import Path

class KMCBatchGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("KMC Batch Processing GUI")
        self.root.geometry("700x600")
        
        # KMC executable path
        tk.Label(root, text="KMC Executable:").grid(row=0, column=0, sticky='w', padx=5, pady=5)
        self.kmc_exe_entry = tk.Entry(root, width=50)
        self.kmc_exe_entry.grid(row=0, column=1, padx=5, pady=5)
        tk.Button(root, text="Browse", command=self.browse_kmc_exe).grid(row=0, column=2, padx=5, pady=5)
        
        # KMC_tools executable path
        tk.Label(root, text="KMC_tools Executable:").grid(row=1, column=0, sticky='w', padx=5, pady=5)
        self.kmc_tools_exe_entry = tk.Entry(root, width=50)
        self.kmc_tools_exe_entry.grid(row=1, column=1, padx=5, pady=5)
        tk.Button(root, text="Browse", command=self.browse_kmc_tools_exe).grid(row=1, column=2, padx=5, pady=5)
        
        # Input folder
        tk.Label(root, text="Input Folder (FASTA):").grid(row=2, column=0, sticky='w', padx=5, pady=5)
        self.input_folder_entry = tk.Entry(root, width=50)
        self.input_folder_entry.grid(row=2, column=1, padx=5, pady=5)
        tk.Button(root, text="Browse", command=self.browse_input_folder).grid(row=2, column=2, padx=5, pady=5)
        
        # Output folder
        tk.Label(root, text="Output Folder:").grid(row=3, column=0, sticky='w', padx=5, pady=5)
        self.output_folder_entry = tk.Entry(root, width=50)
        self.output_folder_entry.grid(row=3, column=1, padx=5, pady=5)
        tk.Button(root, text="Browse", command=self.browse_output_folder).grid(row=3, column=2, padx=5, pady=5)
        
        # Working directory
        tk.Label(root, text="Working Dir (temp):").grid(row=4, column=0, sticky='w', padx=5, pady=5)
        self.work_dir_entry = tk.Entry(root, width=50)
        self.work_dir_entry.grid(row=4, column=1, padx=5, pady=5)
        tk.Button(root, text="Browse", command=self.browse_work_dir).grid(row=4, column=2, padx=5, pady=5)
        
        # Parameters frame
        params_frame = tk.Frame(root)
        params_frame.grid(row=5, column=0, columnspan=3, pady=10)
        
        tk.Label(params_frame, text="k-mer length:").grid(row=0, column=0, padx=5)
        self.k_entry = tk.Entry(params_frame, width=10)
        self.k_entry.insert(0, "21")
        self.k_entry.grid(row=0, column=1, padx=5)
        
        tk.Label(params_frame, text="RAM (GB):").grid(row=0, column=2, padx=5)
        self.m_entry = tk.Entry(params_frame, width=10)
        self.m_entry.insert(0, "4")
        self.m_entry.grid(row=0, column=3, padx=5)
        
        tk.Label(params_frame, text="Threads:").grid(row=0, column=4, padx=5)
        self.t_entry = tk.Entry(params_frame, width=10)
        self.t_entry.insert(0, "4")
        self.t_entry.grid(row=0, column=5, padx=5)
        
        # Run button
        self.run_button = tk.Button(root, text="Run KMC Batch Processing", command=self.run_batch, bg='green', fg='white', font=('Arial', 12, 'bold'))
        self.run_button.grid(row=6, column=0, columnspan=3, pady=10)
        
        # Progress log
        tk.Label(root, text="Progress Log:").grid(row=7, column=0, sticky='w', padx=5)
        self.log_text = scrolledtext.ScrolledText(root, width=80, height=15)
        self.log_text.grid(row=8, column=0, columnspan=3, padx=5, pady=5)
        
    def browse_kmc_exe(self):
        filename = filedialog.askopenfilename(title="Select KMC executable", filetypes=[("Executable", "*.exe"), ("All files", "*.*")])
        if filename:
            self.kmc_exe_entry.delete(0, tk.END)
            self.kmc_exe_entry.insert(0, filename)
    
    def browse_kmc_tools_exe(self):
        filename = filedialog.askopenfilename(title="Select KMC_tools executable", filetypes=[("Executable", "*.exe"), ("All files", "*.*")])
        if filename:
            self.kmc_tools_exe_entry.delete(0, tk.END)
            self.kmc_tools_exe_entry.insert(0, filename)
    
    def browse_input_folder(self):
        folder = filedialog.askdirectory(title="Select folder containing FASTA files")
        if folder:
            self.input_folder_entry.delete(0, tk.END)
            self.input_folder_entry.insert(0, folder)
    
    def browse_output_folder(self):
        folder = filedialog.askdirectory(title="Select output folder")
        if folder:
            self.output_folder_entry.delete(0, tk.END)
            self.output_folder_entry.insert(0, folder)
    
    def browse_work_dir(self):
        folder = filedialog.askdirectory(title="Select working directory")
        if folder:
            self.work_dir_entry.delete(0, tk.END)
            self.work_dir_entry.insert(0, folder)
    
    def log(self, message):
        self.log_text.insert(tk.END, message + "\n")
        self.log_text.see(tk.END)
        self.root.update()
    
    def normalize_fasta(self, input_file, output_file):
        """Convert multi-line FASTA to single-line format for KMC compatibility"""
        try:
            with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
                current_header = None
                current_sequence = []
                
                for line in infile:
                    line = line.strip()
                    if not line:  # Skip empty lines
                        continue
                    
                    if line.startswith('>'):
                        # Write previous sequence if exists
                        if current_header:
                            outfile.write(current_header + '\n')
                            outfile.write(''.join(current_sequence) + '\n')
                        
                        # Start new sequence
                        current_header = line
                        current_sequence = []
                    else:
                        # Accumulate sequence lines
                        current_sequence.append(line)
                
                # Write last sequence
                if current_header:
                    outfile.write(current_header + '\n')
                    outfile.write(''.join(current_sequence) + '\n')
            
            return True
        except Exception as e:
            self.log(f"  ✗ ERROR normalizing FASTA: {str(e)}")
            return False
    
    def run_batch(self):
        # Validate inputs
        kmc_exe = self.kmc_exe_entry.get()
        kmc_tools_exe = self.kmc_tools_exe_entry.get()
        input_folder = self.input_folder_entry.get()
        output_folder = self.output_folder_entry.get()
        work_dir = self.work_dir_entry.get()
        k = self.k_entry.get()
        m = self.m_entry.get()
        t = self.t_entry.get()
        
        if not all([kmc_exe, kmc_tools_exe, input_folder, output_folder, work_dir, k, m, t]):
            messagebox.showerror("Error", "Please fill in all fields!")
            return
        
        # Get all FASTA files
        fasta_files = []
        for ext in ['*.fasta', '*.fa', '*.fna']:
            fasta_files.extend(Path(input_folder).glob(ext))
        
        if not fasta_files:
            messagebox.showerror("Error", "No FASTA files found in input folder!")
            return
        
        self.log_text.delete(1.0, tk.END)
        self.log(f"Found {len(fasta_files)} FASTA files to process")
        self.log("=" * 60)
        
        # Disable run button during processing
        self.run_button.config(state='disabled')
        
        # Create a normalized_fasta subfolder in working directory
        normalized_dir = Path(work_dir) / "normalized_fasta"
        normalized_dir.mkdir(parents=True, exist_ok=True)
        
        # Process each file
        processed_dbs = []
        errors = []
        
        for i, fasta_file in enumerate(fasta_files, 1):
            file_name = fasta_file.stem
            self.log(f"\n[{i}/{len(fasta_files)}] Processing: {fasta_file.name}")
            
            # Normalize FASTA file first
            normalized_file = normalized_dir / fasta_file.name
            self.log(f"  Normalizing FASTA format...")
            if not self.normalize_fasta(fasta_file, normalized_file):
                errors.append(f"Failed to normalize {fasta_file.name}")
                continue
            self.log(f"  ✓ FASTA normalized")
            
            # Create output subfolder
            file_output_dir = Path(output_folder) / file_name
            file_output_dir.mkdir(parents=True, exist_ok=True)
            
            output_db = str(file_output_dir / file_name)
            
            # Run KMC with normalized file
            self.log(f"  Running KMC...")
            kmc_cmd = [
                kmc_exe,
                f"-k{k}", f"-m{m}", f"-t{t}",
                "-fa", str(normalized_file), output_db, work_dir
            ]
            
            try:
                result = subprocess.run(kmc_cmd, check=True, capture_output=True, text=True)
                self.log(f"  ✓ KMC completed")
                
                # Run kmc_tools dump
                self.log(f"  Running kmc_tools dump...")
                dump_file = str(file_output_dir / f"{file_name}_dump.txt")
                dump_cmd = [kmc_tools_exe, "transform", output_db, "dump", dump_file]
                
                subprocess.run(dump_cmd, check=True, capture_output=True, text=True)
                self.log(f"  ✓ Dump completed: {file_name}_dump.txt")
                
                processed_dbs.append(output_db)
                
            except subprocess.CalledProcessError as e:
                error_msg = f"  ✗ ERROR processing {fasta_file.name}: {str(e)}"
                if e.stderr:
                    error_msg += f"\n    stderr: {e.stderr}"
                self.log(error_msg)
                errors.append(error_msg)
        
        # Create overlap_merge
        if processed_dbs:
            self.log("\n" + "=" * 60)
            self.log("Creating overlap_merge (union of all databases)...")
            
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
                self.log("  ✓ Union completed")
                
                # Dump merged database
                overlap_dump = str(overlap_dir / "overlap_merge_dump.txt")
                dump_cmd = [kmc_tools_exe, "transform", overlap_db, "dump", overlap_dump]
                subprocess.run(dump_cmd, check=True, capture_output=True, text=True)
                self.log(f"  ✓ Dump completed: overlap_merge_dump.txt")
                
            except subprocess.CalledProcessError as e:
                error_msg = f"  ✗ ERROR creating overlap_merge: {str(e)}"
                self.log(error_msg)
                errors.append(error_msg)
        
        # Create combination_raw (concatenated dumps)
        self.log("\nCreating combination_raw (concatenated text dumps)...")
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
            self.log(f"  ✓ Concatenation completed: combination_raw.txt")
        except Exception as e:
            error_msg = f"  ✗ ERROR creating combination_raw: {str(e)}"
            self.log(error_msg)
            errors.append(error_msg)
        
        # Create binary_existence (count files where each k-mer appears)
        self.log("\nCreating binary_existence (presence/absence across files)...")
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
            
            self.log(f"  ✓ Binary existence completed: binary_existence.txt")
            self.log(f"  Total unique k-mers across all files: {len(kmer_file_count)}")
        except Exception as e:
            error_msg = f"  ✗ ERROR creating binary_existence: {str(e)}"
            self.log(error_msg)
            errors.append(error_msg)
        
        # Summary
        self.log("\n" + "=" * 60)
        self.log(f"PROCESSING COMPLETE!")
        self.log(f"Successfully processed: {len(processed_dbs)}/{len(fasta_files)} files")
        if errors:
            self.log(f"Errors encountered: {len(errors)}")
            self.log("\nError details:")
            for error in errors:
                self.log(error)
        
        self.run_button.config(state='normal')
        messagebox.showinfo("Complete", f"Batch processing finished!\nProcessed: {len(processed_dbs)}/{len(fasta_files)} files")

if __name__ == "__main__":
    root = tk.Tk()
    app = KMCBatchGUI(root)
    root.mainloop()