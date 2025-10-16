import sys
import subprocess
import os
import shutil
from pathlib import Path

# Step 1: Run all the scripts
scripts = [
    'banking code/accounts.py', 
    'banking code/debit_orders.py',
    'banking code/debit_order_transactions.py', 
    'banking code/loans.py', 
    'banking code/loan_payments.py', 
]

print("=== Running data generation scripts ===")
for script in scripts:
    try:
        print(f"Running {script}...")
        result = subprocess.run([sys.executable, script], check=True, capture_output=True, text=True)
        print(f"{script} completed successfully")
        if result.stdout:
            print(f"Output: {result.stdout}")
    except subprocess.CalledProcessError as e:
        print(f"{script} failed with exit code {e.returncode}")
        print(f"Error: {e.stderr}")
        sys.exit(1)

print("\n=== All scripts completed successfully ===\n")

