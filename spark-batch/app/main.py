"""
Main entry point cho Batch ETL
Chạy cả Bronze -> Silver và Silver -> Gold
"""

import subprocess
import sys


def run_script(script_path, description):
    """
    Chạy một Python script và theo dõi output
    """
    print("\n" + "="*80)
    print(f"RUNNING: {description}")
    print("="*80 + "\n")
    
    try:
        result = subprocess.run(
            [sys.executable, script_path],
            check=True,
            capture_output=False,
            text=True
        )
        print(f"\n✓ {description} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"\n✗ {description} failed with error code {e.returncode}")
        return False


def main():
    """
    Chạy toàn bộ batch ETL pipeline
    """
    print("\n" + "="*100)
    print(" "*35 + "BATCH ETL PIPELINE")
    print("="*100 + "\n")
    
    scripts = [
        ("/opt/spark-apps/batch_bronze_to_silver.py", "Bronze -> Silver ETL"),
        ("/opt/spark-apps/batch_silver_to_gold.py", "Silver -> Gold ETL")
    ]
    
    results = []
    for script_path, description in scripts:
        success = run_script(script_path, description)
        results.append((description, success))
    
    # Summary
    print("\n" + "="*100)
    print(" "*40 + "ETL SUMMARY")
    print("="*100 + "\n")
    
    for description, success in results:
        status = "✓ SUCCESS" if success else "✗ FAILED"
        print(f"  {status:12} - {description}")
    
    all_success = all(success for _, success in results)
    
    if all_success:
        print("\n" + "="*100)
        print(" "*30 + "ALL ETL PROCESSES COMPLETED SUCCESSFULLY")
        print("="*100 + "\n")
    else:
        print("\n" + "="*100)
        print(" "*30 + "SOME ETL PROCESSES FAILED")
        print("="*100 + "\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
