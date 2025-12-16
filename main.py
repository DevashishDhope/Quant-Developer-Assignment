import os
import subprocess
import sys

def main():
    """
    Entry point to run the Streamlit dashboard.
    """
    app_path = os.path.join(os.path.dirname(__file__), 'src', 'app.py')
    print(f"Starting Quant Dashboard from {app_path}...")
    
    # Run Streamlit Command
    # sys.executable ensures we use the same python interpreter
    cmd = [sys.executable, "-m", "streamlit", "run", app_path]
    
    try:
        subprocess.run(cmd, check=True)
    except KeyboardInterrupt:
        print("\nStopping...")

if __name__ == "__main__":
    main()
