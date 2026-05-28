import subprocess
import time
import sys
import os

print("========================================")
print("Launching Walmart Real-Time Pipeline")
print("========================================")
print("Pure Python mode - No Java/Spark required!")

# Define the Python executable to use (from the virtual environment)
if os.name == 'nt':
    python_exe = os.path.join("venv", "Scripts", "python.exe")
    streamlit_exe = os.path.join("venv", "Scripts", "streamlit.exe")
else:
    python_exe = os.path.join("venv", "bin", "python")
    streamlit_exe = os.path.join("venv", "bin", "streamlit")

if not os.path.exists(python_exe):
    print(f"Error: Virtual environment not found at {python_exe}. Make sure you are in the project root.")
    sys.exit(1)

# List of commands to run
processes = [
    {"name": "Kafka Producer", "cmd": [python_exe, "kafka/producer.py"]},
    {"name": "Stream Processor", "cmd": [python_exe, "spark/stream_processor.py"]},
    {"name": "ML Predictor", "cmd": [python_exe, "spark/model_predict.py"]},
    {"name": "Streamlit Dashboard", "cmd": [streamlit_exe, "run", "dashboard/app.py"]},
]

running_procs = []

try:
    for proc in processes:
        print(f"Starting {proc['name']}...")
        p = subprocess.Popen(proc["cmd"])
        running_procs.append(p)
        time.sleep(3)
        
    print("\n[OK] All systems are running!")
    print("Keep this terminal open. Press Ctrl+C to stop everything.\n")
    
    # Wait indefinitely
    for p in running_procs:
        p.wait()

except KeyboardInterrupt:
    print("\nStopping all services...")
    for p in running_procs:
        p.terminate()
    # On Windows, also kill child processes
    if os.name == 'nt':
        for p in running_procs:
            try:
                subprocess.run(["taskkill", "/F", "/T", "/PID", str(p.pid)], 
                             capture_output=True, timeout=5)
            except:
                pass
    print("All services stopped.")
