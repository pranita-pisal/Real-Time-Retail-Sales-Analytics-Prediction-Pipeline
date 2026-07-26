import subprocess
import time
import sys
import os

print("========================================")
print("Launching Walmart Real-Time Pipeline")
print("========================================")
print("Pure Python mode - No Java/Spark required!")

# Define the Python and Streamlit executables using the currently active environment
python_exe = sys.executable
python_dir = os.path.dirname(python_exe)

if os.name == 'nt':
    streamlit_exe = os.path.join(python_dir, "streamlit.exe")
else:
    streamlit_exe = os.path.join(python_dir, "streamlit")

# Verify that we are running inside a virtual environment (i.e. not the system python)
if not os.path.exists(streamlit_exe):
    print("Error: Streamlit executable not found. Make sure your virtual environment is activated.")
    sys.exit(1)

# Automatically start Docker databases
print("Starting database containers...")
if os.name != 'nt':
    subprocess.run("PYTHONNOUSERSITE=1 docker-compose up -d", shell=True)
else:
    subprocess.run("docker-compose up -d", shell=True)
print("Waiting 5 seconds for databases to initialize...")
time.sleep(5)

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
    print("\nStopping all Python services...")
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
    
    # Automatically stop Docker databases
    print("Stopping database containers...")
    if os.name != 'nt':
        subprocess.run("PYTHONNOUSERSITE=1 docker-compose down", shell=True)
    else:
        subprocess.run("docker-compose down", shell=True)
    print("All services stopped.")
