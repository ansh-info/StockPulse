import os
import sys
from pathlib import Path

print("Current working directory:", os.getcwd())
print("Current file location:", __file__)

project_root = str(Path(__file__).parent.parent)
print("Project root path:", project_root)
print("Directory contents:", os.listdir(project_root))

if project_root not in sys.path:
    sys.path.append(project_root)
    print("Added to sys.path:", project_root)

print("Python sys.path:", sys.path)
