import subprocess
import pandas as pd

def validate(sample_sheet: str):
	try:
		pd.read_csv(sample_sheet)
		return True
	except Exception as x:
		return False

def run(cmd):
	return subprocess.run(cmd, shell=True, text=True, capture_output=True)
