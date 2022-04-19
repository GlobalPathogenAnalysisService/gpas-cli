import pandas as pd

def validate(sample_sheet: str):
	try:
		pd.read_csv(sample_sheet)
		return True
	except Exception as x:
		return False
