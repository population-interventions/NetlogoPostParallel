import pandas as pd
import numpy as np
import sys

from processNetlogoOutput import DoAbmProcessing

measureCols_raw = [
	'r0_range',
	'param_policy',
	'data_suffix',
	'data_suffix_2',
	'param_vacincurmult',
]
measureCols = [
	'R0',
	'Policy',
	'VacUptake',
	'AgeLimit',
	'IncurRate',
]

def indexRenameFunc(chunk):
	index = chunk.index.to_frame()
	#index['R0'] = index['global_transmissibility_out'].apply(lambda x: 3.75 if x < 0.61333 else (4.17 if x < 0.681666 else 4.58))
	
	#index['data_suffix'] = index['data_suffix'].replace({
	#	'_bau.csv' : 'BAU',
	#	'_int.csv' : 'INT',
	#	'_az_25.csv' : 'AZ_25',
	#	'_az_50.csv' : 'AZ_50',
	#})
	index['data_suffix'] = index['data_suffix'].replace({
		'_65' : 0.65,
		'_70' : 0.7,
		'_80' : 0.8,
		'_85' : 0.85,
		'_90' : 0.9,
		'_95' : 0.95,
	})
	index['data_suffix_2'] = index['data_suffix_2'].replace({
		'_15.csv' : '16+',
		'_10.csv' : '10+',
		'_12.csv' : '12+',
		'_5.csv' : '5+',
	})
	
	renameCols = {measureCols_raw[i] : measureCols[i] for i in range(len(measureCols))}
	index = index.rename(columns=renameCols)
	
	chunk.index = pd.MultiIndex.from_frame(index)
	return chunk


inputDir = 'outputs_snowy/'
outputDir = 'outputs_post/'
day_override = 364


def main(arrayIndex):
	DoAbmProcessing(inputDir, outputDir, arrayIndex, indexRenameFunc, measureCols, measureCols_raw, day_override=day_override)


if __name__== "__main__":
	# length check for testing with no arguments.
	main(sys.argv[1] if len(sys.argv) > 1 else 1)
