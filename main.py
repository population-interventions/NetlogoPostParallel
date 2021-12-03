import pandas as pd
import numpy as np
import sys

from processNetlogoOutput import DoAbmProcessing

measureCols_raw = [
	'r0_range',
	'policy_pipeline',
	'data_suffix',
	'param_final_phase',
	'param_vacincurmult',
	'compound_trace',
	'min_stage',
	'sensitivity',
]
measureCols = [
	'R0',
	'Policy',
	'VacUptake',
	'Kids',
	'IncurRate',
	'TracePower',
	'MinStage',
	'sensitivity',
]

def indexRenameFunc(chunk):
	index = chunk.index.to_frame()
	#index['R0'] = index['global_transmissibility_out'].apply(lambda x: 3.75 if x < 0.61333 else (4.17 if x < 0.681666 else 4.58))

	index['data_suffix'] = index['data_suffix'].replace({
		"_az_25.csv" : 0.80,
		"_az_25_95.csv" : 0.95,
		"_az_25_90.csv" : 0.9,
		"_az_25_80.csv" : 0.8,
		"_az_25_70.csv" : 0.7,
	})
	index['param_final_phase'] = index['param_final_phase'].replace({
		3 : 'No',
		4 : 'Yes',
	})
	index['min_stage'] = index['min_stage'].replace({
		0 : '1a',
		2 : '2',
	})
	
	renameCols = {measureCols_raw[i] : measureCols[i] for i in range(len(measureCols))}
	index = index.rename(columns=renameCols)
	
	chunk.index = pd.MultiIndex.from_frame(index)
	return chunk


inputDir = 'outputs/snowy/'
outputDir = 'outputs_post/'
day_override = 574


def main(arrayIndex):
	DoAbmProcessing(inputDir, outputDir, arrayIndex, indexRenameFunc, measureCols, measureCols_raw, day_override=day_override)


if __name__== "__main__":
	# length check for testing with no arguments.
	main(sys.argv[1] if len(sys.argv) > 1 else 1)
