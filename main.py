import pandas as pd
import numpy as np
import sys

from processNetlogoOutput import DoAbmProcessing

measureCols_raw = [
	'r0_range',
	'policy_pipeline',
	'data_suffix',
	'param_vac_rate_mult',
	'param_final_phase',
	'param_vacincurmult',
]
measureCols = [
	'R0',
	'Policy',
	'Rollout',
	'VacRate',
	'Kids',
	'IncurRate',
]

def indexRenameFunc(chunk):
	index = chunk.index.to_frame()
	#index['R0'] = index['global_transmissibility_out'].apply(lambda x: 3.75 if x < 0.61333 else (4.17 if x < 0.681666 else 4.58))
	
	index['data_suffix'] = index['data_suffix'].replace({
		'_bau.csv' : 'BAU',
		'_int.csv' : 'INT',
		'_az_25.csv' : 'AZ_25',
		'_az_50.csv' : 'AZ_50',
	})
	index['param_final_phase'] = index['param_final_phase'].replace({
		3 : 'No',
		4 : 'Yes',
	})
	
	renameCols = {measureCols_raw[i] : measureCols[i] for i in range(len(measureCols))}
	index = index.rename(columns=renameCols)
	
	chunk.index = pd.MultiIndex.from_frame(index)
	return chunk


inputDir = 'outputs_snowy/'
outputDir = 'outputs_post/'
day_override = 728


def main(arrayIndex):
	DoAbmProcessing(inputDir, outputDir, arrayIndex, indexRenameFunc, measureCols, measureCols_raw, day_override=day_override)


if __name__== "__main__":
	# length check for testing with no arguments.
	main(sys.argv[1] if len(sys.argv) > 1 else 1)
