
import pandas as pd
import numpy as np
import sys

from processNetlogoOutput import DoAbmProcessing

measureCols_raw = [
	'r0_range',
	'compound_essential',
	'data_suffix',
	'param_vac_rate_mult',
	'param_policy',
]
measureCols = [
	'R0',
	'Essential',
	'Rollout',
	'VacRate',
	'Policy',
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
	
	renameCols = {measureCols_raw[i] : measureCols[i] for i in range(len(measureCols))}
	index = index.rename(columns=renameCols)
	
	chunk.index = pd.MultiIndex.from_frame(index)
	return chunk


inputDir = '../outputs_snowy/'
outputDir = '../outputs_post/'
day_override = 728


def main(arrayIndex):
	DoAbmProcessing(inputDir, outputDir, arrayIndex, indexRenameFunc, measureCols, measureCols_raw, day_override=day_override)


if __name__== "__main__":
	main(sys.argv[1] if 1 in sys.argv else 1)
