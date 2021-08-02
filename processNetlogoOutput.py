# -*- coding: utf-8 -*-
"""
Created on Mon Feb 15 10:22:33 2021

@author: wilsonte
"""

import math
import pandas as pd
import numpy as np
from tqdm import tqdm
import pathlib
import time
import os

from utilities import SplitNetlogoList
from utilities import SplitNetlogoNestedList
from utilities import OutputToFile
from utilities import AddFiles
from utilities import ToHeatmap


def SplitOutDailyData(chunk, cohorts, days, arrayIndex, name, filePath, fileAppend, fillTo=False):
	columnName = name + '_listOut'
	df = SplitNetlogoNestedList(chunk, cohorts, days, columnName, name, fillTo=fillTo)
	OutputToFile(df, filePath + '_' + fileAppend + '_' + str(arrayIndex))


def ProcessAbmChunk(
		chunk: pd.DataFrame, outputStaticData, outputDir, arrayIndex,
		measureCols_raw, indexRenameFunc, day_override=False):
	# Drop colums that are probably never useful.
	
	filename = outputDir + 'process'
	
	chunk = chunk[[
		'[run number]', 'rand_seed',
		'stage_listOut', 'scalephase', 'cumulativeInfected',
		'infectNoVacArray_listOut', 'infectVacArray_listOut',
		'case_listOut', 'case7_listOut',
		'case14_listOut', 'case28_listOut',
		'age_listOut', 'atsi_listOut', 'morbid_listOut'
	] + measureCols_raw]
	
	cohorts = len(chunk.iloc[0].age_listOut.split(' '))
	days = len(chunk.iloc[0].stage_listOut.split(' '))
	if day_override:
		days = day_override
	
	if outputStaticData:
		staticData = pd.DataFrame(
			chunk[['age_listOut', 'atsi_listOut', 'morbid_listOut']].transpose()[0])
		staticData = SplitNetlogoList(staticData, cohorts, 0, '').transpose()
		staticData = staticData.rename(
			columns={'age_listOut': 'age', 'atsi_listOut': 'atsi', 'morbid_listOut': 'morbid'})
		OutputToFile(staticData, filename + '_static') 
	
	chunk = chunk.drop(['age_listOut', 'atsi_listOut', 'morbid_listOut'], axis=1)
	chunk = chunk.rename(mapper={'[run number]' : 'run'}, axis=1)
	chunk = chunk.set_index(['run', 'rand_seed',] + measureCols_raw)
	
	secondaryData = [
		'scalephase', 'cumulativeInfected',
	]
	
	OutputToFile(chunk[secondaryData], filename + '_secondary')
	chunk = chunk.drop(secondaryData, axis=1)
	chunk = indexRenameFunc(chunk)
	
	SplitOutDailyData(chunk, 1, days, arrayIndex, 'case', filename, 'case', fillTo=day_override)
	SplitOutDailyData(chunk, 1, days, arrayIndex, 'case7', filename, 'case7', fillTo=day_override)
	SplitOutDailyData(chunk, 1, days, arrayIndex, 'case14', filename, 'case14', fillTo=day_override)
	SplitOutDailyData(chunk, 1, days, arrayIndex, 'stage', filename, 'stage', fillTo=day_override)
	SplitOutDailyData(chunk, cohorts, days, arrayIndex, 'infectNoVacArray', filename, 'infectNoVac', fillTo=day_override)
	SplitOutDailyData(chunk, cohorts, days, arrayIndex, 'infectVacArray', filename, 'infectVac', fillTo=day_override)


def ProcessAbmOutput(
		inputDir, outputDir, arrayIndex,
		indexRenameFunc, measureCols_raw,
		day_override=False):
	filelist = [(inputDir + '{}_table_{}').format(str(arrayIndex), str(arrayIndex))]
		
	print("Processing Files", filelist)
	chunksize = 4 ** 7
	firstProcess = (arrayIndex == 1)
	for filename in filelist:
		for chunk in tqdm(pd.read_csv(
					filename + '.csv', chunksize=chunksize, header=6),
				total=4):
			ProcessAbmChunk(
				chunk, firstProcess, outputDir, arrayIndex,
				measureCols_raw, indexRenameFunc,
				day_override=day_override)
			firstProcess = False


def ToVisualisation(chunk, outputDir, arrayIndex, append, measureCols, divisor=False, dayStartOffset=0, outputDay=False):
	chunk.columns = chunk.columns.set_levels(chunk.columns.levels[1].astype(int), level=1)
	chunk.columns = chunk.columns.set_levels(chunk.columns.levels[2].astype(int), level=2)
	chunk = chunk.groupby(level=[0, 1], axis=1).sum()
	chunk = chunk.sort_values('day', axis=1)
	if divisor:
		chunk = chunk / divisor
	
	if outputDay:
		chunk_day = chunk.copy()
		chunk_day.columns = chunk_day.columns.droplevel(level=0)
		OutputToFile(chunk_day, outputDir + '_' + append + '_daily_' + str(arrayIndex))
		
	index = chunk.columns.to_frame()
	index['week'] = np.floor((index['day'] - dayStartOffset)/7)
	
	chunk.columns = index
	chunk.columns = pd.MultiIndex.from_tuples(chunk.columns, names=['metric', 'day', 'week'])
	chunk.columns = chunk.columns.droplevel(level=0)
	chunk = chunk.groupby(level=[1], axis=1).sum()
	
	OutputToFile(chunk, outputDir + '_' + append + '_weeklyAgg_' + str(arrayIndex))


def ProcessFileToVisualisation(inputDir, outputDir, arrayIndex, append, measureCols, divisor=False, dayStartOffset=None, outputDay=False):
	chunksize = 4 ** 7
	fileIn = (inputDir + 'process').format(str(arrayIndex))
	for chunk in tqdm(pd.read_csv(
				fileIn + '_' + append + '_' + str(arrayIndex) + '.csv', chunksize=chunksize,
				index_col=list(range(2 + len(measureCols))),
				header=list(range(3)),
				dtype={'day' : int, 'cohort' : int}), 
			total=4):
		ToVisualisation(chunk, outputDir, arrayIndex, append, measureCols, divisor=divisor, dayStartOffset=dayStartOffset, outputDay=outputDay)


def InfectionsAndStageVisualise(inputDir, outputDir, arrayIndex, measureCols, dayStartOffset=0):
	print('Processing infectNoVac')
	ProcessFileToVisualisation(inputDir, outputDir, arrayIndex, 'infectNoVac', measureCols, dayStartOffset=dayStartOffset)
	print('Processing infectVac')
	ProcessFileToVisualisation(inputDir, outputDir, arrayIndex, 'infectVac', measureCols, dayStartOffset=dayStartOffset)

	print('Processing stage')
	ProcessFileToVisualisation(inputDir, outputDir, arrayIndex, 'stage', measureCols, dayStartOffset=dayStartOffset)


def CasesVisualise(inputDir, outputDir, arrayIndex, measureCols, dayStartOffset=0):
	print('Processing cases')
	ProcessFileToVisualisation(
		inputDir, outputDir, arrayIndex, 'case', measureCols, divisor=False,
		dayStartOffset=dayStartOffset, outputDay=True)
	ProcessFileToVisualisation(
		inputDir, outputDir, arrayIndex, 'case7', measureCols, divisor=7,
		dayStartOffset=dayStartOffset, outputDay=True)
	ProcessFileToVisualisation(
		inputDir, outputDir, arrayIndex, 'case14', measureCols, divisor=14,
		dayStartOffset=dayStartOffset, outputDay=True)


def DoAbmProcessing(inputDir, outputDir, arrayIndex, indexRenameFunc, measureCols, measureCols_raw, day_override=False, dayStartOffset=0):
	print('Processing ABM Output', inputDir, arrayIndex)
	ProcessAbmOutput(inputDir, outputDir + 'step_1/', arrayIndex, indexRenameFunc, measureCols_raw, day_override=day_override)
	
	CasesVisualise(outputDir + 'step_1/', outputDir + 'visualise/', arrayIndex, measureCols, dayStartOffset=dayStartOffset)
	InfectionsAndStageVisualise(outputDir + 'step_1/', outputDir + 'visualise/', arrayIndex, measureCols, dayStartOffset=dayStartOffset)

