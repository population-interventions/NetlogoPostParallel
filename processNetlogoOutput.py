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
from utilities import GetCohortData


def SplitOutDailyData(chunk, cohorts, days, arrayIndex, name, filePath, fileAppend, fillTo=False):
	columnName = name + '_listOut'
	df = SplitNetlogoNestedList(chunk, cohorts, days, columnName, name, fillTo=fillTo)
	OutputToFile(df, filePath + '_' + fileAppend + '_' + str(arrayIndex), head=False)


def ProcessAbmChunk(
		chunk: pd.DataFrame, outputStaticData, outputDir, arrayIndex,
		measureCols_raw, indexRenameFunc, day_override=False):
	# Drop colums that are probably never useful.
	
	filename = outputDir + 'processed'
	
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
		OutputToFile(staticData, filename + '_static' + '_' + str(arrayIndex), head=False) 
	
	chunk = chunk.drop(['age_listOut', 'atsi_listOut', 'morbid_listOut'], axis=1)
	chunk = chunk.rename(mapper={'[run number]' : 'run'}, axis=1)
	chunk = chunk.set_index(['run', 'rand_seed',] + measureCols_raw)
	
	secondaryData = [
		'scalephase', 'cumulativeInfected',
	]
	
	OutputToFile(chunk[secondaryData], filename + '_secondary' + '_' + str(arrayIndex), head=False)
	chunk = chunk.drop(secondaryData, axis=1)
	chunk = indexRenameFunc(chunk)
	
	SplitOutDailyData(chunk, 1, days, arrayIndex, 'case', filename, 'case', fillTo=day_override)
	SplitOutDailyData(chunk, 1, days, arrayIndex, 'case7', filename, 'case7', fillTo=day_override)
	SplitOutDailyData(chunk, 1, days, arrayIndex, 'case14', filename, 'case14', fillTo=day_override)
	SplitOutDailyData(chunk, 1, days, arrayIndex, 'stage', filename, 'stage', fillTo=day_override)
	SplitOutDailyData(chunk, cohorts, days, arrayIndex, 'infectNoVacArray', 'output_big/' + filename, 'infectNoVac', fillTo=day_override)
	SplitOutDailyData(chunk, cohorts, days, arrayIndex, 'infectVacArray', 'output_big/' + filename, 'infectVac', fillTo=day_override)


def ProcessAbmOutput(
		inputDir, outputDir, arrayIndex,
		indexRenameFunc, measureCols_raw,
		day_override=False):
	filelist = [(inputDir + '{}_table_{}').format(str(arrayIndex), str(arrayIndex))]
		
	print("Processing Files", filelist)
	chunksize = 4 ** 7
	firstProcess = True
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
		OutputToFile(chunk_day, outputDir + 'processed_' + append + '_daily_' + str(arrayIndex), head=False)
		
	index = chunk.columns.to_frame()
	index['week'] = np.floor((index['day'] - dayStartOffset)/7)
	
	chunk.columns = index
	chunk.columns = pd.MultiIndex.from_tuples(chunk.columns, names=['metric', 'day', 'week'])
	chunk.columns = chunk.columns.droplevel(level=0)
	chunk = chunk.groupby(level=[1], axis=1).sum()
	
	OutputToFile(chunk, outputDir + 'processed_' + append + '_weeklyAgg_' + str(arrayIndex), head=False)


def ProcessFileToVisualisation(inputDir, outputDir, arrayIndex, append, measureCols, divisor=False, dayStartOffset=None, outputDay=False):
	chunksize = 4 ** 7
	fileIn = (inputDir + 'processed').format(str(arrayIndex))
	for chunk in tqdm(pd.read_csv(
				fileIn + '_' + append + '_' + str(arrayIndex) + '.csv', chunksize=chunksize,
				index_col=list(range(2 + len(measureCols))),
				header=list(range(3)),
				dtype={'day' : int, 'cohort' : int}), 
			total=4):
		ToVisualisation(chunk, outputDir, arrayIndex, append, measureCols, divisor=divisor, dayStartOffset=dayStartOffset, outputDay=outputDay)


def InfectionsAndStageVisualise(inputDir, outputDir, arrayIndex, measureCols, dayStartOffset=0):
	print('Processing infectNoVac')
	ProcessFileToVisualisation('output_big/' + inputDir, outputDir, arrayIndex, 'infectNoVac', measureCols, dayStartOffset=dayStartOffset)
	print('Processing infectVac')
	ProcessFileToVisualisation('output_big/' + inputDir, outputDir, arrayIndex, 'infectVac', measureCols, dayStartOffset=dayStartOffset)

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


############### Cohort outputs for mort/hosp ###############

def CheckForProblem(df):
	df = df.apply(lambda s: pd.to_numeric(s, errors='coerce').notnull().all())
	if not df.eq(True).all():
		print(df)


def OutputDayAgeAgg(df, outputPrefix, measureCols, arrayIndex):
	df = df.groupby(level=list(range(2 + len(measureCols))), axis=0).sum()
	OutputToFile(df, outputPrefix + '_daily' + '_' + str(arrayIndex), head=False)
	

def OutputWeek(df, outputPrefix, arrayIndex):
	index = df.columns.to_frame()
	index['week'] = np.floor(index['day']/7)
	df.columns = pd.MultiIndex.from_frame(index)
	
	df = df.groupby(level=['week'], axis=1).sum()
	CheckForProblem(df)
	OutputToFile(df, outputPrefix + '_weeklyAgg' + '_' + str(arrayIndex), head=False)

def OutputTenday(df, outputPrefix, arrayIndex):
	index = df.columns.to_frame()
	index['tenday'] = np.floor(index['day']/10)
	df.columns = pd.MultiIndex.from_frame(index)
	
	df = df.groupby(level=['tenday'], axis=1).sum()
	CheckForProblem(df)
	OutputToFile(df, outputPrefix + '_tendayAgg' + '_' + str(arrayIndex), head=False)


def OutputYear(df, outputPrefix, arrayIndex):
	index = df.columns.to_frame()
	index['year'] = np.floor((index['day'])/365)
	df.columns = pd.MultiIndex.from_frame(index)
	
	df = df.groupby(level=['year'], axis=1).sum()
	CheckForProblem(df)
	OutputToFile(df, outputPrefix + '_yearlyAgg' + '_' + str(arrayIndex), head=False)


def ProcessInfectChunk(df, chortDf, outputPrefix, arrayIndex, doTenday=False):
	df.columns = df.columns.set_levels(df.columns.levels[1].astype(int), level=1)
	df.columns = df.columns.set_levels(df.columns.levels[2].astype(int), level=2)
	df = df.sort_values(['cohort', 'day'], axis=1)
	
	col_index = df.columns.to_frame()
	col_index = col_index.reset_index(drop=True)
	col_index = pd.merge(
		col_index, chortDf,
		on='cohort',
		how='left',
		sort=False)
	col_index = col_index.drop(columns=['atsi', 'morbid'])
	df.columns = pd.MultiIndex.from_frame(col_index)
	
	df = df.groupby(level=[3, 1], axis=1).sum()

	# In the ABM age range 15 represents ages 10-17 while age range 25 is
	# ages 18-30. First redestribute these cohorts so they align with 10 year
	# increments.
	df[15], df[25] = df[15] + df[25]/5, df[25]*4/5

	# Then split the 10 year cohorts in half.
	ageCols = [i*10 + 5 for i in range(10)]
	df = df.stack('day')
	for age in ageCols:
		# TODO, vectorise?
		if age in df:
			df[age - 5] = df[age]/2
			df[age] = df[age]/2
		else:
			df[age - 5] = 0
			df[age] = 0
	df = df.unstack('day')
	
	# Add extra cohorts missing from ABM
	df = df.sort_index(axis=0)
	df = df.sort_index(axis=1)
	for age in [100 + i*5 for i in range(2)]:
		df1 = df.loc[:, slice(80, 80)].rename(columns={80 : age}, level=0)
		df = df.join(df1)
	
	df = df.stack(level=['age'])
	OutputToFile(df, 'output_big/' + outputPrefix + '_' + str(arrayIndex), head=False)
	OutputWeek(df.copy(), outputPrefix, arrayIndex)
	if doTenday:
		OutputTenday(df.copy(), outputPrefix, arrayIndex)
	OutputYear(df.copy(), outputPrefix, arrayIndex)
	return df


def ProcessInfectCohorts(measureCols, filename, cohortFile, outputPrefix, arrayIndex):
	cohortData = GetCohortData(cohortFile)
	chunksize = 4 ** 7
	
	for chunk in tqdm(pd.read_csv(
				filename + '.csv', 
				index_col=list(range(2 + len(measureCols))),
				header=list(range(3)),
				dtype={'day' : int, 'cohort' : int},
				chunksize=chunksize),
			total=4):
		df = ProcessInfectChunk(chunk, cohortData, outputPrefix, arrayIndex)
		OutputDayAgeAgg(df, outputPrefix, measureCols, arrayIndex)


def ProcessInfectionCohorts(inputDir, outputDir, arrayIndex, measureCols):
	print('Processing vaccination infection for MortHosp')
	ProcessInfectCohorts(
		measureCols,
		'output_big/' + inputDir + 'processed_infectVac' + '_' + str(arrayIndex),
		inputDir + 'processed_static' + '_' + str(arrayIndex),
		outputDir + 'infect_vac', arrayIndex)
	print('Processing non-vaccination infection for MortHosp')
	ProcessInfectCohorts(
		measureCols,
		'output_big/' + inputDir + 'processed_infectNoVac' + '_' + str(arrayIndex),
		inputDir + 'processed_static' + '_' + str(arrayIndex),
		outputDir + 'infect_noVac', arrayIndex)


############### Cohort outputs for mort/hosp ###############

def DoAbmProcessing(inputDir, outputDir, arrayIndex, indexRenameFunc, measureCols, measureCols_raw, day_override=False, dayStartOffset=0):
	print('Processing ABM Output', inputDir, arrayIndex)
	ProcessAbmOutput(inputDir, outputDir + 'step_1/', arrayIndex, indexRenameFunc, measureCols_raw, day_override=day_override)
	
	CasesVisualise(outputDir + 'step_1/', outputDir + 'visualise/', arrayIndex, measureCols, dayStartOffset=dayStartOffset)
	InfectionsAndStageVisualise(outputDir + 'step_1/', outputDir + 'visualise/', arrayIndex, measureCols, dayStartOffset=dayStartOffset)
	
	print('ProcessInfectionCohorts', inputDir, arrayIndex)
	ProcessInfectionCohorts(outputDir + 'step_1/', outputDir + 'cohort/', arrayIndex, measureCols)

