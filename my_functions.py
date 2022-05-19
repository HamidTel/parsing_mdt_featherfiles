
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta, timezone


import multiprocessing as mp
from multiprocessing import Pool
from functools import partial

import pyarrow
from pyarrow import feather



def mdtPrep(mdtfile,dataDir,dataSource,timezone_offset,preenddate,poststartdate):
        
        ready_mdt_hr=pd.DataFrame()
        mdtRaw=pyarrow.feather.read_feather(dataDir+mdtfile)
        ColsToKeep=['ECI','Longitude','Latitude','ENodeB_ID','Time_Stamp','ServingPCI','ServingRSRP','ServingRSRQ']
        if mdtRaw.shape[0] != 0 :
        
          #all the preprocessing, datatypes adj, renaming, etc:


          ####------------------------------------------------Building the input for pre-post coverage comparison (PPCC)-----------------------------------------------#####
          #####-------------------------------------------------------------Standardize Columns and Dtypes-------------------------------------------------------------#####
          #1.Standarzing the column headers based on input type
          #2.Adding BandType field
          mdtRaw=mdtRaw[ColsToKeep]
          mdtRaw['LocalCellId'] = pd.to_numeric(mdtRaw['ECI'])- np.multiply(256,pd.to_numeric(mdtRaw['ENodeB_ID']))
          mdtRaw=mdtRaw.drop('ECI',axis=1)

          #Fixing the datatype
          dtypeDict={}
          for col in mdtRaw.columns.tolist():
            if col in ['Time_Stamp']:
            #if col in ['Mess','Time_Stamp','Callid','ECI','EnodeB_ID','Longitude','Latitude']:
              colDtype='object'
            elif col in ['Longitude','Latitude']:
              colDtype=np.float32
            elif col in ['ENodeB_ID']:
              colDtype=np.int32
            elif col in ['LocalCellId']:
              colDtype=np.int16
            else:
              colDtype=np.float16

            dtypeDict[col]=colDtype

          mdtRaw = mdtRaw.astype(dtypeDict)
        
        
        
          if dataSource == "MDT":
              #ppcc_cols=["Time_Stamp","ServingRSRP", "ServingRSRQ", "Latitude", "Longitude", "band","band_name","ENodeB_ID","LocalCellId","ServingPCI"]
              #mdt=mdtRaw[ppcc_cols].dropna(subset=["Time_Stamp","Latitude", "Longitude"])
              mdtRaw=mdtRaw.dropna(subset=["Time_Stamp","Latitude", "Longitude"])
              mdtRaw=mdtRaw.rename(columns={'Time_Stamp':'timestamp','ServingRSRP':'rsrp','ServingRSRQ':'rsrq','Latitude':'latitude','Longitude':'longitude'})

              #Adjusting the timestamps
              mdtRaw["timestamp_Orig"]=mdtRaw["timestamp"].copy()
              mdtRaw["timestamp"] = pd.to_datetime(mdtRaw.timestamp,format= '%Y-%m-%d %H:%M:%S',errors='coerce')-timedelta(hours=timezone_offset)

              #Labeling pre-post data
              mdtRaw["dt"] = mdtRaw.timestamp.dt.normalize()
              mdtRaw.loc[mdtRaw.dt <= preenddate, 'period'] = 'p1'
              mdtRaw.loc[mdtRaw.dt >= poststartdate, 'period'] = 'p2'


              #Adding Band_name based on Local cell ID
              conditions = [
                  (0 <=mdtRaw['LocalCellId']) & (mdtRaw['LocalCellId']< 10),
                  (10 <=mdtRaw['LocalCellId']) & (mdtRaw['LocalCellId']< 20),
                  (20 <=mdtRaw['LocalCellId']) & (mdtRaw['LocalCellId']< 30),
                  (30 <=mdtRaw['LocalCellId']) & (mdtRaw['LocalCellId']< 40),
                  (40 <=mdtRaw['LocalCellId']) & (mdtRaw['LocalCellId']< 50),
                  (50 <=mdtRaw['LocalCellId']) & (mdtRaw['LocalCellId']< 60),
                  (60 <=mdtRaw['LocalCellId']) & (mdtRaw['LocalCellId']< 70),
                  (70 <=mdtRaw['LocalCellId']) & (mdtRaw['LocalCellId']< 80),
                  (80 <=mdtRaw['LocalCellId']) & (mdtRaw['LocalCellId']< 90),
                  (90 <=mdtRaw['LocalCellId']) & (mdtRaw['LocalCellId']< 110),
                  (110 <=mdtRaw['LocalCellId']) & (mdtRaw['LocalCellId']< 140),
                  (140 <=mdtRaw['LocalCellId']) & (mdtRaw['LocalCellId']< 150),
                  (150 <=mdtRaw['LocalCellId']) & (mdtRaw['LocalCellId']< 160),
                  (160 <=mdtRaw['LocalCellId']) & (mdtRaw['LocalCellId']< 170),
                  (mdtRaw['LocalCellId']< 0 ) | (mdtRaw['LocalCellId'] >= 170 )
                  ]                                               

              choices = [
                  '2100MHz_B4',
                  '700MHz_B17',
                  '700MHz_B13',
                  '700MHz_B29',
                  '1900MHz_B2',
                  '1900MHz_B2',
                  '1900MHz_B25',
                  '850MHz_B5',
                  '2300MHz_B30',
                  'NA',
                  '2600MHz_B7',
                  '850MHz_B26',
                  '2100MHz_B66',
                  '700MHz_B12',
                  'NA']

              mdtRaw['band_name']=np.select(conditions,choices)

              #Adding band(band_type) based on band_name
              bands_dict = {
                  '2100MHz_B4': 'Highband',
                  '700MHz_B17': 'Lowband',
                  '700MHz_B13': 'Lowband',
                  '700MHz_B29': 'Lowband',
                  '1900MHz_B2': 'Highband',
                  '1900MHz_B25': 'Highband',
                  '850MHz_B5': 'Lowband',
                  '2300MHz_B30': 'Highband',
                  '2600MHz_B7': 'Highband',
                  '850MHz_B26': 'Lowband',
                  '2100MHz_B66': 'Highband',
                  '700MHz_B12': 'Lowband',
              }

              mdtRaw['band'] = mdtRaw['band_name'].map(bands_dict)
          else:
              print("Data source not supported")

        
        return [mdtRaw]