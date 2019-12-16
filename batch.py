#!/usr/bin/python3

from . import core
import rubricprocessor
import pandas as pd

def checkControlFile (filePath):
   core.checkFileExists (filePath = filePath
                        ,ensureRW = True
                        )
   core.ensureFiletypeCSV (filePath = filePath)

def ensureColumn (cols, colName):
   if colName not in cols:
      raise Exception(f"Required column \'{colName}\' not present")

def getControlFile (filePath):

   checkControlFile (filePath)
   df = pd.read_csv(filePath)
   cols = df.columns.values
   print (cols)
   for ix, c in enumerate(cols):
      if c != c.lower().strip():
         c        = c.lower().strip()
         cols[ix] = c
   df.columns = cols

   ensureColumn (cols, "zipfile")
   ensureColumn (cols, "gradecentre")

   df['status'] = None
   core.DFtoCSV (df, filePath)

   # print (cols)
   return df

def getNow():
   from datetime import datetime
   return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def process (control, outputFolder, writeXML = True, writeJSON = True, preserveDB = True):

   rdict = {"Folder"        : outputFolder
           ,"timeBegin"     : getNow()
           ,"timeEnd"       : None
           ,"rowsControl"   : 0
           ,"rowsProcessed" : 0
           ,"rowsError"     : 0
           }

   df = getControlFile (filePath = control)

   for index, row in df.iterrows():

      rdict["rowsControl"] += 1

      def doThisOne():
         try: # Call "processSingle" for each file
            rdict2 = RubricProcessor.processSingle (row["zipfile"],row["gradecentre"], outputFolder, writeXML, writeJSON, preserveDB)
            returnVal = rdict2["Excel"]
            rdict["rowsProcessed"] += 1
         except Exception as e:
            returnVal = str(e)
            rdict["rowsError"] += 1
         return returnVal

      row["status"] = doThisOne()

   core.DFtoCSV (df, control)

   rdict["timeEnd"] = getNow()

   return rdict
