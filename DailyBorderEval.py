import json
import pandas as pd
from datetime import datetime as dt, date, timedelta
import pyodbc
import re

# load the json file
config = json.load(open('config.json'))

# connect to server
def connect(crossingObj):
    server = crossingObj['Server']
    initialCatalog = crossingObj['Initial_Catalog']
    uid = crossingObj['User_ID']
    pswd = crossingObj['Password']
    connStr = ('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+initialCatalog+';ENCRYPT=yes;UID='+uid+';PWD='+ pswd)
    return pyodbc.connect(connStr) 

# function to fill out all hours of the day from 00:00 to 23:00
def allHours():
    yesterday = (date.today() - timedelta(1))
    fullDay = pd.date_range(start = yesterday, end = yesterday + timedelta(1), freq ='60T').to_frame()
    fullDay.reset_index(inplace = True)
    fullDay = fullDay.drop(columns = 'index')
    fullDay.rename(columns={fullDay.columns[0]: 'DateTime'}, inplace=True)
    fullDay = fullDay.drop(index=24)
    fullDay = fullDay['DateTime'].dt.to_period('H')
    return fullDay

# function to handle an empty dataframe for segment groups
def noDataSegGroups(crossing, eachBridge, fullDay):
    fullDay = fullDay.to_frame()
    if eachBridge != crossing['certainCrossing']:
        fullDay.insert(1, eachBridge['Col_Names_Seg_Groups'][0], 0)
        fullDay.insert(2, eachBridge['Col_Names_Seg_Groups'][1], 0)
        fullDay.insert(3, eachBridge['Col_Names_Seg_Groups'][2], 0)
        noDataIDToID = fullDay.melt(id_vars=["DateTime"], var_name="SegmentGroup", value_name="Count")
        print(noDataIDToID)
    else:
        fullDay.insert(1, eachBridge['Col_Names_Seg_Groups'][0], 0)
        fullDay.insert(2, eachBridge['Col_Names_Seg_Groups'][1], 0)
        fullDay.insert(3, eachBridge['Col_Names_Seg_Groups'][2], 0)
        fullDay.insert(4, eachBridge['Col_Names_Seg_Groups'][3], 0)
        noDataIDToID = fullDay.melt(id_vars=["DateTime"], var_name="SegmentGroup", value_name="Count")
        print(noDataIDToID)
    return noDataIDToID

# function to handle an empty dataframe for the segments
def noDataSegments(colNames, fullDay):
    fullDay = fullDay.to_frame()
    lstColNamesSegments = colNames
    for colNames in lstColNamesSegments:
        if colNames not in fullDay:
            fullDay[colNames] = 0
        noDataSegToSeg = fullDay.melt(id_vars=['DateTime'], var_name="Segments", value_name="Count")
    print(noDataSegToSeg)
    return noDataSegToSeg
        
# function that will perform the first set of modifications to the base dataframe
def sortedData(baseData):
    phase1Data = baseData[['FromReaderId','ToReaderId','FirstFromTimeStampLocal']]
    phase1Data['NewFromReaderId'] = phase1Data['FromReaderId'].str.extract(r'([a-zA-Z]+_[a-zA-Z]+\d)')
    phase1Data['NewToReaderId'] = phase1Data['ToReaderId'].str.extract(r'([a-zA-Z]+_[a-zA-Z]+\d)')
    phase1Data['FromIDToID'] = phase1Data['NewFromReaderId'] + ["-"] + phase1Data['NewToReaderId']
    phase1Data['FromSegmentToSegment'] = phase1Data['FromReaderId'] + ["-"] + phase1Data['ToReaderId']
    phase1Data['DateTime'] = phase1Data['FirstFromTimeStampLocal'].dt.to_period('H')
    return phase1Data
    
# function that connects to the sql database, and stores the dataframe in a variable
def sqlDB(eachBridge):
    connection = connect(config['Crossings'][eachBridge])
    sqlStr = f"SELECT * FROM {config['Crossings'][eachBridge]['Initial_Catalog']} tableName WHERE DATEDIFF(dd,[FirstFromTimeStampLocal], convert(varchar, getdate(), 101))= 1 ORDER BY [FirstFromTimeStampLocal]"
    baseData = pd.read_sql(sqlStr, connection)
    print(baseData)
    return baseData

# function to modify the data further from the sortedData function (IdToId)
def IDToID(phase1Data, colNames, fullDay):
    phase2Data = sortedData(phase1Data)
    pivotIdToId = phase2Data.pivot_table(index=['DateTime'], columns=['FromIDToID'], values=['FirstFromTimeStampLocal'], aggfunc='count')['FirstFromTimeStampLocal'].fillna(0)
    keepColLst1 = []
    dropColLst1 = []
    regexId1 = re.compile('([a-zA-Z]+)_([a-zA-Z]+)(\d+)-([a-zA-Z]+)_([a-zA-Z]+)(\d+)')
    for col in pivotIdToId.columns:
        tokenLst1 = regexId1.findall(col)
        tokenLst1 = tokenLst1[0]
        if len(tokenLst1) >= 5:
            if int(tokenLst1[5]) - int(tokenLst1[2]) == 1:
                keepColLst1.append(col)
            else:
                dropColLst1.append(col)
    dfIdToId = pivotIdToId.drop(dropColLst1, axis=1)
    lstColNames = colNames
    for colNames in lstColNames:
        if colNames not in dfIdToId:
            dfIdToId[colNames] = 0
    dfIdToId = dfIdToId[lstColNames]
    mergeIdToId = pd.merge(dfIdToId, fullDay, how='outer', left_on='DateTime', right_on='DateTime').fillna(0)
    mergeIdToId = mergeIdToId.sort_values(by=['DateTime'])
    mergeIdToId.reset_index(inplace=True)
    mergeIdToId = mergeIdToId.drop(columns='index')
    mergeIdToId['DateTime'] = mergeIdToId['DateTime'].astype(str)
    mergeIdToId = mergeIdToId.melt(id_vars=["DateTime"], var_name="SegmentGroup", value_name="Count")
    print(mergeIdToId.dtypes)
    print(mergeIdToId)
    return mergeIdToId

# function to modify the data further from the sortedData function (SegToSeg)
def SegToSeg(phase1Data, colNames, fullDay):
    phase3Data = sortedData(phase1Data)
    pivotSegToSeg = phase3Data.pivot_table(index=['DateTime'], columns=['FromSegmentToSegment'], values=['FirstFromTimeStampLocal'], aggfunc='count')['FirstFromTimeStampLocal'].fillna(0)
    keepColLst2 = []
    dropColLst2 = []
    regexId2 = re.compile('([a-zA-Z]+)_([a-zA-Z]+)(\d+)([a-zA-Z])?-([a-zA-Z]+)_([a-zA-Z]+)(\d+)([a-zA-Z])?')
    for col in pivotSegToSeg.columns:
        tokenLst2 = regexId2.findall(col)
        tokenLst2 = tokenLst2[0]
        if len(tokenLst2) >= 7:
            if int(tokenLst2[6]) - int(tokenLst2[2]) == 1:
                keepColLst2.append(col)
            else:
                dropColLst2.append(col)
    dfSegToSeg = pivotSegToSeg.drop(dropColLst2, axis=1)
    lstColNames = colNames
    for colNames in lstColNames:
        if colNames not in dfSegToSeg:
            dfSegToSeg[colNames] = 0
    dfSegToSeg = dfSegToSeg[lstColNames]
    mergeSegToSeg = pd.merge(dfSegToSeg, fullDay, how='outer', left_on='DateTime', right_on='DateTime').fillna(0)
    mergeSegToSeg = mergeSegToSeg.sort_values(by=['DateTime'])
    mergeSegToSeg.reset_index(inplace=True)
    mergeSegToSeg = mergeSegToSeg.drop(columns='index')
    mergeSegToSeg['DateTime'] = mergeSegToSeg['DateTime'].astype(str)
    mergeSegToSeg = mergeSegToSeg.melt(id_vars=["DateTime"], var_name="Segments", value_name="Count")
    print(mergeSegToSeg.dtypes)
    print(mergeSegToSeg)
    return mergeSegToSeg

# function to send the completed dataframes to an sql database
def toSQL(mergeIdToId, mergeSegToSeg):
    secondConnection = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};Server=server;Database=db;Trusted_Connection=yes;')
    cursor = secondConnection.cursor()
    for index, row in mergeIdToId.iterrows():
        cursor.execute("INSERT INTO tableName (DateTime,SegmentGroup,Count) values(?,?,?)", row.DateTime, row.SegmentGroup, row.Count)
        secondConnection.commit()
    for index, row in mergeSegToSeg.iterrows():
        cursor.execute("INSERT INTO tableName (DateTime,Segments,Count) values(?,?,?)", row.DateTime, row.Segments, row.Count) 
        secondConnection.commit()
    cursor.close()

# function to send the completed (0's) dataframes to an sql database
def toSQLEmpty(noDataIDToID, noDataSegToSeg):
    secondConnection = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};Server=server;Database=db;Trusted_Connection=yes;')
    cursor = secondConnection.cursor()
    for index, row in noDataIDToID.iterrows():
        cursor.execute("INSERT INTO tableName (DateTime,SegmentGroup,Count) values(?,?,?)", str(row.DateTime), str(row.SegmentGroup), str(row.Count))
        secondConnection.commit()
    for index, row in noDataSegToSeg.iterrows():
        cursor.execute("INSERT INTO tableName (DateTime,Segments,Count) values(?,?,?)", str(row.DateTime), str(row.Segments), str(row.Count))
        secondConnection.commit()
    cursor.close()
        
# the main function will iterate through every crossing in the JSON file, store the dataframe in a variable, pass the dataframe variable to other functions, then
# write the final dataframes to a database
def main():
    for eachBridge in config['Crossings']:
        baseData = sqlDB(eachBridge)
        if len(baseData) != 0:
            fullDay = allHours()
            modifiedData = sortedData(baseData)
            df1 = IDToID(modifiedData, config['Crossings'][eachBridge]['Col_Names_Seg_Groups'], fullDay)
            df2 = SegToSeg(modifiedData, config['Crossings'][eachBridge]['Col_Names_Segments'], fullDay)
            df3 = None
            df4 = None
            export1 = toSQL(df1, df2) 
        else:
            fullDay = allHours()
            df1 = None
            df2 = None
            df3 = noDataSegGroups(config['Crossings'], config['Crossings'][eachBridge], fullDay)
            df4 = noDataSegments(config['Crossings'][eachBridge]['Col_Names_Segments'], fullDay)
            export2 = toSQLEmpty(df3, df4)

if __name__ == '__main__':
    main()