import pandas as pd
import numpy as np
from datetime import datetime


class DatasetBuilder:
    """
    Class contains methods to add, remove and manipulate data for time series.
    """
    
    def __init__(self, baseData=None):
        self.df = pd.DataFrame(baseData)

        
    def append_columns(self, columns):
        """
        Append the dataset with additional columns.
        """
        
        self.df = pd.concat([self.df, columns], axis=1)
    
    
    def remove_columns(self, columnNames):
        """
        Remove columns from the dataset.
        """
        self.df = self.df.drop(columnNames,axis=1)

        
    def get_targets(self, columnNames,removeTargets=False):
        """
        Returns the target column(s) and removes it from the dataset.
        """
        
        targets = self.df[columnNames]
        if removeTargets:
            self.remove_columns(columnNames)
        return targets
    
        
    def shift_columns(self, columnNames, shiftCount, keepNaNRows=True):
        """
        Shift the rows in the given columns by specified integer, 
        which can be positiv og negative.  
        """

        assert (self.df.shape[0] > shiftCount), "Shift count can not exceed number of rows in the dataframe."       
        self.df = self.df[columnNames].shift(shiftCount)

        if keepNaNRows:
            return
        else:
            # remove new rows containing NaN values          
            self.df = self.df.drop(self.df.index[:shiftCount]) 
            self.df = self.df.drop(self.df.index[-shiftCount:])

            
    def rolling_mean(self, columnNames, windowSize, w_type=None,keepNaNRows=True):
        """
        Add columns with rolling mean of specified columns. 
        """
        
        for colName in columnNames:
            rolling = self.df[colName].rolling(windowSize,win_type=w_type).mean()
            rollingShifted = rolling.shift(1)
            newColName = "{}_rolling_winsize{}".format(colName, str(windowSize))
            idx = self.df.columns.get_loc(colName) + 1
            self.df.insert(loc=idx, column=newColName,value=rollingShifted)
            
        if keepNaNRows:
            return
        else:
            # remove new rows containing NaN values          
            self.df = self.df.drop(self.df.index[:windowSize]) 
            self.df = self.df.drop(self.df.index[-windowSize:])
 

    def add_lag_variables(self, columnNames, lagCount, shiftCount=1,keepNaNRows=True):
        """
        Add a number of lag variables to the dataset for specified coloumns.
        """
        
        for colName in columnNames:
            lags = np.empty((1,lagCount))
            firstIdx = self.df.index[0]
            
            for i in range(lagCount):
                column = self.df[colName]
                shiftedColumn = column.shift(shiftCount*(i+1))         
                newColName = "{}_lag{}".format(colName, str(i+1))
                idx = self.df.columns.get_loc(colName) +i+1
                self.df.insert(loc=idx, column=newColName,value=shiftedColumn)
               
        if keepNaNRows:
            return
        else:
            # remove new rows containing NaN values          
            self.df = self.df.drop(self.df.index[:lagCount]) 
            self.df = self.df.drop(self.df.index[-lagCount:])

        
    def get_set(self):
        """
        Returns a copy of the dataset as datafame
        """
        
        dataset = self.df
        return dataset
    
    
    def reset_index(self):
        """
        Reset the index of the dataframe and remove additional index columns
        """
        self.df = self.df.reset_index()
        if "index" in self.df.columns:
            self.df = self.df.drop("index", axis=1)
            
            
    def interpolate(self, columnNames,method='linear'):
        """
        Interpolate NaN values in given columns.
        """
        for colName in columnNames:
            if method=='linear':
                interpolatedColumn = self.df[colName].interpolate()

            elif method=='spline':
                None
            elif method=='pad':
                interpolatedColumn = self.df[colName].interpolate(method='pad')
            else:
                # type not recognized
                None
                
            idx = self.df.columns.get_loc(colName)
            self.remove_columns(colName)
            self.df.insert(loc=idx, column=colName,value=interpolatedColumn)
            
            
    def latest_quarter(self, columnNames, operation):
        """
        Add new column with the mean or the difference of values for the latest 
        quarter.
        """
        
        
        assert 'month' in self.df.columns, "No column with name 'month'"
        newColumnNames = []
        for colName in columnNames:
            # find index of first month in a quarter
            for index, row in self.df.iterrows():
                if int(row['month'])  % 3 == 1 and index > 2:
                    startIdx = index
                    break

            newColumn = np.empty((1))
            newColumnName = "{}_latest_quarter_{}".format(colName,operation)
            
            quartersCount = self.df[:].shape[0] // 3      
            quarters = np.reshape(self.df[colName].values,(quartersCount,3))
            if operation == "mean":
                quarterValues = np.mean(quarters,axis=1)
            elif operation == "diff":
                quarterValues = np.max(quarters,axis=1) - np.min(quarters,axis=1)
            
            nans = np.empty((quarterValues.shape[0], 2,))
            nans[:] = np.nan
            quarterValues = np.hstack((nans, (np.reshape(quarterValues, (len(quarterValues), 1)))))
            quarterValues = np.reshape(quarterValues, (len(quarterValues) * 3, 1))

            idx = self.df.columns.get_loc(colName) + 1

            self.df.insert(loc=idx, column=newColumnName,value=quarterValues)
            newColumnNames.append(newColumnName)
        self.shift_columns(newColumnNames, 1)
        return newColumnNames
        
        
    def dummies(self, columnNames):
        """
        Appends the dataset with dummy encoding of the given columns containing categorial data.
        """

        for colName in columnNames:
            column = self.df[colName]
            categories = pd.unique(column)
            
            # delete one to avoid linearity
            categories = categories[:-1]
            newColumnNames = []
            for i in categories:
                newColumnNames.append("{}_{}".format(colName,i))
            
            dummies = np.zeros((1,len(categories)))
            for i in column:
                newRow = np.zeros((1,len(categories)))
                idx = np.where(categories == i)
                if len(idx[0]) > 0 :
                    newRow[0,idx[0][0] - 1] = 1
                dummies = np.append(dummies, newRow, axis=0)

            # remove first row of zeros and append
            dummies = dummies[1:]
            self.append_columns(pd.DataFrame(dummies,columns=newColumnNames))
            self.remove_columns(colName)


    def split_month_year(self, column):
        """
        split a date column of format "year month" into two columns 
        """
        col = self.df[column]
        self.df[["year","month"]] = col.str.split(" ",expand=True)
        allCols = self.df.columns.tolist()
        reorderedCols = allCols[-2:] + allCols[:-2]
        self.df = self.df[reorderedCols]
        self.remove_columns(column)
        
    def resample_year_to_month(self, columnNames):
        """
        Upsamples columns of yearly oberservations to monthly. 
        """
        self.df[columnNames] = pd.to_datetime(self.df[columnNames], format='%Y')
        self.df = self.df.set_index(columnNames)
        self.df = self.df.resample('MS').asfreq()
        
            
    def filter_years(self, yearFrom, yearTo):
        """
        Removes rows from the dataset based on the year of the observation.
        """
        assert 'year' in self.df.columns, "No column with name 'year'"
        self.df = self.df.loc[self.df['year'] >= yearFrom]
        self.df = self.df.loc[self.df['year'] < yearTo]

        
    def filter_months(self, monthFrom, monthTo):
        """
        Removes rows from the dataset based on the month of the observation.
        """
        assert 'month' in self.df.columns, "No column with name 'month'"
        self.df = self.df.loc[self.df['month'] >= monthFrom]
        self.df = self.df.loc[self.df['month'] < monthTo]
        

    def encode_cyclical(self, columnName, stepsInCycle):
        """
        Appends the dataset with cyclical encoding of the given columns containing cyclical data.
        """
        self.df['sin_'+columnName] = np.sin(2*np.pi*self.df[columnName]/stepsInCycle)
        self.df['cos_'+columnName] = np.cos(2*np.pi*self.df[columnName]/stepsInCycle)
            
        self.remove_columns(columnName)
        
    def remove_nan_rows(self):
        None
        
