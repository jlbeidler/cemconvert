import pandas as pd

class CEM:
    '''
    Functions related to CEM format processing
    '''

    def __init__(self):
        # Columns to read in. Use exactly these column names as specified by CAMD.
        self.cemcols = ['Facility ID','Unit ID','Date','Hour','Gross Load (MW)',
          'Steam Load (1000 lb/hr)','SO2 Mass (lbs)','CO2 Mass (short tons)','Heat Input (mmBtu)',
          'SO2 Mass Measure Indicator','NOx Mass Measure Indicator','CO2 Mass Measure Indicator',
          'Operating Time','NOx Rate (lbs/mmBtu)','NOx Rate Measure Indicator','NOx Mass (lbs)']
        # CEM column to a shortened column name
        self.colmap = {'Facility ID': 'oris_facility_code', 'Unit ID': 'oris_boiler_id',
          'Date': 'date', 'Gross Load (MW)': 'GLOAD', 'Steam Load (1000 lb/hr)': 'SLOAD', 
          'Heat Input (mmBtu)': 'HTINPUT','SO2 Mass (lbs)': 'SO2', 'CO2 Mass (short tons)': 'CO2', 
          'SO2 Mass Measure Indicator': 'SO2MEAS', 'NOx Mass (lbs)': 'NOX',
          'NOx Mass Measure Indicator': 'NOXMEAS','CO2 Mass Measure Indicator': 'CO2MEAS',
          'Operating Time': 'OPTIME', 'NOx Rate (lbs/mmBtu)': 'noxrate', 
          'NOx Rate Measure Indicator': 'noxrmeasure', 'Hour': 'hour'}
        # Value columns from the CEM file to set as values in the resulting pivot 
        self.valcols = ['GLOAD','SLOAD','HTINPUT','SO2','CO2','NOX','OPTIME']#,'SO2MEAS','NOXMEAS','CO2MEAS']
        # Measurement names to integer codes
        self.measxref = {'Measured': '01', 'Calculated': '02', 'Substitute': '03',
          'Measured and Substitute': '04'}
        # Old CEM columns
        self.oldcem = ['oris_facility_code','oris_boiler_id','date','hour','NOX','SO2','noxrate',
         'OPTIME','GLOAD','SLOAD','HTINPUT','htinputmeas','SO2MEAS','NOXMEAS','noxrmeasure','flow']

    def set_measure_codes(self, df):
        '''
        Set the measurement string to an integer for cemcorrect
        '''
        for col in ['SO2MEAS','NOXMEAS','CO2MEAS','noxrmeasure']:
            df[col] = df[col].replace(to_replace=self.measxref)
        return df

    def read_cems_month(self, fn):
        '''
        Read in the monthly CEM hourly values and return a dataframe
        '''
        dtype = {'Facility ID': str, 'Unit ID': str, 'SO2 Mass Measure Indicator': str,
          'NOx Mass Measure Indicator': str, 'CO2 Mass Measure Indicator': str,
          'NOx Rate Measure Indicator': str}
        df = pd.read_csv(fn, usecols=self.cemcols, dtype=dtype)
        # Rename columns to shorten names and fit formats
        df.rename(columns=self.colmap, inplace=True)
        df['date'] = df['date'].str[:4] + df['date'].str[5:7] + df['date'].str[8:]
        df = self.set_measure_codes(df)
        print('Records read: %s  NOX sum (lb): %s' %(len(df), sum(df['NOX'].fillna(0)).round(6)))
        return df

    def format_old_cems(self, df):
        '''
        Format to the old CEMs format by month and export the dataframe
        '''
        # Heat input measure code and unit flow not available in new format
        #  Neither are used by SMOKE
        df['htinputmeas'] = ''
        df['flow'] = '-9'
        cems = df[self.oldcem].copy()
        cems['hour'] = cems['hour'].astype(int)
        cems['date'] = cems['date'].str[2:].astype(int)
        valcols = ['NOX','SO2','noxrate','GLOAD','SLOAD','HTINPUT']
        for col in valcols:
            cems[col] = cems[col].fillna(-9).round(4).astype(str)
            cems.loc[cems[col] == '-9.0', col] = '-9'
        return cems

    def write_old_cems(self, fn, monthly):
        ''''
        Format the old CEMs and write to an output file
        '''
        # Format to the old CEMs format and write the monthly file
        cems = self.format_old_cems(monthly)
        cems.to_csv(fn, index=False, header=False)

    def pivot_hourly(self, df):
        '''
        Pivot the hourly data out to columns by hour
        '''
        idx = ['oris_facility_code','oris_boiler_id','date']
        # Update the pollutant values from lbs->tons
        for col in ['SO2','NOX']:
            df[col] = df[col] / 2000
        df['hour'] = 'hrval' + df['hour'].astype(int).astype(str)
        df = df[idx+self.valcols+['hour',]].copy()
        # Melt down the pollutant names
        df = pd.melt(df, id_vars=idx+['hour',], value_vars=self.valcols, var_name='poll', 
          value_name='val')
        df = pd.pivot_table(df, values='val', columns='hour', index=idx+['poll',], aggfunc='sum')
        df.reset_index(inplace=True)
        hours = ['hrval%s' %hr for hr in range(24)]
        # Fill in hour columns that may be missing
        for hour in hours:
            if hour not in list(df.columns):
                df[hour] = None
        df['daytot'] = df[hours].fillna(0).sum(axis=1).round(4)
        df[hours] = df[hours].round(4)
        print('Records postpivot: %s  NOX sum (lb): %s' %(len(df), 
          sum(df.loc[df['poll'] == 'NOX', 'daytot'].fillna(0)).round(6) * 2000))
        # Drop daily records that are 0 for every hour
        df = df[df['daytot'] > 0].copy()
        return df

