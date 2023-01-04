# Misc processes

import os.path
import pandas as pd
from cemconvert.cem import CEM

def proc_hourly(opts):
    '''
    Read in the hourly CEM values by month in the new format
    Write to the old format
    Return a pivoted version
    '''
    cems_months = ('jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec')
    cems = CEM()
    # Init an empty dataframe to hold the hourly values
    hourly = pd.DataFrame()
    for n in opts.months:
        month = cems_months[n-1]
        print('Processing %s' %month, flush=True)
        # Read in CAMPD CEM inputs
        fn = os.path.join(opts.input_path, 'campd-%s-%s-hourly.txt' %(opts.year, month))
        monthly = cems.read_cems_month(fn)
        if opts.write_cems:
            # Write out old CEM format
            fn = os.path.join(opts.output_path, 'HOUR_UNIT_%s_%0.2d.txt' %(opts.year, n))
            cems.write_old_cems(fn, monthly)
        # Pivot the hourly values to columns
        monthly = cems.pivot_hourly(monthly)
        hourly = pd.concat((hourly, monthly))
    hourly['month'] = hourly['date'].str[4:6].astype(int).astype(str).fillna('')
    return hourly

def proc_hourly_meta(hourlymth, fips, sccs):
    '''
    Add in fips and sccs, write files, merge in NOX, SO2, and CO2 into annual FF10 --  
     update and write
    '''
    hourlymth.reset_index(inplace=True)
    hourlymth = hourlymth.merge(fips, on='facility_id', how='left')
    hourlymth = hourlymth.merge(sccs, on=['unit_id','process_id'], how='left')
    hourlymth = hourlymth[hourlymth['daytot'].fillna(0) > 0].copy()
    hourlymth[hourlymth['region_cd'].isnull()].to_csv('nullfips.csv', index=False)
    return hourlymth[hourlymth['region_cd'].notnull()].copy()

def set_key(df, cols=['oris_facility_code','oris_boiler_id','poll']):
    '''
    Set the ORIS - poll key index
    '''
    for col in cols:
        df[col] = df[col].astype(str).str.strip()
    df['key'] = df[cols].agg('_'.join, axis=1)
    df.set_index('key', inplace=True)
    return df

def scale_hourly(hourly, monemis):
    '''
    Scale the hourly values to the monthly values from the annual FF10
    '''
    hrcols = list(hourly.columns)
    # Roll the hourly up to monthly
    idx = ['oris_facility_code','oris_boiler_id','poll','month']
    hrmonth = hourly[idx+['daytot',]].reset_index().groupby(idx, as_index=False).sum()
    unitfac = monemis.merge(hrmonth, on=idx, how='left')
    # Calculate a unit monthly factor for CEMs to annual FF10
    unitfac['scalar'] = unitfac['montot'].fillna(0)/unitfac['daytot'].fillna(0)
    hourly = hourly.merge(unitfac[idx+['scalar',]], on=idx, how='left')
    print(list(hourly.columns))
    valcols = ['daytot',]+['hrval%s' %hr for hr in range(24)]
    hourly[valcols] = hourly[valcols].fillna(0).multiply(hourly['scalar'].fillna(0), axis=0)
    return hourly[hrcols].copy()


