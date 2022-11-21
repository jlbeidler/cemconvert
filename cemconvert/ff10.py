#!/usr/bin/env python

import os.path
import csv
import numpy as np
import pandas as pd
from cemconvert.qa import write_annual_qa

class FF10:

    def __init__(self, opts):
        # Variable name to use in the FF10 pollutant field for the temporalizer
        self.temporalvar = opts.temporalvar
        self.ann_cols = ('country_cd','region_cd','tribal_code','facility_id','unit_id','rel_point_id',
          'process_id','agy_facility_id','agy_unit_id','agy_rel_point_id','agy_process_id','scc','poll','ann_value',
          'ann_pct_red','facility_name','erptype','stkhgt','stkdiam','stktemp','stkflow','stkvel','naics',
          'longitude','latitude','ll_datum','horiz_coll_mthd','design_capacity','design_capacity_units',
          'reg_codes','fac_source_type','unit_type_code','control_ids','control_measures','current_cost',
          'cumulative_cost','projection_factor','submitter_id','calc_method','data_set_id',
          'facil_category_code','oris_facility_code','oris_boiler_id','ipm_yn','calc_year','date_updated',
          'fug_height','fug_width_xdim','fug_length_ydim','fug_angle','zipcode','annual_avg_hours_per_year',
          'jan_value','feb_value','mar_value','apr_value','may_value','jun_value','jul_value','aug_value',
          'sep_value','oct_value','nov_value','dec_value','jan_pctred','feb_pctred','mar_pctred',
          'apr_pctred','may_pctred','jun_pctred','jul_pctred','aug_pctred','sep_pctred','oct_pctred',
          'nov_pctred','dec_pctred','comment')
        self.ff10_dtype = {'region_cd': str, 'tribal_code': str, 'facility_id': str, 'unit_id': str,
          'process_id': str, 'scc': str, 'poll': str, 'erptype': str, 'naics': str, 'll_datum': str,
          'fac_source_type': str, 'unit_type_code': str, 'submitter_id': str, 'data_set_id': str,
          'facil_category_code': str, 'oris_facility_code': str, 'oris_boiler_id': str, 'ipm_yn': str,
          'calc_year': str, 'date_updated': str, 'zipcode': str, 'comment': str, 'facility_name': str, 
          'agy_facility_id': str, 'agy_unit_id': str, 'agy_rel_point_id': str, 'agy_process_id': str,
          'reg_codes': str, 'control_ids': str, 'design_capacity_units': str, 'rel_point_id': str,
          'calc_method': str}
        self.hrvals = ['hrval%s' %hr for hr in range(24)]
        self.hourly_cols = ['country_cd','region_cd','tribal_code','facility_id','unit_id','rel_point_id',
          'process_id','scc','poll','op_type_cd','calc_method','date_updated','date','daytot'] + \
          self.hrvals + ['comment',]
        self.month_vals = ['jan_value','feb_value','mar_value','apr_value','may_value','jun_value',
          'jul_value','aug_value','sep_value','oct_value','nov_value','dec_value']
        self.id_cols = ['facility_id','unit_id','rel_point_id','process_id','oris_facility_code',
          'oris_boiler_id']

    def extract_ann_emis(self, df):
        '''
        Extract only those columns related to the hourly. Define process-level apportionment factors
          by pollutant
        '''
        emis = df[self.id_cols+['poll','ann_value']].copy()
        # Keep only those units with a valid boiler and facility
        emis = emis[(emis['oris_facility_code'].notnull()) & \
          (emis['oris_boiler_id'].notnull())].copy()
        # Calc ORIS unit emissions by pollutant for later apportionment
        #  to process-level values
        idx = ['oris_facility_code','oris_boiler_id','poll']
        unit = emis[idx+['ann_value',]].groupby(idx, as_index=False).sum()
        emis = emis.merge(unit, on=idx, how='left', suffixes=['','_unit'])
        emis['unit_frac'] = emis['ann_value'] / emis['ann_value_unit']
        return emis[self.id_cols+['poll','ann_value','unit_frac']].copy()

    def write_annual(self, fn, annual, ann_ff10, year):
        '''
        Write the annual FF10
        Specify the output file name, 
        '''
        monthly = self.calc_monthly_vals(annual)
        annual = annual.groupby(self.id_cols+['poll',], as_index=False).sum()
        annual = annual.merge(monthly, on=self.id_cols+['poll',], how='left', suffixes=['_old',''])
        annual.rename(columns={'daytot': 'ann_value'}, inplace=True)
        # Fill FF10 fields for HOURACT values
        temp = annual[annual['poll'] == self.temporalvar].copy()
        ann_cols = self.id_cols + ['scc','latitude','longitude','erptype','region_cd',
          'facility_name','ipm_yn','country_cd']
        temp = temp.merge(ann_ff10[ann_cols].drop_duplicates(self.id_cols), 
          on=self.id_cols, how='left')
        # Fill FF10 fields for non-HOURACT values
        annual = annual[annual['poll'] != self.temporalvar].merge(ann_ff10, 
          on=self.id_cols+['poll',], how='outer', suffixes=['_cem',''])
        annual = pd.concat((annual, temp))
        idx = annual['ann_value_cem'].notnull()
        annual.loc[idx, 'ann_value'] = annual.loc[idx, 'ann_value_cem']
        annual = annual.merge(monthly, on=self.id_cols+['poll',], how='left', suffixes=['_f',''])
        country = str(annual['country_cd'].values[0])
        round_cols = ['ann_value','latitude','longitude']
        annual[round_cols] = annual[round_cols].round(8)
        with open(fn, 'w') as f:
            head = '#FORMAT=FF10_POINT\n#COUNTRY=%s\n#YEAR=%s\n' %(country, year)
            f.write(head)
            f.write('%s\n' %','.join(self.ann_cols))
            annual.to_csv(f, columns=self.ann_cols, index=False, quoting=csv.QUOTE_NONNUMERIC,
              header=False)
        fn = os.path.join(os.path.dirname(fn), 'qa_%s'  %os.path.basename(fn))
        write_annual_qa(fn, annual, ann_ff10, self.temporalvar)  

    def calc_monthly_vals(self, df):
        '''
        Calculate the monthly values from the daily values
        '''
        df['month'] = df['month'].astype(int)
        df = df.groupby(self.id_cols+['month','poll'], as_index=False).sum()
        df['month'] = df['month'].apply(lambda x: self.month_vals[x-1])
        df = pd.pivot_table(df, columns='month', values='daytot', index=self.id_cols+['poll',], 
          aggfunc=np.sum, fill_value=0).reset_index()
        for col in self.month_vals:
            if col not in list(df.columns):
                df[col] = 0
        df[self.month_vals] = df[self.month_vals].round(8)
        return df

    def write_monthly_ff10(self, df, fn):
        '''
        Write the hourly monthly FF10
        '''
        print(fn)
        country = str(df['country_cd'].drop_duplicates()[0])
        year = str(df['date'].values[0])[:4]
        df[self.hrvals+['daytot',]] = df[self.hrvals+['daytot',]].round(8) 
        with open(fn, 'w') as f:
            head = '#FORMAT=FF10_HOURLY_POINT\n#COUNTRY=%s\n#YEAR=%s\n' %(country, year)
            f.write(head)
            df.to_csv(f, columns=self.hourly_cols, index=False)

    def read_ann_ff10(self, fn):
        '''
        Read in the entire annual FF10
        '''
        with open(fn) as f:
            head = []
            for l in f:
                if l.startswith('#') or l.strip() == '':
                    head.append(l.strip())
                else:
                    break
        df = pd.read_csv(fn, dtype=self.ff10_dtype, skiprows=len(head))
        return head, df

