#!/usr/bin/env python

###############################################################
# < next few lines under version control, D O  N O T  E D I T >
# $Date: 2018-03-29 10:12:00 -0400 (Thu, 29 Mar 2018) $
# $Revision: 100014 $
# $Author: Barry.Baker@noaa.gov $
# $Id: nemsio2nc4.py 100014 2018-03-29 14:12:00Z Barry.Baker@noaa.gov $
###############################################################

__author__ = 'Patrick Campbell'
__email__ = 'Patrick.C.Campbell@noaa.gov'
__license__ = 'GPL'

import os
import subprocess
import sys
from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser

import cartopy.crs as ccrs
import dask
import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import numpy as np

import monet
from monet.util.tools import calc_8hr_rolling_max,calc_24hr_ave,get_relhum
sns.set_context('notebook')

plt.ioff()
'''
Simple utility to make Taylor from the NAQFC forecast
'''
initial_datetime = None

def  make_24hr_regulatory(df,col=None):
     """ Make 24-hour averages """
     return calc_24hr_ave(df,col)

def  make_8hr_regulatory(df,col=None):
     """ Make 8-hour rolling average daily """
     return calc_8hr_rolling_max(df,col,window=8)


def chdir(fname):
    dir_path = os.path.dirname(os.path.realpath(fname))
    os.chdir(dir_path)
    return os.path.basename(fname)


def load_paired_data(fname):
    return pd.read_hdf(fname)


def make_taylor_diagram(df, col1, col2, scale, savename,date=None):
    dia = monet.plots.plots.taylordiagram(
        df, col1=col1, col2=col2, label1='AIRNOW', label2='CMAQ', scale=scale)
    date = df.time.min()
    #date = pd.Timestamp(date)
    #dt = date - initial_datetime
    #dtstr = str(dt.days * 24 + dt.seconds // 3600).zfill(2)
    plt.legend(loc=(.8, .8))
    name = "{}.{}.jpg".format(savename, date.strftime('tyr.%d%H'))
    
    monet.plots.savefig(
        name, bbox_inches='tight', dpi=100, loc=3, decorate=True)
    return dia 

def make_plots(df, variable, obs_variable, startdate, enddate, scale, out_name):
        if startdate == None and enddate == None: 
          for t in df.time.unique():
            date = pd.Timestamp(t)
            print(
                ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
            print('Creating Plot:', obs_variable, 'at time:', date)
            print(
                ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
            odf = df.loc[df.time ==
                          date, ['time', 'latitude', 'longitude', obs_variable, variable]]
            if ~odf.empty:
              make_taylor_diagram(odf, col1=obs_variable, col2=variable, scale=scale, savename=out_name, date=t)
        #make total period taylor plot
        else:
          sdate=pd.Timestamp(startdate)
          edate=pd.Timestamp(enddate)
          print(
                ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
          print('Creating Plot:', obs_variable, 'for period:', startdate, 'to ', enddate  )
          print(
                ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
          make_taylor_diagram(df, col1=obs_variable, col2=variable, scale=scale, savename=out_name, date=edate)         

if __name__ == '__main__':

    parser = ArgumentParser(
        description='Make Taylor Plots for each time step or total period in files',
        formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '-p',
        '--paired_data',
        help='paired data input file',
        type=str,
        required=True)
    parser.add_argument(
        '-s', '--species', nargs='+', help='Species', required=False, default=['OZONE'])
    parser.add_argument(
        '-b',
        '--subset_epa',
        help='EPA Region Subset true/false',
        type=bool,
        required=False,
        default=False)
    parser.add_argument(
        '-e',
        '--epa_region',
        help='EPA Region ACRONYMs',
        type=str,
        required=False,
        default='domain')
    parser.add_argument(
        '-n',
        '--output_name',
        help='Taylor plot Output base name',
        type=str,
        required=False,
        default='CMAQ_AIRNOW')
    parser.add_argument(
        '-r',
        '--regulatory',
        help='boolean set to True fore 8-hrmax  or 24-ave NAAQS regulatory calcs',
        type=bool,
        required=False,
        default=False)
    parser.add_argument(
        '-sd',
        '--startdate',
        help='Startdate for taylor statistics over a period YYYY-MM-DD HH:MM:SS',
        type=str,
        required=False,
        default=None)
    parser.add_argument(
        '-ed',
        '--enddate',
        help='Enddate for taylor statisics over a period YYYY-MM-DD HH:MM:SS',
        type=str,
        required=False,
        default=None)
    parser.add_argument(
        '-sc',
        '--scale',
        help='Scaling factor for standard deviation axes on Taylor diagram',
        type=float,
        required=False,
        default=1.5)
    args = parser.parse_args()

    paired_data = args.paired_data
    species     = args.species
    out_name    = args.output_name
    subset      = args.subset_epa
    region      = args.epa_region
    startdate   = args.startdate
    enddate     = args.enddate
    reg         = args.regulatory   
    scale       = args.scale
    
#load the paired dataframe 
    df = load_paired_data(paired_data)

    mapping_table = {'OZONE':'O3', 'PM2.5':'PM25_TOT', 'PM10':'PMC_TOT', 'CO':'CO_new', 'NO':'NO_new', 'NO2':'NO2_new', 'SO2':'SO2_new','NOX':'NOX_new','NOY':'NOY_new','TEMP':'TEMP2','WS':'WSPD10','WD':'WDIR10','SRAD':'GSW','BARPR':'PRSFC','PRECIP':'RT','RHUM':'Q2'}
    sub_map = {i: mapping_table[i] for i in species if i in mapping_table}
# subset  only the correct region
    if subset is True:
     df.query('epa_region == '+'"'+region+'"',inplace=True)

#Loop through species
    for jj in species:
     df_replace = df.replace(0.0,np.nan) #Replace all exact 0.0 values with nan
     df_drop=df_replace.dropna(subset=[jj,sub_map.get(jj)]) #Drops all rows with obs species = NaN    

#Converts OZONE, PM10, or PM2.5 dataframe to NAAQS regulatory values
     if jj == 'OZONE' and reg is True:
      df2 = make_8hr_regulatory(df_drop,[jj,sub_map.get(jj)]).rename(index=str,columns={jj+'_y':jj,sub_map.get(jj)+'_y':sub_map.get(jj)})
     elif jj == 'PM2.5' and reg is True:
      df2 = make_24hr_regulatory(df_drop,[jj,sub_map.get(jj)]).rename(index=str,columns={jj+'_y':jj,sub_map.get(jj)+'_y':sub_map.get(jj)})
     elif jj == 'PM10' and reg is True:
      df2 = make_24hr_regulatory(df_drop,[jj,sub_map.get(jj)]).rename(index=str,columns={jj+'_y':jj,sub_map.get(jj)+'_y':sub_map.get(jj)})
     else:
      df2=df_drop
#Convert airnow met variable if necessary:
     if jj == 'WS':
      df2.loc[:,'WS']=df2.loc[:,'WS']*0.514  #convert obs knots-->m/s
      df2.query('WS > 0.2',inplace=True)  #Filter out calm WS obs (< 0.2 m/s), should not be trusted--creates artificially larger postive  model bias
     elif jj == 'BARPR':
      df2.loc[:,'PRSFC']=df2.loc[:,'PRSFC']*0.01 #convert model Pascals-->millibars
     elif jj == 'PRECIP':
      df2.loc[:,'PRECIP']=df2.loc[:,'PRECIP']*0.1 #convert obs mm-->cm
     elif jj == 'TEMP':
      df2.loc[:,'TEMP2'] = df2.loc[:,'TEMP2']-273.16 #convert model K-->C
     elif jj == 'RHUM':
     #convert model mixing ratio to relative humidity
      df2.loc[:,'Q2'] = get_relhum(df2.loc[:,'TEMP2'],df2.loc[:,'PRSFC'],df2.loc[:,'Q2'])
     # df2.rename(index=str,columns={"Q2": "RH_mod"},inplace=True)
     elif jj == 'CO':
      df2.loc[:,'CO']=df2.loc[:,'CO']*1000.0 #convert obs ppm-->ppb
     else:
      df2=df2
#subset for period, or use output frequency
     if startdate != None and enddate != None:
      mask = (df2['time'] >= startdate) & (df2['time'] <= enddate)
      dfnew =df2.loc[mask]
      import datetime
      startdatename_obj = datetime.datetime.strptime(startdate, '%Y-%m-%d %H:%M:%S')
      enddatename_obj   = datetime.datetime.strptime(enddate, '%Y-%m-%d %H:%M:%S')
      startdatename = str(datetime.datetime.strftime(startdatename_obj,'%Y-%m-%d_%H'))
      enddatename = str(datetime.datetime.strftime(enddatename_obj,'%Y-%m-%d_%H'))
      outname = "{}.{}.{}.{}.{}".format(out_name, region, jj, startdatename, enddatename)
      if reg is True:
       outname = "{}.{}.{}.{}.{}.{}".format(out_name,region, jj,startdatename, enddatename,'reg')
      if jj == 'PM2.5':
       outname = outname.replace('PM2.5','PM2P5')
      if region == 'domain':
       outname = outname.replace('domain','5X')
     else:
      dfnew = df2
      outname = "{}.{}.{}".format(out_name,region, jj)
      if reg is True:
       outname = "{}.{}.{}.{}".format(out_name,region, jj,'reg')
      if jj == 'PM2.5':
       outname = outname.replace("PM2.5","PM2P5")
      if region == 'domain':
       outname = outname.replace("domain","5X")  

     dfnew_drop=dfnew.dropna(subset=[jj,sub_map.get(jj)]) 
# make the plots
     make_plots(dfnew, sub_map.get(jj), jj, startdate, enddate, scale, outname)
