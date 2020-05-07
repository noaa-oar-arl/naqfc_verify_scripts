#!/usr/bin/env python                                                                                                                                                                                                               

__author__ = 'Patrick Campbell'
__email__ = 'patrick.c.campbell@noaa.gov'
__license__ = 'GPL'

#Simple MONET utility to command line pair model vs. observations                                                                                                                                                                   

import os
from glob import glob
import sys

import subprocess
from distutils.spawn import find_executable
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter

import monet
from monet.util.tools import long_to_wide
import pandas as pd


def pair_point(da, df, sub_map, interp):
    dfpair = da[sub_map].monet.combine_point(
        df, method=interp, reuse_weights=True)
    return dfpair


def get_airnow(start, end, n_procs=None, datapath=None, verbose=False):
    dates = pd.date_range(start=start, end=end, freq='H')
    dfairnow = monet.obs.airnow.add_data(dates,n_procs=n_procs)
    return dfairnow.drop_duplicates(subset=['time', 'siteid'])


def open_cmaq(finput, verbose=False):
    dset = monet.models.cmaq.open_mfdataset(finput)
    return dset


if __name__ == '__main__':
    parser = ArgumentParser(
        description='pairs cmaq model data to aqs observations',
        formatter_class=ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-f',
        '--files',
        help='string input model file directory/names',
        nargs='+',
        type=str,
        required=True)
    parser.add_argument(
        '-s',
        '--species',
        help='string/list input for obs species-variables to pair',
        type=str,
        nargs='+',
        required=False,
        default=['OZONE', 'PM2.5'])
    parser.add_argument(
        '-o',
        '--output',
        help='string output path for paired dataframe, stats, plots',
        type=str,
        required=False,
        default='./')
    parser.add_argument(
        '-p',
        '--path',
        help='string path to director of network observations',
        type=str,
        required=False,
        default='/data/aqf2/barryb/5xpm/AQS_DATA/')
    parser.add_argument(
        '-i',
        '--interp',
        help=
        'xesmf interpolation scheme, bilinear, conservative, nearest_s2d, nearest_d2s, patch',
        type=str,
        required=False,
        default='bilinear')
    parser.add_argument(
        '-v',
        '--verbose',
        help='print debugging information',
        action='store_true',
        required=False)
    parser.add_argument(
        '-n',
        '--n_procs',
        help='number of processors to speed up download',
        type=int,
        default=None,
        required=False)

    args = parser.parse_args()

    finput = args.files
    species = args.species
    output = args.output
    datapath = args.path
    interp = args.interp
    verbose = args.verbose
    n_procs = args.n_procs

    da = open_cmaq(finput, verbose=verbose)
    start = da.time.to_index()[0]
    end = da.time.to_index()[-1]
    df = get_airnow(start, end, n_procs=n_procs, datapath=None)
    mapping_table = {
            'OZONE': 'O3',
            'PM2.5': 'PM25_TOT',
            'PM10': 'PMC_TOT',
            'CO': 'CO',
            'NO': 'NO',
            'NO2': 'NO2',
            'SO2': 'SO2',
            'NOX': 'NOX',
            'NOY': 'NOY',
            'TEMP': 'TEMP2',
            'WS': 'WSPD10',
            'WD': 'WDIR10',
            'SRAD': 'GSW',
            'BARPR': 'PRSFC',
            'PRECIP': 'RT',
            'RHUM': 'Q2'
             }
    sub_map = {i: mapping_table[i] for i in species if i in mapping_table}
    use_these = [sub_map[i] for i in sub_map.keys()]
    invert_sub_map = dict(map(reversed, sub_map.items()))
    print(df.keys())
    dfpair = pair_point(da, df, use_these, interp)

    dfpair.to_hdf(
            'AIRNOW_CMAQ_' + start.strftime('%Y-%m-%d-%H') + '_' +
            end.strftime('%Y-%m-%d-%H') + '_pair.hdf',
            'dfpair',
            format='table',
            mode='w')
    sys.exit(0)
