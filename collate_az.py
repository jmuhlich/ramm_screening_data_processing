import os
import glob
import re
import itertools
import collections
import zipfile
import pandas as pd
from unipath import Path


def finddupe(a):
    return [x for x, y in collections.Counter(a).items() if y > 1]


def fixplateintegrity(df):
    # cycle through the rows
    letters = 'CDEFGHIJKLMN'
    numbers = range(3,22+1)
    print 'DF Shape checking', df.shape
    for indx, wellname in enumerate(itertools.product(letters,numbers)):
        wellname = wellname[0] + str(wellname[1])
        if wellname != df['WellName'][indx]:
            print 'Indx %d InternalWN %s ReadWN %s' % (indx, wellname, df['WellName'][indx])
            print 'Problem found: fixing plate'

            if indx % 2 == 0:
	        # even
	        #newrow = df.iloc[indx].values
	        df = pd.concat([df.iloc[0:indx+1], df.iloc[indx:]], axis=0)
            else:
	        #newrow = df.iloc[indx-1]
	        df = pd.concat([df.iloc[0:indx], df.iloc[indx-1:]], axis=0)
            df = df.reset_index(drop=True)

            print 'Fixed missing value ->', df.shape
    return df

def subdirs(path):
    return [p for p in path.listdir() if p.isdir()]

PROJECT_NAME = 'az'

# List of duplicate analysis result files, to be skipped.
badpaths = (
    ('160816_HCI_AZ_Rep1[4216]/Sim_000002[15604]/2016-08-16T223947-0400[19206]'
     '/struct_max_features[6026113].result.1.csv'),
    ('160821_HCI_AZ_Rep2[4226]/Sim_000004[15713]/2016-08-22T065459-0400[19349]'
     '/funct_max_features[6086183].result.1.csv'),
)

input_path = Path(__file__).parent.child('input', PROJECT_NAME)
output_path = Path(__file__).parent.child('output', PROJECT_NAME)
assert input_path.exists()
output_path.mkdir()

zippaths = [p for p in input_path.listdir() if p.ext == '.zip' and 'Rep3' not in p]

df = {}
plate_df_r1 = []
plate_df_r234 = []

platefilelist = ['Plate layout_AZcpds&liver-kidney_Rep1.xlsx',
                 'Plate layout_AZcpds&liver-kidney_Rep2-4.xlsx']
for pl_df, pl_name in zip((plate_df_r1, plate_df_r234), platefilelist):
    pl_path = input_path.child(pl_name)
    for sheet in (0, 1):
        pl_sheet = pd.read_excel(pl_path, sheet)
        pl_sheet.Row = pl_sheet.Row.astype(unicode)
        pl_sheet.Column = pl_sheet.Column.astype(unicode)
        pl_df.append(pl_sheet)

for zpath in zippaths:
    zfile = zipfile.ZipFile(zpath)
    replicate = re.findall('_Rep(\d)', zpath)[0]

    plate_df = plate_df_r1 if replicate == '1' else plate_df_r234
    df.setdefault(replicate, {})

    # Get Sim_.* directory paths from zip file.
    plate_paths = [n for n in zfile.namelist()
                   if re.search(r'/Sim_\d+\[\d+\]/$', n)]

    for plate_path in plate_paths:
        sim = re.findall('(Sim_00000\d)', plate_path)[0]

        print 'Found', zpath, 'over', plate_path
        print 'R', replicate, 'Sim', sim,

        df[replicate].setdefault(sim, [])

        # Num is used to find the right plate layout file

        # Go through files

        timepoint_paths = [p for p in zfile.namelist()
                           if p.startswith(plate_path) and p.endswith('.csv')
                           and p not in badpaths]

        num_timepoints = len(timepoint_paths)
        assert num_timepoints == 8, ("Expected 8 timepoint .csv files, found %d"
                                     % num_timepoints)

        for csvpath in timepoint_paths:

            print 'csvfile', csvpath
            # Specify 'str' as dtype to prevent any parsing of floats etc. to
            # preserve original values exactly.
            tempdf = pd.read_csv(zfile.open(csvpath), encoding='utf-8', dtype='str')
            tempdf = fixplateintegrity(tempdf)

            # check file integrity

            #For sim1 or sim2
            #Add a time column
            #Add a replicate column, but ignore the sim, we will join sim1 and 3 and sim will not be needed
            print tempdf.shape

            if sim == 'Sim_000001' or sim == 'Sim_000002':

                if sim == 'Sim_000001':
                    pl = plate_df[0]
                if sim == 'Sim_000002':
                    pl = plate_df[1]

                tempdf['ReplicateNumber'] = replicate

                assert len(tempdf) == len(pl), "design-experiment mismatch"
                tempdf = pl.merge(tempdf, on=['WellName', 'Row', 'Column'])
                assert len(tempdf) == len(pl), "design merge failure"

            print 'Adding to df with %s and %s' % (replicate, sim), 'Shape', tempdf.shape
            df[replicate][sim].append(tempdf)

import pdb; pdb.set_trace()
#Ok all read in, now go through each replicate (1 to 4)
#For each sim1 item, join it to corresponding sim3 item, this is replicate X, compounds Y-Z, 7 time points, full set of struct and func features

def non_ascii(text):
    return ''.join([i if ord(i) < 128 else ' ' for i in text])


def drop_duplicate_columns(df):
    keep = [True] * len(df.columns)
    seen = {}
    for i, cname in enumerate(df.columns):
        if cname not in seen:
            seen[cname] = i
        else:
            if not (df.iloc[:, seen[cname]] == df.iloc[:, i]).all():
                raise ValueError(
                    'Duplicate "%s" columns differ; not dropping.' % cname)
            keep[i] = False
    return df.iloc[:, keep]


for replicate, rdf in df.items():

    print 'Checking replicate %s' % replicate
    print 'Length', len(rdf['Sim_000001'])

    final = pd.DataFrame()

    for panel_a, panel_b in (('Sim_000001', 'Sim_000003'),
                             ('Sim_000002', 'Sim_000004')):
        for df1,df2 in zip(rdf[panel_a],rdf[panel_b]):

            print 'Adding for set A with shapes', df1.shape, df2.shape
            df2.columns = [x + '_2' for x in df2.columns.values]
            tempdf = pd.concat([df1, df2], axis=1)
            tempdf = drop_duplicate_columns(tempdf)
            final.append(tempdf)

    for df1,df2 in zip(rdf['Sim_000002'],rdf['Sim_000004']):
        print 'Adding for set B'
        df2.columns = [x + '_2' for x in df2.columns.values]
        tempdf = pd.concat([df1, df2], axis=1)
        tempdf = drop_duplicate_columns(tempdf)
        if 'set 39-73' not in rdf:
            rdf['set 39-73'] = tempdf
        else:
            print 'Pre drop shape', tempdf.shape
            print 'Joining shapes', rdf['set 39-73'].shape, tempdf.shape
            rdf['set 39-73'] = rdf['set 39-73'].append(tempdf)

    finaldf = pd.concat([rdf['set 1-38'], rdf['set 39-73']], axis=0)
    finaldf.index = range(0, len(finalrdf))

    with open(output_path.child('set_'+replicate+'.csv'), 'w') as fp:
        cols = finaldf.columns
        cols = [non_ascii(col) for col in cols]
        finaldf.columns = cols
        finaldf.to_csv(fp)
        fp.write('# Raw data with all replicates, light processing replace blank wells with their technical replicate\n')
