import os
import glob
import re
import itertools
import collections
import zipfile
import datetime
import pandas as pd
import dateutil
from unipath import Path


def finddupe(a):
    return [x for x, y in collections.Counter(a).items() if y > 1]


def fixplateintegrity(df):
    # FIXME this should merge with a full list of wellnames rather than
    # iterate, as the current code will mishandle the case where both replicate
    # wells are missing.
    letters = 'CDEFGHIJKLMN'
    numbers = [str(i) for i in range(3, 22+1)]
    for indx, wellname in enumerate(itertools.product(letters,numbers)):
        wellname = wellname[0] + wellname[1]
        if wellname != df['WellName'][indx]:
            print ('        !! Missing data for well %s; copying from'
                   ' replicate' % wellname)
            if indx % 2 == 0:
	        df = pd.concat([df.iloc[0:indx+1], df.iloc[indx:]], axis=0)
            else:
	        df = pd.concat([df.iloc[0:indx], df.iloc[indx-1:]], axis=0)
            df = df.reset_index(drop=True)
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

print "Reading experimental design\n==========\n"

platefilelist = ['Plate layout_AZcpds&liver-kidney_Rep1.xlsx',
                 'Plate layout_AZcpds&liver-kidney_Rep2-4.xlsx']
for pl_df, pl_name in zip((plate_df_r1, plate_df_r234), platefilelist):
    pl_path = input_path.child(pl_name)
    print "File:", pl_path
    for sheet in (0, 1):
        print "  sheet", sheet
        pl_sheet = pd.read_excel(pl_path, sheet)
        pl_sheet.Row = pl_sheet.Row.astype(unicode)
        pl_sheet.Column = pl_sheet.Column.astype(unicode)
        pl_df.append(pl_sheet)


print "\n\nReading data files\n==========\n"

for zpath in zippaths:
    print "Scanning", zpath
    zfile = zipfile.ZipFile(zpath)
    replicate = re.findall('_Rep(\d)', zpath)[0]

    plate_df = plate_df_r1 if replicate == '1' else plate_df_r234
    df.setdefault(replicate, {})

    # Get Sim_.* directory paths from zip file.
    plate_paths = [n for n in zfile.namelist()
                   if re.search(r'/Sim_\d+\[\d+\]/$', n)]

    for plate_path in plate_paths:
        sim = re.findall('(Sim_00000\d)', plate_path)[0]

        print "Searching for plate data in", plate_path
        print "(Rep: %s  Plate: %s)" % (replicate, sim)

        df[replicate].setdefault(sim, [])

        # Find csv files under plate_path, but skip the ones listed in badpaths.
        timepoint_paths = [p for p in zfile.namelist()
                           if p.startswith(plate_path) and p.endswith('.csv')
                           and p not in badpaths]

        num_timepoints = len(timepoint_paths)
        assert num_timepoints == 8, ("Expected 8 timepoint .csv files, found %d"
                                     % num_timepoints)

        # Here we rely on having an ISO8601 timestamp in the paths so that
        # lexically sorting them puts them in time-course order. We'll still
        # verify our assumption later by inspecting the timestamps inside the
        # files, but this is a nice shortcut.
        timepoint_paths = sorted(timepoint_paths)
        timestamp0 = timepoint_paths[0].split('/')[2][:22]
        t0 = dateutil.parser.parse(timestamp0)

        for csvpath in timepoint_paths:

            timestamp = csvpath.split('/')[2][:22]
            t = dateutil.parser.parse(timestamp)
            delta_t = t - t0

            delta_t_minutes_s = delta_t.total_seconds() // 60 * 60
            delta_t_minutes = datetime.timedelta(seconds=delta_t_minutes_s)
            print '   %s @ %s' % (delta_t_minutes, csvpath.split('/', 2)[-1])
            # Specify 'str' as dtype to prevent any parsing of floats etc. to
            # preserve original values exactly.
            tempdf = pd.read_csv(zfile.open(csvpath), encoding='utf-8', dtype='str')
            tempdf = fixplateintegrity(tempdf)
            assert len(tempdf) == 240, ('Expected 240 rows, found %d'
                                        % len(tempdf))

            #For sim1 or sim2
            #Add a time column
            #Add a replicate column, but ignore the sim, we will join sim1 and 3 and sim will not be needed

            # Prepend experimental design and replicate number to Structural
            # Panel plates; Functional Panel plates will be merged later.
            if sim == 'Sim_000001' or sim == 'Sim_000002':

                if sim == 'Sim_000001':
                    pl = plate_df[0]
                if sim == 'Sim_000002':
                    pl = plate_df[1]
                pl = pl.copy()
                pl['ReplicateNumber'] = replicate

                assert len(tempdf) == len(pl), "design-experiment mismatch"
                tempdf = pl.merge(tempdf, on=['WellName', 'Row', 'Column'])
                assert len(tempdf) == len(pl), "design merge failure"

            df[replicate][sim].append(tempdf)

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


print "\n\nMerging plates\n==========\n"

for replicate, rdf in df.items():

    print "Replicate", replicate

    final = None

    for panel_a, panel_b in (('Sim_000001', 'Sim_000003'),
                             ('Sim_000002', 'Sim_000004')):

        for tp, (df1, df2) in enumerate(zip(rdf[panel_a], rdf[panel_b])):

            print "    Timepoint %d: %s / %s" % (tp, panel_a, panel_b)
            df2.columns = [x + '_2' for x in df2.columns.values]
            tempdf = pd.concat([df1, df2], axis=1)
            tempdf = drop_duplicate_columns(tempdf)
            if final is None:
                final = tempdf
            else:
                assert (tempdf.columns == final.columns).all(), "column mismatch"
                final = final.append(tempdf)

    final = final.sort_values(['MeasurementDate', 'PlateName'])
    final = final.reset_index(drop=True)
    assert final.shape == (3840, 691), "final merged table has wrong size"

    final_path = output_path.child('Replicate_'+replicate+'.csv')
    print "    Writing output to", final_path
    with open(final_path, 'w') as fp:
        cols = final.columns
        cols = [non_ascii(col) for col in cols]
        final.columns = cols
        final.to_csv(fp, index=False)
