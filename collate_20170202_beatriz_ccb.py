from __future__ import division
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


loc_columns = ['WellName', 'Row', 'Column']
plate_invariant_columns = [
    'ScreenName', 'ScreenID', 'PlateName', 'PlateID', 'MeasurementDate',
    'MeasurementID', 'Timepoint', 'Plane'
]


def _build_wells():
    rows = [(str(i), r) for i, r in enumerate('BCDEFG', 2)]
    columns = [str(i) for i in range(2, 11+1)]
    rc = list(itertools.product(rows, columns))
    data = {
        'WellName': [r[1] + c for r, c in rc],
        'Row': [r[0] for r, c in rc],
        'Column': [c for r, c in rc],
    }
    return pd.DataFrame(data)

def fixplateintegrity(df, _wells=_build_wells()):
    """Fill in any missing rows with nan for all data."""
    # Normally we would copy missing data from a corresponsing technical
    # replicate well, but this plate layout is a bit more scattered so we'll
    # just punt and fill with nan.
    df = df.merge(_wells, how='right')
    missing_idx = df.ScreenName.isnull().nonzero()[0]
    for idx in missing_idx:
        loc = df.iloc[idx][loc_columns]
        column = int(loc.Column)
        # Copy invariant columns from any old row and leave data columns as nan.
        invariant_data = df.iloc[0][plate_invariant_columns]
        df.iloc[idx][plate_invariant_columns] = invariant_data
        print ("        !! Missing data for well %s! Inserting null values."
               % loc.WellName)

    return df


def round_timedelta(delta, granularity):
    """Rounds a timedelta to a given granularity in seconds."""
    s = delta.total_seconds()
    rounded_seconds = (s + granularity / 2) // granularity * granularity
    return datetime.timedelta(0, rounded_seconds)



PROJECT_NAME = '20170202_beatriz_ccb'

# List of duplicate analysis result files, to be skipped.
badpaths = ()

input_path = Path(__file__).parent.child('input', PROJECT_NAME)
output_path = Path(__file__).parent.child('output', PROJECT_NAME)
assert input_path.exists()
output_path.mkdir()

df = pd.DataFrame()

print "Reading experimental design\n==========\n"

layout_filename = 'Layout_plates_170202_24h_72h.xlsx'
# Must resolve() to traverse to parent if input path is a symlink, which it
# typically is.
layout_path = input_path.child(layout_filename)
print "File:", layout_path
xls_converters = {'Row': unicode, 'Column': unicode}
plate_df = pd.read_excel(layout_path, 0, converters=xls_converters)

print "\n\nReading data files\n==========\n"

seen_scans = {}

# The timepoint for each plate (Sim_00000x).
tp_map = {
    '1': 24,
    '2': 72,
}

for rpath in (input_path,):
    print "Scanning", rpath
    replicate = '1'

    # Get Sim_.* directory paths
    plate_paths = [n for n in rpath.listdir()
                   if re.match(r'sim_\d+\[\d+\]$', n.name, re.IGNORECASE)]

    for plate_path in plate_paths:
        sim, sim_num = re.findall('(sim_00000(\d))', plate_path,
                                  re.IGNORECASE)[0]

        print "Searching for plate data in", Path(*plate_path.components()[-2:])
        print "(Rep: %s  Plate: %s)" % (replicate, sim)

        # Find csv files under plate_path, but skip the ones listed in badpaths.
        timepoint_paths = [p for p in plate_path.walk()
                           if p.endswith('.csv')
                           and p not in badpaths]

        # Experiment uses fixed cells, so the two plates are different
        # timepoints, each imaged once.
        expected_num_tps = 1
        num_tps = len(timepoint_paths)
        assert num_tps == expected_num_tps, \
            ("Expected %s timepoint .csv files, found %d"
             % (expected_num_tps, num_tps))

        for csvpath in timepoint_paths:

            exp_timepoint = tp_map[sim_num]
            print '   %sh @ %s' % (exp_timepoint,
                                   Path(*csvpath.components()[-4:]))
            # Specify 'str' as dtype to prevent any parsing of floats etc. to
            # preserve original values exactly.
            tempdf = pd.read_csv(csvpath, encoding='utf-8', dtype='str')
            tempdf = fixplateintegrity(tempdf)
            assert len(tempdf) == 60, ('Expected 60 rows, found %d'
                                       % len(tempdf))

            # Add replicate and timepoint columns.
            # Prepend experimental design and replicate number.
            if True:

                pl = plate_df.copy()
                pl['ReplicateNumber'] = replicate
                pl['ExperimentalTimepointHours'] = exp_timepoint

                assert len(tempdf) == len(pl), "design-experiment mismatch"
                tempdf = pl.merge(tempdf, on=loc_columns)
                assert len(tempdf) == len(pl), "design merge failure"

            df = df.append(tempdf)


#Ok all read in, now go through each replicate (1 to 2)
#For each sim1 item, join it to corresponding sim2 item, this is replicate X, compounds Y-Z, 7 time points, full set of struct and func features

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

if True:

    final = df
    final = final.sort_values('ExperimentalTimepointHours')
    final = final.reset_index(drop=True)
    assert final.shape[0] == 60*len(tp_map), "wrong number of rows"
    assert final.shape[1] == 40, "wrong number of columns"

    final_path = output_path.child('merged.csv')
    print "    Writing output to", final_path
    with open(final_path, 'w') as fp:
        cols = final.columns
        cols = [non_ascii(col) for col in cols]
        final.columns = cols
        final.to_csv(fp, index=False)
