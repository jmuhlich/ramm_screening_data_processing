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
    rows = [(str(i), r) for i, r in enumerate('CDEFGHIJKLMN', 3)]
    columns = [str(i) for i in range(3, 22+1)]
    rc = list(itertools.product(rows, columns))
    data = {
        'WellName': [r[1] + c for r, c in rc],
        'Row': [r[0] for r, c in rc],
        'Column': [c for r, c in rc],
    }
    return pd.DataFrame(data)

def fixplateintegrity(df, _wells=_build_wells()):
    """Fill in any missing rows from the corresponding technical replicate."""
    # FIXME: This could be more robust if done after merging with the layout,
    # since we could either find the replicate well directly or at least verify
    # our assumptions. (This experiment lays the technical replicates out
    # horizontally.)
    df = df.merge(_wells, how='right')
    missing_idx = df.ScreenName.isnull().nonzero()[0]
    for idx in missing_idx:
        loc = df.iloc[idx][loc_columns]
        column = int(loc.Column)
        # Select the technical replicate -- they come in horizontal pairs.
        if column % 2 == 0:
            rep_column = column - 1
        else:
            rep_column = column + 1
        rep_indexer = (df.Column == str(rep_column)) & (df.Row == loc.Row)
        rep_data = df[rep_indexer].iloc[0].copy()
        # Check the final (data) column since we'd never touch that one in the
        # "both replicates missing" case.
        if pd.isnull(rep_data.iloc[-1]):
            # Both replicates are missing - copy invariant columns from any old
            # row and leave data columns as nan.
            invariant_data = df.iloc[0][plate_invariant_columns]
            rep_data[plate_invariant_columns] = invariant_data
            print ("        !! Missing data for well %s; replicate also"
                   " missing! Inserting null values." % loc.WellName)
        else:
            rep_wellname = rep_data.WellName
            print ("        ** Missing data for well %s; copying from"
                   " replicate in %s" % (loc.WellName, rep_wellname))
        rep_data[loc_columns] = loc
        df.iloc[idx] = rep_data

    return df


def round_timedelta(delta, granularity):
    """Rounds a timedelta to a given granularity in seconds."""
    s = delta.total_seconds()
    rounded_seconds = (s + granularity / 2) // granularity * granularity
    return datetime.timedelta(0, rounded_seconds)



PROJECT_NAME = '20170127_beatriz_pfizer'

# List of duplicate analysis result files, to be skipped.
badpaths = ()

input_path = Path(__file__).parent.child('input', PROJECT_NAME)
output_path = Path(__file__).parent.child('output', PROJECT_NAME)
assert input_path.exists()
output_path.mkdir()

df = {}

print "Reading experimental design\n==========\n"

layout_filename = 'Plate layout_Pfizer_Rep1_Rep2.xlsx'
# Must resolve() to traverse to parent if input path is a symlink, which it
# typically is.
layout_path = input_path.child(layout_filename)
print "File:", layout_path
xls_converters = {'Row': unicode, 'Column': unicode}
plate_df = pd.read_excel(layout_path, 0, converters=xls_converters)

print "\n\nReading data files\n==========\n"

seen_scans = {}

# The different timepoints for each replicate.
tp_map = {
    '1': [0, 4, 8, 12, 16, 20, 24, 52, 72],
    '2': [0, 4, 8, 24, 48, 72],
}

for rpath in (input_path.listdir(filter=Path.isdir)):
    print "Scanning", rpath
    matches = re.findall('rep(\d)', rpath, re.IGNORECASE)
    replicate = matches[0]

    df.setdefault(replicate, {})

    # Get Sim_.* directory paths
    plate_paths = [n for n in rpath.listdir()
                   if re.match(r'sim_\d+\[\d+\]$', n.name, re.IGNORECASE)]

    for plate_path in plate_paths:
        sim, sim_num = re.findall('(sim_00000(\d))', plate_path,
                                  re.IGNORECASE)[0]
        sim_num = int(sim_num)

        print "Searching for plate data in", Path(*plate_path.components()[-2:])
        print "(Rep: %s  Plate: %s)" % (replicate, sim)

        df[replicate].setdefault(sim, [])

        # Find csv files under plate_path, but skip the ones listed in badpaths.
        timepoint_paths = [p for p in plate_path.walk()
                           if p.endswith('.csv')
                           and p not in badpaths]

        expected_tps = tp_map[replicate]
        expected_num_tps = len(expected_tps)
        num_tps = len(timepoint_paths)
        assert num_tps == expected_num_tps, \
            ("Expected %s timepoint .csv files, found %d"
             % (expected_num_tps, num_tps))

        # Here we rely on having an ISO8601 timestamp in the paths so that
        # lexically sorting them puts them in time-course order. We'll still
        # verify our assumption later by inspecting the timestamps inside the
        # files, but this is a nice shortcut.
        timepoint_paths = sorted(timepoint_paths)
        timestamp0 = timepoint_paths[0].components()[-2][:22]
        t0 = dateutil.parser.parse(timestamp0)
        seen_timepoints = []

        for csvpath in timepoint_paths:

            # Ensure we don't have duplicate files for the same plate +
            # timepoint (apparently some scans were processed more than once).
            # badpaths is supposed to contain all of the duplicates for
            # filtering above, and this code makes sure we didn't miss any.
            scan_name = csvpath.components()[-2]
            timestamp, scan_id = re.findall(r'^([^[]+)\[(\d+)\]$', scan_name)[0]
            full_path = plate_path + csvpath
            if scan_id in seen_scans:
                other_path = seen_scans[scan_id]
                msg = ("duplicate scan ID %s found in filenames:"
                       "\n    %s\n    %s" % (scan_id, other_path, full_path))
                raise Exception(msg)
            seen_scans[scan_id] = full_path

            t = dateutil.parser.parse(timestamp)
            delta_t = t - t0

            hour = 60 * 60
            # Experimental timepoints are supposed to be a multiple of 4 hours.
            delta_t_4h = round_timedelta(delta_t, 4 * hour)
            exp_timepoint = int(delta_t_4h.total_seconds() / hour)
            actual_timepoint = delta_t.total_seconds() / hour
            print '   %sh (%.1fh) @ %s' % (exp_timepoint, actual_timepoint,
                                           Path(*csvpath.components()[-4:]))
            seen_timepoints.append(exp_timepoint)
            # Specify 'str' as dtype to prevent any parsing of floats etc. to
            # preserve original values exactly.
            tempdf = pd.read_csv(csvpath, encoding='utf-8', dtype='str')
            tempdf = fixplateintegrity(tempdf)
            assert len(tempdf) == 240, ('Expected 240 rows, found %d'
                                        % len(tempdf))


            # Insert actual timepoint column.
            tempdf.insert(0, 'ActualTimepointHours', actual_timepoint)
            # Verify timestamp column matches file path.
            unique_mds = tempdf.MeasurementDate.unique()
            assert len(unique_mds) == 1, "multiple timestamps, expected one"
            data_timestamp = dateutil.parser.parse(tempdf.MeasurementDate[0])
            assert data_timestamp == t, "timestamp mismatch"

            # For sim1 and sim2:
            # Add replicate and timepoint columns.
            # Prepend experimental design and replicate number to Structural
            # Panel plates; Functional Panel plates will be merged later.
            if sim_num in (1, 2):

                pl = plate_df.copy()
                pl['ReplicateNumber'] = replicate
                pl['ExperimentalTimepointHours'] = exp_timepoint

                assert len(tempdf) == len(pl), "design-experiment mismatch"
                tempdf = pl.merge(tempdf, on=loc_columns)
                assert len(tempdf) == len(pl), "design merge failure"

            df[replicate][sim].append(tempdf)

        assert seen_timepoints == expected_tps, "wrong timepoint"


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

for replicate, rdf in df.items():

    print "Replicate", replicate

    data = []

    for panel_a, panel_b in (('Sim_000001', 'Sim_000003'),
                             ('Sim_000002', 'Sim_000004')):

        for tp, (df1, df2) in enumerate(zip(rdf[panel_a], rdf[panel_b])):

            print "    Timepoint %d: %s / %s" % (tp, panel_a, panel_b)
            df2.columns = [x + '_2' for x in df2.columns.values]
            tempdf = df1.merge(df2, left_on='WellName', right_on='WellName_2')
            assert len(tempdf) == len(df1) == len(df2), "panel length mismatch"
            tempdf = drop_duplicate_columns(tempdf)
            data.append(tempdf)
            # Trivially succeeds on first iteration, of course.
            assert (tempdf.columns == data[0].columns).all(), "column mismatch"

    final = data[0].append(data[1:])
    final = final.sort_values(['ExperimentalTimepointHours', 'PlateName'])
    final = final.reset_index(drop=True)
    assert final.shape[0] == 240*len(tp_map[replicate])*2, "wrong number of rows"
    assert final.shape[1] == 694, "wrong number of columns"

    final_path = output_path.child('Replicate_'+replicate+'.csv')
    print "    Writing output to", final_path
    with open(final_path, 'w') as fp:
        cols = final.columns
        cols = [non_ascii(col) for col in cols]
        final.columns = cols
        final.to_csv(fp, index=False)
