from collections import OrderedDict
import itertools
import pandas as pd
import pathlib2 as pathlib


PROJECT_NAME = '20170516_hptec_oat1_two_panel'

# List of duplicate analysis result files, to be skipped.
badfiles = {}

project_path = pathlib.Path(__file__).parent
input_path = project_path.joinpath('input', PROJECT_NAME)
output_path = project_path.joinpath('output', PROJECT_NAME)
assert input_path.exists()
output_path.mkdir(exist_ok=True)

# Synthesize experimental design (one row per plate).
exp_meta_table_1 = OrderedDict((
    ('CellType', ('HPTEC', 'OAT1')),
    ('BiologicalReplicate', (1, 2, 3)),
    ('TimePointHours', (0, 6, 12, 24, 48, 72)),
))
exp_meta_table_2 = OrderedDict((
    ('CellType', ('OAT1',)),
    ('BiologicalReplicate', exp_meta_table_1['BiologicalReplicate']),
    ('TimePointHours', (18,)),
))
exp_metadata = sorted(
    list(itertools.product(*exp_meta_table_1.values()))
    + list(itertools.product(*exp_meta_table_2.values()))
)
exp_design = pd.DataFrame(exp_metadata,
                          columns=exp_meta_table_1.keys())

# Load plate map.
plate_design_path = input_path.joinpath('1705015_4batch_Layout_Plates.xlsx')
plate_design = pd.read_excel(str(plate_design_path))

# Take cartesian product of experimental and plate design to produce one master
# design table. Pandas doesn't actually provide a cartesian product
# implementation, so we fake it by merging on a (temporary) column with just one
# value for all rows in both tables.
exp_design['_temp'] = 0
plate_design['_temp'] = 0
design = pd.merge(exp_design, plate_design, on='_temp').drop('_temp', axis=1)


# Sorting ensures we always iterate in the same order, as filesystem enumeration
# order may not be consistent across runs or between platforms. This is
# important as file order determines column order in the final DataFrame if some
# files have differing columns (see comment on file loading code below).
data_paths = sorted(p for p in input_path.glob('**/*.csv')
                    if p.name not in badfiles)

# Load data files into two DataFrames by antibody panel, preserving original
# column order. Earlier files take precedence in case of ordering conflicts
# between files, i.e. columns from later files may be reordered and separated if
# necessary.
data_struct = pd.DataFrame()
data_func = pd.DataFrame()
# We'll just use the keys in this OrderedDict as an ordered set.
#columns = OrderedDict()
for p in data_paths:
    new_data = pd.read_csv(str(p), float_precision='high',
                           parse_dates=['MeasurementDate'])
    assert len(new_data.PlateName.unique()) == 1, 'Multiple plates in file'
    assert new_data.PlateName.str.contains(r'Sim_00000[1-6]').all(), \
        'Bad plate name'
    sim_num = int(new_data.PlateName.iloc[0][-1])
    if sim_num in (1, 2, 3):
        new_data['BiologicalReplicate'] = sim_num
        data_struct = data_struct.append(new_data, ignore_index=True)
    elif sim_num in (4, 5, 6):
        new_data['BiologicalReplicate'] = sim_num - 3
        data_func = data_func.append(new_data, ignore_index=True)
    else:
        assert False, 'Unexpected plate (sim) number'
    #columns.update(OrderedDict.fromkeys(new_data.columns))

# Pull per-plate design variables out of ScreenName
cell_type = data_struct.ScreenName.str.extract(r'(HPTEC|OAT1)', expand=False)
cell_type.name = 'CellType'
data_struct = pd.concat([cell_type, data_struct], axis=1)

cell_type = data_func.ScreenName.str.extract(r'(HPTEC|OAT1)', expand=False)
cell_type.name = 'CellType'
data_func = pd.concat([cell_type, data_func], axis=1)

# Compute TimePointHours from timestamps. Get t0 for each plate and then round
# the time deltas for each readout to 6-hour increments.
for df in data_struct, data_func:
    time_info = df[['PlateID', 'MeasurementDate']].set_index('PlateID')
    t0 = df.groupby('PlateID').MeasurementDate.min()
    time_info['t0'] = t0
    hours = (time_info.MeasurementDate - time_info.t0).dt.round('6h') / pd.to_timedelta('1h')
    hours = hours.astype('int').reset_index(drop=True)
    df['TimePointHours'] = hours

#data = data.loc[:, columns.keys()]
# Row and Column are redundant with the design table; Timepoint is not useful
# (always 1, not the actual timepoint).
for df in data_struct, data_func:
    df.drop(['Row', 'Column', 'Timepoint'], axis=1, inplace=True)

# Merge design with structural panel data.
data = pd.merge(
    design, data_struct,
    on=['CellType', 'BiologicalReplicate', 'TimePointHours', 'WellName'],
    how='left', indicator=True
)

# Fill missing data from technical replicates.
missing = data[data._merge=='left_only'].reset_index()
mcols = ['CellType', 'BiologicalReplicate', 'TimePointHours', 'pert_iname',
         'pert_dose']
mm = pd.merge(data.reset_index(drop=True), missing[mcols + ['index']])
mm = mm[mm.ScreenName.notnull()].set_index('index')
fill_idx = pd.IndexSlice['ScreenName':]
data.loc[mm.index, fill_idx] = mm.loc[:, fill_idx]

# Merge with functional panel data.
data = pd.merge(data, data_func, on=['CellType', 'BiologicalReplicate',
                                     'TimePointHours', 'WellName'],
                suffixes=('_struc', '_func'))

# group_sizes = {
#     'ScreenID': (432, 360),
#     'PlateID': 72,
#     'MeasurementID': 72,
# }
# for column, size in group_sizes.items():
#     assert (data.groupby(column).size() == size).all(), \
#         "Expected groups by column '%s' to have size(s) %s" % (column, size)


# Sort rows and order columns in a meaningful order.
# sort_cols = ['CellType', 'TimePoint', 'Randomization', 'Column', 'Row',
#              'WellName']
# data.sort_values(sort_cols, inplace=True)
# sort_col_idx = data.columns.isin(sort_cols)
# # Move non-data columns to the front.
# data_col_idx = data.columns.str.startswith('Nuclei')
# other_col_idx = ~sort_col_idx & ~data_col_idx
# ordered_cols = (sort_cols + list(data.columns[other_col_idx])
#                 + list(data.columns[data_col_idx]))
# assert set(data.columns) == set(ordered_cols), "Missing columns in reorder"
# data = data.loc[:, ordered_cols]
# data.reset_index(drop=True, inplace=True)

# Build dataframe of numeric data, and also control subset, indexed by plate,
# biological replicate and cell type.
data_cols = data.columns[data.columns.str.contains(r'(?:Nuclei|dead|Spots)')]
num_data = data[list(data_cols) + ['MeasurementID_func', 'BiologicalReplicate',
                                   'CellType']].copy()
num_data['CellTypeAndReplicate'] = (
    num_data.CellType + '_' + num_data.BiologicalReplicate.astype('str')
)
control_data = num_data[data['pert_iname'] == '0.5_DMSO']
for df in num_data, control_data:
    df.set_index(['MeasurementID_func', 'CellTypeAndReplicate'], inplace=True)
# Compute control mean by plate and control standard deviation by
# celltype-replicate.  Note that the index of each dataframe is the index level
# we grouped by.
plate_mean = control_data.groupby(level='MeasurementID_func').mean()
cellrep_std = control_data.groupby(level='CellTypeAndReplicate').std()
# Compute z-score normalization.
num_data_norm = (num_data - plate_mean) / cellrep_std
# Copy data to data_norm and set normalized data values.
num_data_norm.index = data.index
data_norm = data.copy()
data_norm[data_cols] = num_data_norm[data_cols]

# Write source data
data.to_csv(str(output_path.joinpath('original.csv')),
            index=False, encoding='utf-8')

# Write normalized data
data_norm.to_csv(str(output_path.joinpath('zscore_norm.csv')),
                 index=False, encoding='utf-8')
