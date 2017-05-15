from collections import OrderedDict
import pandas as pd
import pathlib2 as pathlib


PROJECT_NAME = '20170506_hptec_oat1_randomized'

# List of duplicate analysis result files, to be skipped.
badfiles = {}

project_path = pathlib.Path(__file__).parent
input_path = project_path.joinpath('input', PROJECT_NAME)
output_path = project_path.joinpath('output', PROJECT_NAME)
assert input_path.exists()
output_path.mkdir(exist_ok=True)

# Load experimental design.
design_path = input_path.joinpath('Layout_plates.xlsx')
single_designs = [pd.read_excel(str(design_path), ws) for ws in range(3)]
for i, name in enumerate(('ABC', 'BCA', 'CAB')):
    single_designs[i]['Randomization'] = name
design = pd.concat(single_designs)
design.rename(columns={'Well Name': 'WellName'}, inplace=True)
design.columns = design.columns.str.strip()

# Sorting ensures we always iterate in the same order, as filesystem enumeration
# order may not be consistent across runs or between platforms. This is
# important as file order determines column order in the final DataFrame if some
# files have differing columns (see comment on file loading code below).
data_paths = sorted(p for p in input_path.glob('**/*.csv')
                    if p.name not in badfiles)

# Load all data files into a single DataFrame, preserving original column order.
# Earlier files take precedence in case of ordering conflicts between files,
# i.e. columns from later files may be reordered and separated if necessary.
data = pd.DataFrame()
# We'll just use the keys in this OrderedDict as an ordered set.
columns = OrderedDict()
for p in data_paths:
    new_data = pd.read_csv(str(p), float_precision='high')
    data = data.append(new_data, ignore_index=True)
    columns.update(OrderedDict.fromkeys(new_data.columns))
data = data.loc[:, columns.keys()]
# Row and Column are redundant with the design table; Timepoint is not useful
# (always 1, not the actual timepoint).
data.drop(['Row', 'Column', 'Timepoint'], axis=1, inplace=True)

group_sizes = {
    'ScreenID': (432, 360),
    'PlateID': 72,
    'MeasurementID': 72,
}
for column, size in group_sizes.items():
    assert (data.groupby(column).size() == size).all(), \
        "Expected groups by column '%s' to have size(s) %s" % (column, size)

# Pull per-plate design variables out of PlateName
plate_design = data.PlateName.str.split('_').apply(pd.Series)
plate_design.columns = ['CellType', 'TimePoint', 'Randomization']
# Make capitalization consistent.
plate_design.TimePoint = plate_design.TimePoint.str.lower()
data = pd.concat([plate_design, data], axis=1)

# Merge design with data.
data = pd.merge(design, data, on=('Randomization', 'WellName'), how='outer',
                indicator=True)
assert (data._merge == 'both').all(), "Design-data merge is incomplete"
data.drop('_merge', axis=1, inplace=True)

# Sort rows and order columns in a meaningful order.
sort_cols = ['CellType', 'TimePoint', 'Randomization', 'Column', 'Row',
             'WellName']
data.sort_values(sort_cols, inplace=True)
sort_col_idx = data.columns.isin(sort_cols)
# Move non-data columns to the front.
data_col_idx = data.columns.str.startswith('Nuclei')
other_col_idx = ~sort_col_idx & ~data_col_idx
ordered_cols = (sort_cols + list(data.columns[other_col_idx])
                + list(data.columns[data_col_idx]))
assert set(data.columns) == set(ordered_cols), "Missing columns in reorder"
data = data.loc[:, ordered_cols]
data.reset_index(drop=True, inplace=True)

# Build dataframe of numeric data, and also control subset, indexed by plate and
# cell type.
data_cols = data.columns[data.columns.str.startswith('Nuclei')]
num_data = data[list(data_cols) + ['MeasurementID', 'CellType']]
control_data = num_data[data['Compound'] == '0.5% DMSO']
for df in num_data, control_data:
    df.set_index(['MeasurementID', 'CellType'], inplace=True)
# Compute control mean by plate and control standard deviation by cell type.
# Note that the index of each dataframe is the index level we grouped by.
plate_mean = control_data.groupby(level='MeasurementID').mean()
cell_std = control_data.groupby(level='CellType').std()
# Compute z-score normalization.
num_data_norm = (num_data - plate_mean) / cell_std
# Copy data to data_norm and set normalized data values.
num_data_norm.index = data.index
data_norm = data.copy()
data_norm[data_cols] = num_data_norm

# Write source data
for cell_type, subset in data.groupby('CellType'):
    dest_path = output_path.joinpath('%s.csv' % cell_type)
    subset = subset.loc[:, subset.notnull().all()]
    subset.to_csv(str(dest_path), index=False, encoding='utf-8')

# Write normalized data
for cell_type, subset in data_norm.groupby('CellType'):
    dest_path = output_path.joinpath('%s_zscore_norm.csv' % cell_type)
    subset = subset.loc[:, subset.notnull().all()]
    subset.to_csv(str(dest_path), index=False, encoding='utf-8')
