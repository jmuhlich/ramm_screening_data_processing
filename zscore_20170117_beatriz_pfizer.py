import re
import pandas as pd
import unipath

PROJECT_NAME = '20170117_beatriz_pfizer_zscore'
input_path = unipath.Path(__file__).parent.child('input', PROJECT_NAME)
output_path = unipath.Path(__file__).parent.child('output', PROJECT_NAME)
assert input_path.exists()
output_path.mkdir()

def control_row_idx(df):
    # DMSO treatment is in many more wells than this, but it was decided
    # to only use cols 8-14, rows 5-6 (a more central part of the plate)
    # due to edge effects.
    return ((8 <= df.Column) & (df.Column <= 14)
            & (5 <= df.Row) & (df.Row <= 6))

csvpaths = [p for p in input_path.listdir() if p.ext == '.csv']

for path in csvpaths:

    print 'Reading', path
    rep = pd.read_csv(path, float_precision='high')

    data_col_idx = rep.columns.str.contains(r'^(?:Nuclei|dead cells|Spots)')
    assert len(data_col_idx.nonzero()[0]) == 660, "wrong # of columns"

    assert len(rep) == 240*9*2, "wrong # replicate rows"

    rep_data = rep.loc[:, data_col_idx]
    rep_controls = rep.loc[control_row_idx, data_col_idx]
    rep_controls_std = rep_controls.std()

    for meas_id in rep.MeasurementID.unique():

        print "    measurement %d" % meas_id

        pl_row_idx = (rep.MeasurementID == meas_id)
        assert len(pl_row_idx.nonzero()[0]) == 240, "wrong # plate rows"

        pl = rep.loc[pl_row_idx]
        pl_data = pl.loc[:, data_col_idx]
        pl_controls = pl.loc[control_row_idx, data_col_idx]
        pl_controls_mean = pl_controls.mean()

        pl_data = (pl_data - pl_controls_mean) / rep_controls_std

        rep.loc[pl_row_idx, data_col_idx] = pl_data

    rep_number = re.findall(r'_(\d)\.csv', path.name)[0]
    dest_name = 'Replicate_%s_zscore_norm.csv' % rep_number
    dest_path = output_path.child(dest_name)
    print 'Writing', dest_path
    with open(dest_path, 'w') as fp:
        rep.to_csv(fp, index=False)
