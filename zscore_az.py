import re
import pandas as pd
import unipath

PROJECT_NAME = 'az_zscore'
input_path = unipath.Path(__file__).parent.child('input', PROJECT_NAME)
output_path = unipath.Path(__file__).parent.child('output', PROJECT_NAME)
assert input_path.exists()
output_path.mkdir()

def control_row_idx(df):
    # Originally the controls were designed to be all of columns 21 and 22, but
    # due to possible edge effects this was changed to column 21 and the top
    # half of columns 11 and 13.
    #
    # return (df.Column == 21) | (df.Column == 22)
    return (
        (df.Column == 21)
        | (((df.Column == 11) | (df.Column == 13)) & (df.Row <= 8))
    )

csvpaths = [p for p in input_path.listdir() if p.ext == '.csv']

for path in csvpaths:

    print 'Reading', path
    rep = pd.read_csv(path, float_precision='high')

    data_col_idx = rep.columns.str.contains(r'^(?:Nuclei|dead cells|Spots)')
    assert len(data_col_idx.nonzero()[0]) == 660, "wrong # of columns"

    rep = rep[rep.ExperimentalTimepointHours <= 24].copy()
    assert len(rep) == 3360, "wrong # replicate rows"

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
