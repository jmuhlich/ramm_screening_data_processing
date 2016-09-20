import re
import pandas as pd
import unipath

PROJECT_NAME = 'az_zscore'
input_path = unipath.Path(__file__).parent.child('input', PROJECT_NAME)
output_path = unipath.Path(__file__).parent.child('output', PROJECT_NAME)
assert input_path.exists()
output_path.mkdir()

csvpaths = [p for p in input_path.listdir() if p.ext == '.csv']

for path in csvpaths:

    print 'Reading', path
    df = pd.read_csv(path, float_precision='high')

    data_col_selector = df.columns.str.contains(r'^(?:Nuclei|dead cells|Spots)')
    assert len(data_col_selector.nonzero()[0]) == 660, "wrong # of columns"

    rep = df[df.ExperimentalTimepointHours <= 24].copy()
    assert len(rep) == 3360, "wrong # replicate rows"

    rep_data = rep.loc[:, data_col_selector]
    rep_controls = rep_data[((rep.Column==21) | (rep.Column==22))]
    rep_controls_std = rep_controls.std()

    for meas_id in rep.MeasurementID.unique():

        print "    measurement %d" % meas_id

        pl_row_selector = (rep.MeasurementID == meas_id)
        assert len(pl_row_selector.nonzero()[0]) == 240, "wrong # plate rows"

        pl = rep.loc[pl_row_selector]
        pl_data = pl.loc[:, data_col_selector]
        pl_controls = pl_data[((pl.Column==21) | (pl.Column==22))]
        pl_controls_mean = pl_controls.mean()

        pl_data = (pl_data - pl_controls_mean) / rep_controls_std

        rep.loc[pl_row_selector, data_col_selector] = pl_data

    replicate = re.findall(r'_(\d)\.csv', path.name)[0]
    dest_name = 'Replicate_%s_zscore_norm.csv' % replicate
    dest_path = output_path.child(dest_name)
    print 'Writing', dest_path
    with open(dest_path, 'w') as fp:
        rep.to_csv(fp, index=False)
