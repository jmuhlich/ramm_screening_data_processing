import os
import glob
import re
import itertools
import collections
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
	        newdf = pd.concat([df.iloc[0:indx+1], df.iloc[indx:]], axis=0)
 	        newdf.index = range(0,240)

            else:
	        #newrow = df.iloc[indx-1]
	        newdf = pd.concat([df.iloc[0:indx], df.iloc[indx-1:]], axis=0)
 	        newdf.index = range(0,240)

            print 'Fixed plate', newdf.shape
            return newdf
    return df

def subdirs(path):
    return [p for p in path.listdir() if p.isdir()]

PROJECT_NAME = '210'

input_path = Path(__file__).parent.child('input', PROJECT_NAME)
output_path = Path(__file__).parent.child('output', PROJECT_NAME)
assert input_path.exists()
output_path.mkdir()

immediate_subdirs = subdirs(input_path)

df = {}
plate_df = []

platefilelist = ['Plate layout_1-18.csv', 'Plate Layout_19-38.csv',
                 'Plate layout_39-57.csv', 'Plate Layout_58-73.csv']
for pl_name in platefilelist:
    pl_path = input_path.child(pl_name)
    # Specify 'str' as dtype to prevent any parsing of floats etc. to preserve
    # original values exactly.
    plate_df.append(pd.read_csv(pl_path, comment='#', dtype='str'))

for subdir in immediate_subdirs:
    if re.search('1-38', subdir):
        # 1-38
        chemrange = '1-38'
    elif re.search('39-73', subdir):
        # 39-73
        chemrange = '39-73'
    replica = re.findall('(R.)', subdir)
    replica = replica[0]

    replicaname = str(replica)

    if replicaname not in df:
        df[replicaname] = {}

    # navigate down subdir, go to each Sim directory
    immediate_subdirs_2 = subdirs(subdir)

    for subdir2 in immediate_subdirs_2:
        sim = re.findall('(Sim_00000.)', subdir2)
        sim = sim[0]

        print 'Found', subdir, 'over', subdir2
        print 'R', replica, 'Sim', sim,

        if sim not in df[replicaname]:
          df[replicaname][sim] = []

        # Num is used to find the right plate layout file

        # Go through files

        csvpaths = [p for p in subdir2.listdir() if p.ext == '.csv']

        for csvpath in csvpaths:

            exptime = re.findall('T(\d\d\d\d)', csvpath)
            exptime = exptime[0]
            print 'csvfile', csvpath, 'extacted exptime', exptime
            # Specify 'str' as dtype to prevent any parsing of floats etc. to
            # preserve original values exactly.
            tempdf = pd.read_csv(csvpath, dtype='str')
            tempdf = fixplateintegrity(tempdf)

            # check file integrity

            #For sim1 or sim2
            #Add a time column
            #Add a replica column, but ignore the sim, we will join sim1 and 3 and sim will not be needed
            print tempdf.shape

            if sim == 'Sim_000001' or sim == 'Sim_000002':

                #For sim1
                if chemrange == '1-38':
                    if sim == 'Sim_000001':
                        pl = plate_df[0]
                    if sim == 'Sim_000002':
                        pl = plate_df[1]
                if chemrange == '39-73':
                    if sim == 'Sim_000001':
                        pl = plate_df[2]
                    if sim == 'Sim_000002':
                        pl = plate_df[3]

                wt = pd.DataFrame([str(exptime) for i in range(0, 240)],
                                  index=tempdf.index, columns=['WallTime'])
                tempdf = pd.concat([wt, tempdf], axis=1)
                rp = pd.DataFrame([replicaname for i in range(0, 240)],
                                  index=tempdf.index, columns=['ReplicaNumber'])
                tempdf = pd.concat([rp, tempdf], axis=1)
                tempdf = pd.concat([pl, tempdf], axis=1)

            print 'Adding to df with %s and %s' % (replicaname, sim), 'Shape', tempdf.shape
            df[replicaname][sim].append(tempdf)

#Ok all read in, now go through each replica (1 to 4)
#For each sim1 item, join it to corresponding sim3 item, this is replica X, compounds Y-Z, 7 time points, full set of struct and func features

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


finaldf = {}
collist = []

altcollist = []
with open(input_path.child('rownames.txt'),'r') as fp:
    for f in fp:
        altcollist.append(f.strip())

for replica in df:

    print 'Checking replica %s' % replica
    print 'Length', len(df[replica]['Sim_000001'])

    for df1,df2 in zip(df[replica]['Sim_000001'],df[replica]['Sim_000003']):

        print 'Adding for set 1-38 with shapes', df1.shape, df2.shape
        df2.columns = [x + '_2' for x in df2.columns.values]
        tempdf = pd.concat([df1, df2], axis=1)
        tempdf = drop_duplicate_columns(tempdf)

        if 'set 1-38' not in df[replica]:
            df[replica]['set 1-38'] = tempdf
        else:
            print 'Pre drop shape', tempdf.shape
            #print 'Predrop exist of column NAF ',tempdf['Number of Analyzed Fields'].values.shape
            print 'Joining shapes', df[replica]['set 1-38'].shape, tempdf.shape
            #print 'New index ',tempdf.columns
            #print 'Postdrop exist of column NAF ',tempdf['Number of Analyzed Fields'].values.shape
            collist.append(tempdf.columns)
            #print zip(df[replica]['set 1-38'].columns, tempdf.columns)
            df[replica]['set 1-38'] = df[replica]['set 1-38'].append(tempdf)

    for df1,df2 in zip(df[replica]['Sim_000002'],df[replica]['Sim_000004']):
        print 'Adding for set 39-73'
        df2.columns = [x + '_2' for x in df2.columns.values]
        tempdf = pd.concat([df1, df2], axis=1)
        tempdf = drop_duplicate_columns(tempdf)
        if 'set 39-73' not in df[replica]:
            df[replica]['set 39-73'] = tempdf
        else:
            print 'Pre drop shape', tempdf.shape
            print 'Joining shapes', df[replica]['set 39-73'].shape, tempdf.shape
            df[replica]['set 39-73'] = df[replica]['set 39-73'].append(tempdf)

    finaldf[replica] = pd.concat([df[replica]['set 1-38'], df[replica]['set 39-73']], axis=0)
    finaldf[replica].index = range(0, len(finaldf[replica]))

    with open(output_path.child('set_'+replica+'.csv'), 'w') as fp:
        cols = finaldf[replica].columns
        cols = [non_ascii(col) for col in cols]
        finaldf[replica].columns = cols
        finaldf[replica] = finaldf[replica][altcollist]
        finaldf[replica].to_csv(fp)
        fp.write('# Raw data with all replicates, light processing replace blank wells with their technical replicates\n')
