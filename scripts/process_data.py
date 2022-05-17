from os import walk
from os.path import join
from pandas import read_csv, to_datetime, concat
from numpy import *
from multiprocessing import Pool

#------------------------------------------------------------------------------

#input directory to walk
dirin = join('..', 'data', 'raw', 'wegenernet', '2022')

#number of processes/cpus to use
nproc = 7

#columns to keep
cols = [
    'Station',
    'Time [YYYY-MM-DD HH:MM:SS]',
    'Relative humidity [%]',
    'Precipitation [mm]',
    'Air temperature [degC]',
]

#new columns names for those that are kept
rcols = [
    'station',
    'datetime',
    'RH',
    'P',
    'T'
]

#output file
dirout = join('..', 'data', 'pro')

#info for splitting the datetime column
ts = [
    ('yr', slice(0,4), int16),
    ('mon', slice(5,7), int8),
    ('day', slice(8,10), int8),
    ('hr', slice(11,13), int8),
    ('min', slice(14,16), int8),
    ('sec', slice(17,19), int8)
]

#------------------------------------------------------------------------------

def process_table(path):
    print("processing file:", path)
    df = read_csv(path)
    #select and rename columns
    df = df[cols]
    df.columns = rcols
    #convert station number to short integer format (all are <156)
    df.station = df.station.astype(int16)
    #split datetime
    for (c,s,t) in ts:
        df[c] = [t(x[s]) for x in df.datetime]
    df.drop('datetime', axis=1, inplace=True)
    #ensure no off-minute records
    assert all(df.sec == 0)
    df.drop('sec', axis=1, inplace=True)
    #reduce float precision of meteorolgical variables
    for c in rcols[-3:]:
        df[c] = df[c].astype(float32)
    return df

#------------------------------------------------------------------------------

if __name__ == '__main__':

    #assemble target file paths
    paths = []
    for root, _, fns in walk(dirin):
        for fn in fns:
            #must be a "basis data" csv
            if ('BD' in fn) and (fn[-4:] == '.csv'):
                #make sure station number is <500
                if int(fn[21:24]) <= 155:
                    paths.append(join(root, fn))
    print(len(paths), 'target files found')

    #start a pool of workers and process tables in parallel
    pool = Pool(processes=nproc)
    tasks = [pool.apply_async(process_table, (path,)) for path in paths]
    weg = [task.get() for task in tasks]

    #combine and write columns individually
    print('concatenating...')
    weg = concat(weg, axis=0, ignore_index=True)
    print(weg)
    for col in weg:
        fn = join(dirout, col + '_' + str(weg[col].dtype))
        weg[col].values.tofile(fn)
        print('file written:', fn)