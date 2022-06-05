from os import walk, listdir, remove
from os.path import join
from pandas import read_csv, to_datetime, concat
from numpy import *
from scipy.signal import detrend
from multiprocessing import Pool

#------------------------------------------------------------------------------

#input directory to walk
dirin = join('..', 'data', 'raw', 'wegenernet', '2022')

#number of processes/cpus to use
nproc = 4

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
    'timestamp',
    'RH',
    'P',
    'T'
]

#output file
dirout = join('..', 'data', 'pro')

#info for splitting the datetime column
ts = [
    ('year', slice(0,4), int16),
    ('month', slice(5,7), int8),
    ('day', slice(8,10), int8),
    ('hour', slice(11,13), int8),
    ('minute', slice(14,16), int8),
    ('second', slice(17,19), int8)
]

#time to reference from
t0 = to_datetime('2007-01-01')

#------------------------------------------------------------------------------

def process_table(path):
    print("processing file:", path)
    df = read_csv(path)
    #select and rename columns
    df = df[cols]
    df.columns = rcols
    #convert station number to short integer format (all are <156)
    df.station = df.station.astype(int16)
    #convert datetime
    df.timestamp = to_datetime(df.timestamp)
    df['time'] = (df.timestamp - t0).apply(lambda x: int32(x.total_seconds())//60)
    df.drop('timestamp', axis=1, inplace=True)
    #reduce float precision of meteorolgical variables
    for c in rcols[-3:]:
        df[c] = df[c].astype(float32)
    return df

def detrend_temperature(df):
    #make sure of proper sorting
    df.sort_values('time', inplace=True)
    #detrend temperature record, preserving the mean
    df['T'] = df['T'].mean() + detrend(df['T'].values)
    #return the same df
    print('station {} detrended'.format(df.station.iat[0]))
    return(df)

#------------------------------------------------------------------------------

if __name__ == '__main__':

    #start up the process pool
    pool = Pool(processes=nproc)

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

    #clear the output directory
    for fn in listdir(dirout):
        remove(join(dirout, fn))
        print("removed file:", join(dirout, fn))

    #process tables in parallel
    tasks = [pool.apply_async(process_table, (path,)) for path in paths]
    weg = [task.get() for task in tasks]

    #combine
    print('concatenating...')
    weg = concat(weg, axis=0, ignore_index=True)
    print(weg)

    #detrend each station's temperature signal
    print('detrending temperature records...')
    weg = [x[1] for x in weg.groupby('station')]
    tasks = [pool.apply_async(detrend_temperature, (df,)) for df in weg]
    weg = concat([task.get() for task in tasks], axis=0, ignore_index=True)
    print(weg)

    #write a single binary format table to file
    p = join(dirout, 'wegenernet.feather')
    weg.to_feather(p)
    print('file written:', p)
    pool.close()
