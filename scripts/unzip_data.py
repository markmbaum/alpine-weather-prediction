from os import walk, mkdir
from os.path import join, isdir
import zipfile

#------------------------------------------------------------------------------

dirin = join('..', 'data', 'raw', 'wegenernet', '2022')

#------------------------------------------------------------------------------

for root, _, fns in walk(dirin):
    for fn in fns:
        if ('BD' in fn) and (fn[-4:] == '.zip'):
            year = fn[19:23]
            d = join(root, year)
            if not isdir(d):
                mkdir(d)
            with zipfile.ZipFile(join(root, fn), 'r') as zf:
                zf.extractall(d)
