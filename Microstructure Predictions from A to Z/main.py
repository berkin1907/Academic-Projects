import math
import pandas as pd
from pandas import read_parquet
from glob import glob
import os
from tqdm import tqdm
import csv


def readFile(path):
    data = read_parquet(path)
    data['ReceiveTime'] = data['ReceiveTime'].map(lambda x: math.floor(x))
    data.sort_index(ascending=True)
    data.drop_duplicates(subset=['ReceiveTime'], keep='first', inplace=True)
    return data


def dfcat(paths):
    ret = readFile(paths[0])
    for path in paths[1:]:
        data = readFile(path)
        ret = pd.concat([ret, data])
    return ret


def classify(ls):
    d = {}
    for l in ls:
        k = os.path.split(l)[-1].split('#')[0]
        if k in d.keys():
            d[k].append(l)
        else:
            d[k] = [l]
    return d


def process(ls):
    s = set()
    for l in ls:
        s.add('_'.join(os.path.split(l)[-1].split('_')[:3]))
    return s


workdir = r'./gupiao' 
folders = classify(glob(os.path.join(workdir, '*')))
#folders = {'BTCBUSD': ['../BTCBUSD#F']}
for k, adirs in folders.items():
    if os.path.exists(k + '.csv'):
        os.remove(k + '.csv')
    f = open(k + '.csv', 'a+', newline='')
    writer = csv.writer(f)
    writer.writerow(['ReceiveTime_A', 'EventTime_B', 'price', 'percent'])

    ret = None
    for adir in adirs:
        files = glob(os.path.join(adir, '*'))
        _times = process(files)
        for _time in _times:
            filesA = glob(os.path.join(adir, _time + '*ticker*.parquet'))
            filesB = glob(os.path.join(adir, _time + '*depth*.parquet'))
            if len(filesB) == 0 or len(filesA) == 0:
                continue
            dataA = dfcat(filesA)
            dataB = dfcat(filesB)
            dataA['NewTime'] = dataA['ReceiveTime'] + 10000
            testdata = pd.merge(dataA, dataB, how='inner', left_on=['NewTime'], right_on=['EventTime'],
                                suffixes=('_A', '_B'))
            pdata = testdata.loc[:, ('ReceiveTime_A', 'EventTime_B', 'BidPx0', 'AskPx0')]
            pdata['price'] = (pdata['BidPx0'] + pdata['AskPx0']) * 0.5
            pdata = pdata.drop(['BidPx0', 'AskPx0'], axis=1)
            last = None
            for index, row in tqdm(pdata.iterrows()):
                if last == None:
                    last = row['price']
                    continue
                pdata.loc[index, 'percent'] = str((row['price'] - last) * 100 / last) + '%'
                last = row['price']
                writer.writerow([row['ReceiveTime_A'], row['EventTime_B'],row['price'], str((row['price'] - last) * 100 / last) + '%'])
