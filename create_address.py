#!/usr/bin/env python
import pandas as pd
import numpy as np

from database import Database
db = Database()


def createAddress():
    df = pd.read_csv('data.csv', usecols=['lane'])
    df.dropna(inplace=True)

    addresses = df.as_matrix()

    sqls = np.array([])

    for addressesPairArray in addresses:
        addressesPair = addressesPairArray[0].split(' -> ')

        for cityState in addressesPair:
            pair = cityState.split(',')
            sql = ''.join([
                'INSERT INTO public.address (city,state) VALUES (', "'", pair[0], "'", ",", "'", pair[1], "'", ');'])
            print(sql)
            sqls = np.append(sqls, [sql])

        sqls = list(dict.fromkeys(sqls))

    db.insert(sqls)

    print('Addresses created.')
