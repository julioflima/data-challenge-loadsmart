#!/usr/bin/env python

from database import connect

import pandas as pd
import numpy as np


def createShipper():
    df = pd.read_csv('data.csv', usecols=['shipper_name'])

    shippersIds = df.as_matrix()

    print()
    sqls = np.array([])

    for shipperString in shippersIds:
        shipperId = shipperString[0].split(' ')[1]
        sql = ''.join([
            'INSERT INTO public.shipper (source_id) VALUES (', shipperId, ');'])
        sqls = np.append(sqls, [sql])
        sqls = list(dict.fromkeys(sqls))

    connect(sqls)

    print('Shipper created.')
