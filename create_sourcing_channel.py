#!/usr/bin/env python
import pandas as pd
import numpy as np

from database import Database
db = Database()


def createSourcingChannel():
    df = pd.read_csv('data.csv', usecols=['sourcing_channel'])
    df.dropna(inplace=True)
    sourcingChannels = df.as_matrix()

    sqls = np.array([])

    for sourcingChannelArray in sourcingChannels:
        sourcingChannel = sourcingChannelArray[0]
        sql = ''.join([
            'INSERT INTO public.sourcing_channel (channel) VALUES (', "'", sourcingChannel, "'", ');'])
        sqls = np.append(sqls, [sql])
        sqls = list(dict.fromkeys(sqls))

    db.insert(sqls)

    print('Sourcing Channels created.')
