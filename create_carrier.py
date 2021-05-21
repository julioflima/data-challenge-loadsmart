#!/usr/bin/env python
import numpy as np
import pandas as pd

from database import Database
db = Database()


def createCarrier():
    # df = pd.read_csv('data.csv', usecols=['sourcing_channel'])
    # df.dropna(inplace=True)
    # sourcingChannels = df.as_matrix()

    # sqls = np.array([])

    # for sourcingChannelArray in sourcingChannels:
    #     sourcingChannel = sourcingChannelArray[0]
    #     sql = ''.join([
    #         'INSERT INTO public.sourcing_channel (channel) VALUES (', "'", sourcingChannel, "'", ');'])
    #     sqls = np.append(sqls, [sql])
    #     sqls = list(dict.fromkeys(sqls))

    db.select()

    print('Sourcing Channels created.')
