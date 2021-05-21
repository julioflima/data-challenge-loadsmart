#!/usr/bin/env python
import numpy as np
import pandas as pd

import psycopg2
from config import config


def createCarrier():
    conn = None

    try:
        print('Connecting to the PostgreSQL database...')
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()

        df = pd.read_csv('data.csv', usecols=['equipment_type',	'carrier_rating',
                                              'sourcing_channel',	'vip_carrier',	'carrier_dropped_us_count', 'carrier_name'])
        df.dropna(subset=['carrier_name'], inplace=True)
        carriers = df.as_matrix()

        sqls = np.array([])

        for carriesArray in carriers:
            cur = conn.cursor()

            sourceId = carriesArray[5].split(' ')[1]
        # ----------------------------------------------------------

            sqlEquipmentId = ''.join([
                'SELECT eq.id from equipment eq where eq.type = ', "'", carriesArray[0], "'", ';'])

            cur.execute(sqlEquipmentId)

            equipmentId = cur.fetchone()[0]
        # ----------------------------------------------------------

            def carrierRatingIsNan(carrierRating):
                if carrierRating == carrierRating:
                    return str(carrierRating)
                return '0.0'

            carrierRating = carrierRatingIsNan(carriesArray[1])
        # ----------------------------------------------------------

            def sourcingChannelIsNan(channelId):
                if channelId == channelId:
                    sqlChannelId = ''.join([
                        'SELECT sc.id from sourcing_channel sc where sc.channel = ', "'", carriesArray[2], "'", ';'])

                    cur.execute(sqlChannelId)

                    return ''.join(["'", cur.fetchone()[0], "'"])
                return 'null'

            channelId = sourcingChannelIsNan(carriesArray[2])
        # ----------------------------------------------------------

            vipCarrier = str(carriesArray[3])
        # ----------------------------------------------------------

            carrierDroppedUsCount = str(carriesArray[4])
        # ----------------------------------------------------------

            sql = ''.join([
                'INSERT INTO public.carrier (source_id,equipment_id,carrier_rating,sourcing_channel_id,vip_carrier,carrier_dropped_us_count) VALUES (',
                "", sourceId, "", ",",
                "'", equipmentId, "'", ",",
                "", carrierRating, "", ",",
                "", channelId, "", ",",
                "'", vipCarrier, "'::boolean", ",",
                "", carrierDroppedUsCount, "",
                ');'])

            sqls = np.append(sqls, [sql])
            sqls = list(dict.fromkeys(sqls))

        for sql in sqls:
            cur.execute(sql)

        conn.commit()
        print("Record inserted successfully in table.")

        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print('Database failed.', error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

    print('Carriers created.')
