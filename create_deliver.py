#!/usr/bin/env python
import numpy as np
import pandas as pd

import psycopg2
from config import config


def createDeliver():
    conn = None

    try:
        print('Connecting to the PostgreSQL database...')
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()

        df = pd.read_csv('data.csv', usecols=[
            'loadsmart_id',
            'lane',
            'quote_date',
            'book_date',
            'source_date',
            'pickup_date',
            'delivery_date',
            'book_price',
            'source_price',
            'pnl',
            'mileage',
            'carrier_name',
            'shipper_name',
            'carrier_on_time_to_pickup',
            'carrier_on_time_to_delivery',
            'carrier_on_time_overall',
            'pickup_appointment_time',
            'delivery_appointment_time',
            'has_mobile_app_tracking',
            'has_macropoint_tracking',
            'has_edi_tracking',
            'contracted_load',
            'load_booked_autonomously',
            'load_sourced_autonomously',
            'load_was_cancelled'
        ])
        deliveries = df.as_matrix()

        sqls = np.array([])

        deliveries = deliveries[0:10]

        for deliveriesArray in deliveries:
            loadsmartId = str(deliveriesArray[0])
            # ----------------------------------------------------------

            addressesPair = deliveriesArray[1].split(' -> ')
            # ----------------------------------------------------------

            cityFrom = addressesPair[0].split(',')[0]

            sqlFromId = ''.join([
                'SELECT ad.id from address ad where ad.city = ', "'", cityFrom, "'", ';'])

            cur.execute(sqlFromId)

            fromId = cur.fetchone()[0]
            # ----------------------------------------------------------
            cityTo = addressesPair[1].split(',')[0]

            sqlToId = ''.join([
                'SELECT ad.id from address ad where ad.city = ', "'", cityTo, "'", ';'])

            cur.execute(sqlToId)

            toId = cur.fetchone()[0]
            # ----------------------------------------------------------

            quoteDate = deliveriesArray[2]
            bookDate = deliveriesArray[3]
            sourceDate = deliveriesArray[4]
            pickupDate = deliveriesArray[5]
            deliveryDate = deliveriesArray[6]
            bookPrice = str(deliveriesArray[7])
            sourcePrice = str(deliveriesArray[8])
            pnl = str(deliveriesArray[9])
            mileage = str(deliveriesArray[10])
            # ----------------------------------------------------------

            carrierSourceId = deliveriesArray[11].split(' ')[1]

            sqlCarrierId = ''.join([
                'SELECT ca.id from carrier ca where ca.source_id = ', "", carrierSourceId, "", ';'])

            cur.execute(sqlCarrierId)
            carrierId = cur.fetchone()[0]
            # ----------------------------------------------------------

            shipperSourceId = deliveriesArray[12].split(' ')[1]

            sqlShipperId = ''.join([
                'SELECT sp.id from shipper sp where sp.source_id = ', "", shipperSourceId, "", ';'])

            cur.execute(sqlShipperId)
            shipperId = cur.fetchone()[0]
            # ----------------------------------------------------------

            carrierOnTimeToPickup = deliveriesArray[13]
            carrierOnTimeToDelivery = deliveriesArray[14]
            carrierOnTimeOverall = deliveriesArray[15]
            pickupAppointmentTime = deliveriesArray[16]
            deliveryAppointmentTime = deliveriesArray[17]
            hasMobileAppTracking = deliveriesArray[18]
            hasMacropointTracking = deliveriesArray[19]
            hasEdiTracking = deliveriesArray[20]
            contractedLoad = deliveriesArray[21]
            loadBookedAutonomously = deliveriesArray[22]
            loadSourcedAutonomously = deliveriesArray[23]
            loadWasCancelled = deliveriesArray[24]

            print(carrierOnTimeToPickup)
# ,carrier_on_time_to_pickup,carrier_on_time_to_delivery,carrier_on_time_overall,pickup_appointment_time,delivery_appointment_time,has_mobile_app_tracking,has_macropoint_tracking,has_edi_tracking,contracted_load,load_booked_autonomously,load_sourced_autonomously,load_was_cancelled
            sql = ''.join([
                'INSERT INTO public.deliver (loadsmart_id,from_id,to_id,quote_date,book_date,source_date,pickup_date,delivery_date,book_price,source_price,pnl,mileage,carrier_id,shipper_id,carrier_on_time_to_pickup) VALUES (',
                "'", loadsmartId, "'", ",",
                "'", fromId, "'", ",",
                "'", toId, "'", ",",
                "'", quoteDate, "'", ",",
                "'", bookDate, "'", ",",
                "'", sourceDate, "'", ",",
                "'", pickupDate, "'", ",",
                "'", deliveryDate, "'", ",",
                "'", bookPrice, "'", ",",
                "'", sourcePrice, "'", ",",
                "'", pnl, "'", ",",
                "'", mileage, "'", ",",
                "'", carrierId, "'", ",",
                "'", shipperId, "'", ",",
                "'", carrierOnTimeToPickup, "'",
                # "'", carrierOnTimeToDelivery, "'::boolean",
                # "'", carrierOnTimeOverall, "'::boolean",
                # "'", pickupAppointmentTime, "'::timestamtz",
                # "'", deliveryAppointmentTime, "'::timestamtz",
                # "'", hasMobileAppTracking, "'::boolean",
                # "'", hasMacropointTracking, "'::boolean",
                # "'", hasEdiTracking, "'::boolean",
                # "'", contractedLoad, "'::boolean",
                # "'", loadBookedAutonomously, "'::boolean",
                # "'", loadSourcedAutonomously, "'::boolean",
                # "'", loadWasCancelled, "'::boolean",
                ');'])

            print(sql)

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

    print('Delivers created.')
