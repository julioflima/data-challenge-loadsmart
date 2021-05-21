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

        for deliveriesArray in deliveries:
            loadsmartId = deliveriesArray[0]
            # ----------------------------------------------------------

            fromId = deliveriesArray[1]
            # ----------------------------------------------------------

            toId = deliveriesArray[0]
            # ----------------------------------------------------------

            quoteDate = deliveriesArray[0]
            bookDate = deliveriesArray[0]
            sourceDate = deliveriesArray[0]
            pickupDate = deliveriesArray[0]
            deliveryDate = deliveriesArray[0]
            bookPrice = deliveriesArray[0]
            sourcePrice = deliveriesArray[0]
            pnl = deliveriesArray[0]
            mileage = deliveriesArray[0]
            # ----------------------------------------------------------

            carrierId = deliveriesArray[0]
            # ----------------------------------------------------------

            shipperId = deliveriesArray[0]
            # ----------------------------------------------------------

            carrierOnTimeToPickup = deliveriesArray[0]
            carrierOnTimeToDelivery = deliveriesArray[0]
            carrierOnTimeOverall = deliveriesArray[0]
            pickupAppointmentTime = deliveriesArray[0]
            deliveryAppointmentTime = deliveriesArray[0]
            hasMobileAppTracking = deliveriesArray[0]
            hasMacropointTracking = deliveriesArray[0]
            hasEdiTracking = deliveriesArray[0]
            contractedLoad = deliveriesArray[0]
            loadBookedAutonomously = deliveriesArray[0]
            loadSourcedAutonomously = deliveriesArray[0]
            loadWasCancelled = deliveriesArray[0]

            sql = ''.join([
                'INSERT INTO public.deliver (loadsmart_id,from_id,to_id,quote_date,book_date,source_date,pickup_date,delivery_date,book_price,source_price,pnl,mileage,carrier_id,shipper_id,carrier_on_time_to_pickup,carrier_on_time_to_delivery,carrier_on_time_overall,pickup_appointment_time,delivery_appointment_time,has_mobile_app_tracking,has_macropoint_tracking,has_edi_tracking,contracted_load,load_booked_autonomously,load_sourced_autonomously,load_was_cancelled) VALUES (',
                "", sourceId, "", ",",
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
