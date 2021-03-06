#!/usr/bin/env python

import psycopg2
from config import config


class Database:
    def insert(self, sqls):
        """ Connect to the PostgreSQL database server """
        conn = None
        try:
            # read connection parameters
            params = config()

            # connect to the PostgreSQL server
            print('Connecting to the PostgreSQL database...')
            conn = psycopg2.connect(**params)

            # create a cursor
            cur = conn.cursor()

            # execute a statement
            for sql in sqls:
                print('Executing...', sql)
                cur.execute(sql)

            conn.commit()
            print("Record inserted successfully in table.")

            # close the communication with the PostgreSQL
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print('Failed to insert record into table.', error)
        finally:
            if conn is not None:
                conn.close()
                print('Database connection closed.')

    def select(self, sql):
        """ Connect to the PostgreSQL database server """
        conn = None
        try:
            # read connection parameters
            params = config()

            # connect to the PostgreSQL server
            print('Connecting to the PostgreSQL database...')
            conn = psycopg2.connect(**params)

            # create a cursor
            cur = conn.cursor()

        # execute a statement
            print('PostgreSQL database version:')
            cur.execute(sql)

        # display the PostgreSQL result.
            result = cur.fetchone()
            print(result)

        # close the communication with the PostgreSQL
            cur.close()

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
                print('Database connection closed.')
                return result
