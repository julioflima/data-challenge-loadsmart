#!/usr/bin/env python
import pandas as pd
import numpy as np

from database import Database
db = Database()


def createEquipment():
    df = pd.read_csv('data.csv', usecols=['equipment_type'])
    equipmentsTypes = df.as_matrix()

    sqls = np.array([])

    for equipmentTypeArray in equipmentsTypes:
        equipmentType = equipmentTypeArray[0]
        sql = ''.join([
            'INSERT INTO public.equipment ("type") VALUES (', "'", equipmentType, "'", ');'])
        sqls = np.append(sqls, [sql])
        sqls = list(dict.fromkeys(sqls))

    db.insert(sqls)

    print('Equipments created.')
