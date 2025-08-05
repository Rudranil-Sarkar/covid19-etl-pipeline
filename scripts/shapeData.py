#!/usr/bin/env python3

import datetime
import pandas as pd
from getData import get_data

def shapeData():

    data = get_data()
    df = pd.json_normalize(data)

    selected_columns = df[['country', 'cases', 'deaths', 'recovered', 'updated']]

    updated_utc = df['updated'].apply(lambda x: datetime.datetime.fromtimestamp(x / 1000, datetime.UTC))
    updated_utc.name = 'updated_utc'

    new_df = pd.concat([selected_columns, updated_utc], axis=1)

    new_df['pipeline_run_date'] = datetime.datetime.now(datetime.UTC)

    # drop the old updated series
    final_shaped_data = new_df.drop('updated', axis=1)

    return final_shaped_data
