import pandas as pd
import os
import sqlite3
from sqlite3 import Error
import datetime
import numpy as np

def create_connection(db_file):
    """ create a database connection to a SQLite database """
    try:
        conn = sqlite3.connect(db_file)
        print(sqlite3.version)
    except Error as e:
        print(e)
    finally:
        conn.close()

if __name__ == '__main__':

    CREATE_DB=0
    CREATE_FLAT_ONLY=0

    print('init db:')
    engine = sqlite3.connect('.//db//' + "favorita.db", check_same_thread=False)
    engine.execute('PRAGMA auto_vacuum = FULL;')
    engine.commit()
    res = dict()

    result= pd.DataFrame()
    dir_list = [".//data//" +a for a in os.listdir('.//data') if a.endswith('.csv')]
    print('found csv files:',dir_list)

    # find top stores
    year_filter=2017
    ITEMS_FILTER=1
    STORES_FILTER=0
    STORES_MAX_NUMBER=10
    ITEMS_MAX_NUM=100

    data = pd.read_csv('.//data//train.csv', low_memory=False)
    data['date'] = pd.to_datetime(data['date'])
    data = data[data['date'].dt.year == year_filter]
    df = data.groupby(['store_nbr']).size().reset_index(name='counts')
    df = df.sort_values(by=['counts'], ascending=False)
    stores = df[0:STORES_FILTER]['store_nbr']
    df = data.groupby(['item_nbr']).size().reset_index(name='counts')
    df = df.sort_values(by=['counts'], ascending=False)
    items = df[0:ITEMS_MAX_NUM]['item_nbr']


    for file in dir_list:
        print(file)
        data=pd.read_csv(file,low_memory=False)
        name_t=file.split('/')[-1]
        table_name=name_t.split('.')[0]
        try :
            data['date'] = pd.to_datetime(data['date'])
            data = data[data['date'].dt.year == year_filter]
            if table_name in ['train','test','transactions']:
                if STORES_FILTER:
                    data_filtered = data[data['store_nbr'].isin(stores)]
                    #print(data_filtered)
                    data=data_filtered
                if ITEMS_FILTER:
                    data_filtered = data[data['item_nbr'].isin(items)]
                    #print(data_filtered)
                    data=data_filtered
            if table_name=='train':
                train_data=data
            if table_name=='test':
                test_data=data

            if table_name=='oil':
                oil_data=data
            if table_name=='holidays_events':
                holidays_data=data

            res[table_name] = data
        except:
            res[table_name] = data
            if table_name=='items':
                items_data=data
            if table_name=='stores':
                stores_data=data
            print('no date')

    #train_data.to_sql('flat_table0', con=engine, index=False, if_exists='append')

    train_data['oil_price'] = np.nan
    for index, row in oil_data.iterrows():
        curr_indx_t=train_data['date'] == row['date']
        train_data.loc[curr_indx_t, 'oil_price'] = row['dcoilwtico']
    #train_data.to_sql('flat_table1', con=engine, index=False, if_exists='append')


    train_data['holiday_type'] = ''
    train_data['holiday_locale'] = ''
    train_data['holiday_locale_name'] = ''
    train_data['holiday_description'] = ''
    train_data['holiday_transferred'] = np.nan
    for index, row in holidays_data.iterrows():
        curr_indx_t=train_data['date'] == row['date']
        train_data.loc[curr_indx_t, 'holiday_type'] = row['type']
        train_data.loc[curr_indx_t, 'holiday_locale'] = row['locale']
        train_data.loc[curr_indx_t, 'holiday_locale_name'] = row['locale_name']
        train_data.loc[curr_indx_t, 'holiday_description'] = row['description']
        train_data.loc[curr_indx_t, 'holiday_transferred'] = row['transferred']
    #train_data.to_sql('flat_table2', con=engine, index=False, if_exists='append')


    train_data['item_family'] = ''
    train_data['item_class'] = np.nan
    train_data['item_perishable'] = np.nan
    for index, row in items_data.iterrows():
        curr_indx_t=train_data['item_nbr'] == row['item_nbr']
        train_data.loc[curr_indx_t, 'item_family'] = row['family']
        train_data.loc[curr_indx_t, 'item_class'] = row['class']
        train_data.loc[curr_indx_t, 'item_perishable'] = row['perishable']
    #train_data.to_sql('flat_table3', con=engine, index=False, if_exists='append')


    train_data['store_city'] = ''
    train_data['store_state'] = ''
    train_data['store_type'] = ''
    train_data['store_cluster'] = np.nan
    for index, row in stores_data.iterrows():
        curr_indx_t=train_data['store_nbr'] == row['store_nbr']
        train_data.loc[curr_indx_t, 'store_city'] = row['city']
        train_data.loc[curr_indx_t, 'store_state'] = row['state']
        train_data.loc[curr_indx_t, 'store_type'] = row['type']
        train_data.loc[curr_indx_t, 'store_cluster'] = row['cluster']


    train_data['date_year'] = train_data.apply(lambda x: x['date'].year , axis=1)

    #train_data['date_month'] = train_data.apply(lambda x: x['date'].month, axis=1)
    #train_data['date_day'] = train_data.apply(lambda x: x['date'].day, axis=1)
    #train_data['date_season'] = train_data.apply(lambda x: (x['date_month'] % 12 + 3) // 3, axis=1)


    #season = train_data['date_month'].apply(lambda dt:(dt % 12 + 3) // 3)
    #train_data['date_season'] = np.nan


    print(train_data)
    df.to_sql(train_data, con=engine, index=False, if_exists='append')
    res['flat_table'] = train_data


    if CREATE_DB==1:
        for k, df in res.items():
            if len(df) == 0:
                continue
            if CREATE_FLAT_ONLY:
                print(k)
                if k=='flat_table':
                    df.to_sql(res['flat_table'], con=engine, index=False, if_exists='append')
                else:
                    continue
            print(k)
            df.to_sql(k, con=engine, index=False, if_exists='append')

        engine.close()

    #print(res)
