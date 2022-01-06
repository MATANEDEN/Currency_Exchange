import os
from connection_to_db import Database
from currencyplayer_py import MakeRequest
import pandas as pd
from dotenv import load_dotenv

#connect and get request from API
def connect_to_currencyplayer():
    params = {'access_key': os.getenv("api_access_key"), 'date': os.getenv("Date"),
              'currencies': os.getenv("Currencies"), 'format': os.getenv("Format")}
    answer_from_api = MakeRequest(params)
    return answer_from_api.request()


def  create_sql_table(conn):
    #sql query to create the table foramt
    postgres_insert_query = """     
     CREATE TABLE IF NOT EXISTS public.csv_table
    (
    "Date_" date NOT NULL,
    "Bill_ID" integer NOT NULL,
    "Currency" character varying COLLATE pg_catalog."default" NOT NULL,
    "Name" character varying COLLATE pg_catalog."default" NOT NULL,
    "Product1 revenue" numeric NOT NULL,
    "Product2 revenue" numeric NOT NULL,
    CONSTRAINT csv_table_pkey PRIMARY KEY ("Bill_ID")
    )     """

    #implement the query
    db1.connection.execute(postgres_insert_query)
    #commit(save changes)
    db1.session.commit()

def connect_to_db(file_path,db1):

    create_sql_table(db1.connection) #create empty table function
    currency_exchange_dict =  connect_to_currencyplayer()
    df = pd.read_csv(file_path )   #read csv table to pandas dataframe

    for index, row in df.iterrows(): #For loop on each row of the table

         if row['Currency'] != 'USD': #Get the currencies that we need to exchange

             #the currency that we want to change
             currency_type = 'USD'+row['Currency']
             #Divide by the currency value to reach the amount in dollars for "Product1 revenue" column
             df.at[index,'Product1 revenue'] /= currency_exchange_dict[currency_type]

             df.at[index,'Product1 revenue'] = round(df.at[index,'Product1 revenue'],2)

             df.at[index, 'Product2 revenue'] /= currency_exchange_dict[currency_type]

             df.at[index, 'Currency'] = 'USD'

    try:
        df.to_sql(os.getenv("TABLE_NAME"), db1.engine, if_exists='append', index=False)
    except:
        print("Cant inert rows with the same PK (Bill_ID)")


if __name__ == '__main__':
    load_dotenv()
    file_path = os.getenv("CSV_PATH")
    db1 = Database()
    db1.connect()
    connect_to_db(file_path,db1)
    db1.close_connection()

