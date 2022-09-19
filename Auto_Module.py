#!/usr/bin/env python
# coding: utf-8

# Importing CSV files into postgres database

#Project Steps:
# - importing a csv file into a dattaframe
# - Cleaning the the data table name 
# - Cleaning the column headers and the data set 
# - Creating a table with SQL
# - Importing the cleaned data into a cloud database

#Importing all the Libraries



#importing libraries
import os
import pandas as pd
import numpy as np
import psycopg2 as ps
import re
import shutil


#Find CSV files in the Directory
# Steps:
# - Find all CSV files in the current directory
# - Isolate only the CSV files
# - Shift the CSV files into a new directory


#get working directory

def csv_files():
    csv_files = []
    for fname in os.listdir(os.getcwd()):
        if fname.endswith('.csv'):
            csv_files.append(fname)
    return csv_files


def config_data_dir(csv_files,data_dir):
    #make directory
    mkdir = '{0}'.format(data_dir)
    os.makedirs(mkdir, exist_ok = True)
    
    #Move csv files to the dir
    for file in csv_files:
        original = r'{0}'.format(file)
        target = r'{0}'.format(data_dir)
        shutil.move(original, target)    
        print(file)

    return 


# Importing the CSV file into database 
# Importing multiple CSV files to go through all the data cleaning process

def create_df(csv_files,data_dir):
    #path to csv files
    dir_path = os.getcwd()+'/'+data_dir+'/'
    #loops through files and creates a dataframe
    df = {}
    for file in csv_files:
        try:
            df[file] = pd.read_csv(dir_path+file)
        except UnicodeDecodeError:
            df[file] = pd.read_csv(dir_path+file , encoding="ISO-8859-1")
            
    return df


def clean_table(fname):
    
    
    Clean_tname = fname.lower().replace("-","_").replace(" ","_")
    replace_spl = re.sub('[^a-zA-Z0-9-_\n\.]', '',Clean_tname)
    fname = replace_spl
 
    table_name = '{0}'.format(fname.split('.')[0])    
    
    return table_name


def clean_columns_and_dataset(dataframe):
    
    df1 = dataframe.copy()

    df1.columns = [q.lower().replace(" ","_") for q in df1]

    df1.columns = [(re.sub(r'[^a-zA-Z0-9_\n\.]', r"", x)) for x in df1.columns]

    columns = df1.columns
 

    df1['year_of_release'] = df1['year_of_release'].apply(lambda row: re.sub("[^0-9–]", "", str(row) ))

  

    df1[['year_of_release', 'year_ended']] = df1['year_of_release'].str.split('–', expand=True)
    df1.fillna("",inplace=True)



    df1['year_ended'] = df1['year_ended'].replace(r'^\s*$', "Present", regex=True)
    df1['certificate_rating'] = df1['certificate_rating'].replace(r'^\s*$', "Not Rated", regex=True)
    df1['review'] = df1['review'].replace(r'^\s*$', "none", regex=True)

    return columns,df1





def map_datatype(df1):
    
  
    new_dtypes  = { 'object' : 'varchar', 
                    'float64': 'varchar',
                    'int64'  : 'varchar'
                  }
    
    d_cols = ", ".join("{} {}".format(c,d) for (c,d) in zip(df1.columns,df1.dtypes.replace(new_dtypes)))

    return d_cols





def database_upload(host,db ,username,password,table_name,d_cols,columns,fname ,dataframe):

    connect = "host = %s dbname = %s user =%s password = %s" % (host,db ,username,password)
    try:
        conn = ps.connect(connect)

    except ps.OperationalError as e:
        raise e
    else:
        print('Connected!')
      

    conn.autocommit = True
    cur = conn.cursor()
    
 
    cur.execute("drop table if exists %s;" %(table_name))

    cur.execute ("create table %s (%s); " % (table_name, d_cols))
    print ('{0} created successfully'.format(table_name))
    
    #insert values

    #save df into csv
    dataframe.to_csv(fname,header =dataframe.columns,index= False,encoding='utf-8' )


    #opne the csv file, save it as an object and upload it into db
    files = open(fname)
    print("file opened!")

    #upload it into db
    sql_query = """ copy %s from STDIN with
                    CSV
                    HEADER
                    DELIMITER as ',' """

    cur.copy_expert(sql = sql_query % table_name , file =files)
    print("file copied!")

    cur.close()
    print("{0} table import to database completed".format(table_name))
    

