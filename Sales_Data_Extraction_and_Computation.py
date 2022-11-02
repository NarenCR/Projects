import pandas as pd
from sqlalchemy import create_engine
import pymongo
import logging as lg
import mysql.connector as canct
lg.basicConfig(filename="Sales_Data_Output.log",level = lg.DEBUG,format='%(message)s',filemode='w')
lg.info('MySQL and Pandas are used to solve these task given on July 24th')
lg.info('Reading the given files')

try:
    df1 = pd.read_excel('D:\FSDS\Resouces\Pandas\data fsds -20221005T113210Z-001\data fsds\Attribute DataSet.xlsx')
    df2 = pd.read_excel('D:\FSDS\Resouces\Pandas\data fsds -20221005T113210Z-001\data fsds\Dress Sales.xlsx')
except Exception as e:
    lg.error(e)
    lg.error("Please Give Valid Input")
# print(df1)
lg.info('Connecting to SQL database')

try:
    eng = create_engine('mysql+pymysql://Naren:Naren8485@localhost/dress_sales')
    conc = eng.connect()
    lg.info("connection established ")
except Exception as el:
    lg.error("please check the displayed error ",el)

lg.info("Loading attributes and sales dataset to SQL")
lg.info(df1.head())
lg.info(df2.head())
df1.to_sql('attributes', con=conc, if_exists='replace', index=False)
df2.to_sql('sales', con=conc, if_exists='replace', index=False)

lg.info("reading attributes and sales data set from SQL")
df3 = pd.read_sql(sql='select * from attributes', con=conc)
df4 = pd.read_sql(sql='select * from sales', con=conc)
lg.info(df3.head())
lg.info(df4.head())

lg.info("Converting dataset attributes to JSON format")
try:
    df6 = df3.to_json('D:\FSDS\Resouces\Pandas\data fsds -20221005T113210Z-001\data fsds\Attribute_j.json')
    lg.info("JSON Formated Attributes dataset\n")
    lg.info(df6)
    print(df6)
except Exception as ek:
    lg.error("please check weather file name already exists or already open")
    lg.error('Also check the error message displayed below\n',ek)
# print(df3,'\n', df4)

lg.info("Connecting to MongoDB to insert Attributes dataset in JSON format")
try:
    client = pymongo.MongoClient("mongodb+srv://narencr:Naren8485@naren-fsds.emagwow.mongodb.net/?retryWrites=true&w=majority")
    db = client.test
    db_task  = client['Mysql_pandas']
    c = db_task['Dress_Attributes']
    c.insert_many(df6)
    lg.info("Succesfully uploaded data to MongoDB")
except Exception as ex:
    print(ex)
    print('Check connection details')
# I am not sure about the issue here connection is not establish with MongoDB

lg.info('Joining attributes and sales dataset with respect to Dress_ID')
df5 = pd.read_sql(sql='select * from attributes left join sales on attributes.Dress_ID = sales.Dress_ID', con=conc)
df5.to_sql('attribute_sales_combined', con=conc, if_exists='replace', index=False)
df5.to_excel('D:\FSDS\Resouces\Pandas\data fsds -20221005T113210Z-001\data fsds\Attribute_Sales_combined.xlsx')
lg.info("Attributes and Sales data set is combined and stored as Attribute_Sales_combined.xlsx")
lg.info("Combined dataset\n")
lg.info(df5.head())
# print(df5)
# print(df3.Dress_ID.nunique(dropna = True))

lg.info("Finding No.of Unique Dress_ID")
Unique_Dress_ID_Attributes = pd.read_sql(sql='select count(distinct Dress_ID) from attributes', con=conc)
# Unique_Dress_ID_Sales = pd.read_sql(sql = 'select count(distinct Dress_ID) from sales', con = conc)
# Unique_Dress_ID_Combined = pd.read_sql(sql = 'select count(distinct Dress_ID) from attribute_sales_combined', con = conc)
lg.info("No.of unique Dress_ID: ")
lg.info(Unique_Dress_ID_Attributes)


# print(Unique_Dress_ID_Sales)
# print(Unique_Dress_ID_Combined) ''' Combined is having 550 rows because of non unique data being added twice'''

lg.info("Finding Dress_ID with Reccomendation = 0")
Dress_ID_reccom_0 = pd.read_sql(sql='select count(Dress_ID) from attributes where Recommendation = 0', con=conc)
lg.info("Total No. of Dress_ID with recomendation = 0 are ")
lg.info(Dress_ID_reccom_0)

lg.info("Finding Total sales per Dress_ID")
l = list(df2.columns)
l.pop(0)
# print(l)
df4['Total'] = df2[l].sum(axis=1).astype('int')
df4.to_sql('sales', con=conc, if_exists='replace', index=False)
lg.info("New sales dataset with Total sales per Dress_ID\n")
lg.info(df4.head())

lg.info("Finding 3rd Highest Dress by Total Sales")
Dress_ID_3rd_highest = pd.read_sql(sql='select Dress_ID, Total from sales e1 where 2=(select count(distinct Total) from sales e2 where e2.Total> e1.Total)', con=conc)
lg.info("3rd Highest dress sales is:\n")
lg.info(Dress_ID_3rd_highest)

lg.info("End of Task")
