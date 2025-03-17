import requests
import pandas 
import json
import pyodbc
import datetime
#import numpy as np

url = 'http://localhost:5002'
header = {"Content-Type":"application/json",
          "Accept-Encoding":"deflate"}

response = requests.get(url,headers=header)
#print(response)

responseData= response.json()
#print(responseData)

#Clientacc = df['ClientAccount']
#print(Clientacc)
def convert(date_time):
    format = '%b %d %Y %I:%M%p'
    datetime_str = datetime.datetime.strptime(date_time, format)

    return datetime_str
  
df2 = pandas.json_normalize(responseData["RegulatoryCompliance"]['EU'])
df3 = pandas.json_normalize(responseData["RegulatoryCompliance"]['US'])
print(df3.columns)
print(df2.columns)
print("data_Types ",df2.dtypes)
df2['MIFID_II_COMPLIANCE_FLAG'] = df2['MiFID II Compliance'].astype(int)
df3['FINRA_COMPLIANCE_FLAG'] = df3['FINRA Compliance'].astype(int)
df3['SEC_REPORTING_FLAG'] = df3['SEC Reporting'].astype(int)
#print("new int:  ",df2['MIFID_II_COMPLIANCE_FLAG'][0])
        

df1 =  pandas.json_normalize(responseData)


print(df1.columns)
print("Counterparty: ",df1['Counterparty'][0])
print("CounterpartyLocation: ",df1['CounterpartyLocation'][0])
print("Currency: ",df1['Currency'][0])
print("InstrumentName",df1['InstrumentName'][0])
print("InstrumentType",df1['InstrumentType'][0]) 
print("Timestamp: ",df1['Timestamp'][0])
print("TransactionAmount",df1['TransactionAmount'][0])
print("TransactionID : ",df1['TransactionID'][0])
print("TransactionType: ",df1['TransactionType'][0])

df =  pandas.json_normalize(responseData["TransactionDetails"])


TRANSACTION_ID = df1['TransactionID'][0]
df['TRANSACTION_ID'] = TRANSACTION_ID
CREATE_DATE = datetime.datetime.fromisoformat(df1['Timestamp'][0])
df['CREATE_DATE'] = CREATE_DATE
print(df.columns)

SEC_REPORTING_FLAG = df3['SEC Reporting'].astype(int)
df1['SEC_REPORTING_FLAG'] = SEC_REPORTING_FLAG
MIFID_II_COMPLIANCE_FLAG = df2['MiFID II Compliance'].astype(int)
df1['MIFID_II_COMPLIANCE_FLAG'] = MIFID_II_COMPLIANCE_FLAG
FINRA_COMPLIANCE_FLAG = df3['FINRA Compliance'].astype(int)
df1['FINRA_COMPLIANCE_FLAG'] = FINRA_COMPLIANCE_FLAG



conn = pyodbc.connect(
    'Data Source Name=odbc_drivers_sql;'
    'Driver={SQL Server};'
    'Server=LAPTOP-LLBKM3AD\SQLEXPRESS;'
    'Database=EDWH;'
    'Trusted_connection=yes;'
)

cursor2 = conn.cursor()
for index in df.iterrows():
            cursor2.execute("INSERT INTO dbo.ODS_TRANSACTION_DETAIL_DATA(TRANSACTION_ID,CLIENTACCOUNT,EXECUTIONPRICE,ORDERPRICE,ORDERTYPE,ORDERQUANTITY,CREATE_DATE,ETL_DATE)"
                "values(?,?,?,?,?,?,?,?)", 
                    df['TRANSACTION_ID'][0],
                    df['ClientAccount'][0],
                    df['ExecutionPrice'][0],
                    int(df['OrderPrice'][0]),
                    df['OrderType'][0],
                    int(df['OrderQuantity'][0]),
                    df['CREATE_DATE'][0],
                    datetime.datetime.now()
                    
                    )
conn.commit()
cursor2.close()

cursor = conn.cursor()

for index in df1.iterrows():
     cursor.execute("INSERT INTO dbo.ODS_TRANSACTION_DATA(TRANSACTION_ID,TRANSACTION_TYPE,COUNTERPARTY,COUNTERPARTY_LOCATION,CURRENCY,INSTRUMENTNAME,INSTRUMENTTYPE,MIFID_II_COMPLIANCE_FLAG,FINRA_COMPLIANCE_FLAG,SEC_REPORTING_FLAG,TRANSACTION_AMOUNT,CREATE_DATE,ETL_DATE) "
     "values(?,?,?,?,?,?,?,?,?,?,?,?,?)", 
                    df1['TransactionID'][0],
                    df1['TransactionType'][0],
                    df1['Counterparty'][0],
                    df1['CounterpartyLocation'][0],
                    df1['Currency'][0],
                    df1['InstrumentName'][0],
                    df1['InstrumentType'][0],
                    int(df1['MIFID_II_COMPLIANCE_FLAG'][0]),
                    int(df1['FINRA_COMPLIANCE_FLAG'][0]),
                    int(df1['SEC_REPORTING_FLAG'][0]),
                    float(df1['TransactionAmount'][0]),
                    datetime.datetime.fromisoformat(df1['Timestamp'][0]),
                    datetime.datetime.now()
                    )
conn.commit()
cursor.close()




#df1.to_sql(name='Alper1',con=conn,if_exists='replace',index=False)






#df2 =  pandas.DataFrame(responseData)
#print(df2)

