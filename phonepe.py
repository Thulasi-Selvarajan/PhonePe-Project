# Importing necessary libraries

import os
import psycopg2
import json
import git
import pandas as pd

# Setting the path for aggregated transaction data

path1 = "C:/Users/Acer/Desktop/Python-V/PhonePe-Pro2/pulse/data/aggregated/transaction/country/india/state/"
agg_tran_list = os.listdir(path1)

# Creating an initial dictionary for aggregated transaction data

column1 ={
    "States":[], 
    "Years":[], 
    "Quarter":[], 
    "Transaction_type":[], 
    "Transaction_count":[],
    "Transaction_amount":[] 
}

# Looping through state folders, years, and files to extract data

for state in agg_tran_list:
    cur_states =path1+state+"/"
    agg_year_list = os.listdir(cur_states)
    
    for year in agg_year_list:
        cur_years = cur_states+year+"/"
        agg_file_list = os.listdir(cur_years)

        for file in agg_file_list:
            cur_files = cur_years+file
            data = open(cur_files,"r")
            A = json.load(data)

            for i in A["data"]["transactionData"]:
                name = i["name"]
                count = i["paymentInstruments"][0]["count"]
                amount = i["paymentInstruments"][0]["amount"]
                column1["Transaction_type"].append(name)
                column1["Transaction_count"].append(count)
                column1["Transaction_amount"].append(amount)
                column1["States"].append(state)
                column1["Years"].append(year)
                column1["Quarter"].append(int(file.strip(".json")))

# Creating a Pandas DataFrame for aggregated transaction data 
                               
aggre_transaction = pd.DataFrame(column1)

# Cleaning and standardizing state names in the DataFrame

aggre_transaction["States"] = aggre_transaction["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
aggre_transaction["States"] = aggre_transaction["States"].str.replace("-"," ")
aggre_transaction["States"] = aggre_transaction["States"].str.title()
aggre_transaction['States'] = aggre_transaction['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")

# Setting the path for aggregated user data
path2 = "C:/Users/Acer/Desktop/Python-V/PhonePe-Pro2/pulse/data/aggregated/user/country/india/state/"
agg_user_list = os.listdir(path2)

# Creating an initial dictionary for aggregated user data
column2 = {"States":[], "Years":[], "Quarter":[], "Brands":[],"Transaction_count":[], "Percentage":[]}

# Looping through state folders, years, and files to extract data
for state in agg_user_list:
    cur_states = path2+state+"/"
    agg_year_list = os.listdir(cur_states)
    
    for year in agg_year_list:
        cur_years = cur_states+year+"/"
        agg_file_list = os.listdir(cur_years)
        
        for file in agg_file_list:
            cur_files = cur_years+file
            data = open(cur_files,"r")
            B = json.load(data)

            try:

                for i in B["data"]["usersByDevice"]:
                    brand = i["brand"]
                    count = i["count"]
                    percentage = i["percentage"]
                    column2["Brands"].append(brand)
                    column2["Transaction_count"].append(count)
                    column2["Percentage"].append(percentage)
                    column2["States"].append(state)
                    column2["Years"].append(year)
                    column2["Quarter"].append(int(file.strip(".json")))
            
            except:
                pass

# Creating a Pandas DataFrame for aggregated user data
aggre_user = pd.DataFrame(column2)

# Cleaning and standardizing state names in the DataFrame
aggre_user["States"] = aggre_user["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
aggre_user["States"] = aggre_user["States"].str.replace("-"," ")
aggre_user["States"] = aggre_user["States"].str.title()
aggre_user['States'] = aggre_user['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")


# Setting the path for Map transaction data
path3 = "C:/Users/Acer/Desktop/Python-V/PhonePe-Pro2/pulse/data/map/transaction/hover/country/india/state/"
map_tran_list = os.listdir(path3)

# Creating an initial dictionary for Map transaction data
column3 = {"States":[], "Years":[], "Quarter":[],"District":[], "Transaction_count":[],"Transaction_amount":[]}

# Looping through state folders, years, and files to extract data
for state in map_tran_list:
    cur_states = path3+state+"/"
    map_year_list = os.listdir(cur_states)
    
    for year in map_year_list:
        cur_years = cur_states+year+"/"
        map_file_list = os.listdir(cur_years)
        
        for file in map_file_list:
            cur_files = cur_years+file
            data = open(cur_files,"r")
            C = json.load(data)

            for i in C['data']["hoverDataList"]:
                name = i["name"]
                count = i["metric"][0]["count"]
                amount = i["metric"][0]["amount"]
                column3["District"].append(name)
                column3["Transaction_count"].append(count)
                column3["Transaction_amount"].append(amount)
                column3["States"].append(state)
                column3["Years"].append(year)
                column3["Quarter"].append(int(file.strip(".json")))

# Creating a Pandas DataFrame for Map transaction data
map_transaction = pd.DataFrame(column3)

# Cleaning and standardizing state names in the DataFrame
map_transaction["States"] = map_transaction["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
map_transaction["States"] = map_transaction["States"].str.replace("-"," ")
map_transaction["States"] = map_transaction["States"].str.title()
map_transaction['States'] = map_transaction['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")

                
# Setting the path for Map user data
path4 = "C:/Users/Acer/Desktop/Python-V/PhonePe-Pro2/pulse/data/map/user/hover/country/india/state/"
map_user_list = os.listdir(path4)

# Creating an initial dictionary for Map user data
column4 = {"States":[], "Years":[], "Quarter":[], "Districts":[], "RegisteredUser":[], "AppOpens":[]}

# Looping through state folders, years, and files to extract data
for state in map_user_list:
    cur_states = path4+state+"/"
    map_year_list = os.listdir(cur_states)
    
    for year in map_year_list:
        cur_years = cur_states+year+"/"
        map_file_list = os.listdir(cur_years)
        
        for file in map_file_list:
            cur_files = cur_years+file
            data = open(cur_files,"r")
            D = json.load(data)

            for i in D["data"]["hoverData"].items():
                district = i[0]
                registereduser = i[1]["registeredUsers"]
                appopens = i[1]["appOpens"]
                column4["Districts"].append(district)
                column4["RegisteredUser"].append(registereduser)
                column4["AppOpens"].append(appopens)
                column4["States"].append(state)
                column4["Years"].append(year)
                column4["Quarter"].append(int(file.strip(".json")))

# Creating a Pandas DataFrame for Map user data
map_user = pd.DataFrame(column4)

# Cleaning and standardizing state names in the DataFrame
map_user["States"] = map_user["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
map_user["States"] = map_user["States"].str.replace("-"," ")
map_user["States"] = map_user["States"].str.title()
map_user['States'] = map_user['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")

   

# Setting the path for Top transaction data
path5 = "C:/Users/Acer/Desktop/Python-V/PhonePe-Pro2/pulse/data/top/transaction/country/india/state/"
top_tran_list = os.listdir(path5)

# Creating an initial dictionary for Top transaction data
column5 = {"States":[], "Years":[], "Quarter":[], "Pincodes":[], "Transaction_count":[], "Transaction_amount":[]}

# Looping through state folders, years, and files to extract data
for state in top_tran_list:
    cur_states = path5+state+"/"
    top_year_list = os.listdir(cur_states)
    
    for year in top_year_list:
        cur_years = cur_states+year+"/"
        top_file_list = os.listdir(cur_years)
        
        for file in top_file_list:
            cur_files = cur_years+file
            data = open(cur_files,"r")
            E = json.load(data)

            for i in E["data"]["pincodes"]:
                entityName = i["entityName"]
                count = i["metric"]["count"]
                amount = i["metric"]["amount"]
                column5["Pincodes"].append(entityName)
                column5["Transaction_count"].append(count)
                column5["Transaction_amount"].append(amount)
                column5["States"].append(state)
                column5["Years"].append(year)
                column5["Quarter"].append(int(file.strip(".json")))

# Creating a Pandas DataFrame for Top transaction data
top_transaction = pd.DataFrame(column5)

# Cleaning and standardizing state names in the DataFrame
top_transaction["States"] = top_transaction["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
top_transaction["States"] = top_transaction["States"].str.replace("-"," ")
top_transaction["States"] = top_transaction["States"].str.title()
top_transaction['States'] = top_transaction['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")


# Setting the path for Top user data
path6 = "C:/Users/Acer/Desktop/Python-V/PhonePe-Pro2/pulse/data/top/user/country/india/state/"
top_user_list = os.listdir(path6)

# Creating an initial dictionary for Top user data
column6 = {"States":[], "Years":[], "Quarter":[], "Pincodes":[], "RegisteredUser":[]}

# Looping through state folders, years, and files to extract data
for state in top_user_list:
    cur_states = path6+state+"/"
    top_year_list = os.listdir(cur_states)

    for year in top_year_list:
        cur_years = cur_states+year+"/"
        top_file_list = os.listdir(cur_years)

        for file in top_file_list:
            cur_files = cur_years+file
            data = open(cur_files,"r")
            F = json.load(data)

            for i in F["data"]["pincodes"]:
                name = i["name"]
                registeredusers = i["registeredUsers"]
                column6["Pincodes"].append(name)
                column6["RegisteredUser"].append(registereduser)
                column6["States"].append(state)
                column6["Years"].append(year)
                column6["Quarter"].append(int(file.strip(".json")))

# Creating a Pandas DataFrame for Top user data
top_user = pd.DataFrame(column6)

# Cleaning and standardizing state names in the DataFrame
top_user["States"] = top_user["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
top_user["States"] = top_user["States"].str.replace("-"," ")
top_user["States"] = top_user["States"].str.title()
top_user['States'] = top_user['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")

# PostgreSQL Database Connection
mydb = psycopg2.connect(host="localhost",
                        user="postgres",
                        password="2022",
                        database="phonePe",
                        port="5432"
                        )
cursor = mydb.cursor()

#Creating a aggregated transaction table and inserting data
create_query1 = '''CREATE TABLE if not exists aggregated_transaction (States varchar(50),
                                                                      Years int,
                                                                      Quarter int,
                                                                      Transaction_type varchar(50),
                                                                      Transaction_count bigint,
                                                                      Transaction_amount bigint
                                                                      )'''
cursor.execute(create_query1)
mydb.commit()

for index,row in aggre_transaction.iterrows():
    insert_query1 = '''INSERT INTO aggregated_transaction (States, Years, Quarter, Transaction_type, Transaction_count, Transaction_amount)
                                                        values(%s,%s,%s,%s,%s,%s)'''
    values = (row["States"],
              row["Years"],
              row["Quarter"],
              row["Transaction_type"],
              row["Transaction_count"],
              row["Transaction_amount"]
              )
    cursor.execute(insert_query1,values)
    mydb.commit()

#creating a aggregated user table and inserting data
create_query2 = '''CREATE TABLE if not exists aggregated_user (States varchar(50),
                                                                Years int,
                                                                Quarter int,
                                                                Brands varchar(50),
                                                                Transaction_count bigint,
                                                                Percentage float)'''
cursor.execute(create_query2)
mydb.commit()

for index,row in aggre_user.iterrows():
    insert_query2 = '''INSERT INTO aggregated_user (States, Years, Quarter, Brands, Transaction_count, Percentage)
                                                    values(%s,%s,%s,%s,%s,%s)'''
    values = (row["States"],
              row["Years"],
              row["Quarter"],
              row["Brands"],
              row["Transaction_count"],
              row["Percentage"])
    cursor.execute(insert_query2,values)
    mydb.commit()

#Creating a map_transaction_table and inserting the data
create_query3 = '''CREATE TABLE if not exists map_transaction (States varchar(50),
                                                                Years int,
                                                                Quarter int,
                                                                District varchar(50),
                                                                Transaction_count bigint,
                                                                Transaction_amount float)'''
cursor.execute(create_query3)
mydb.commit()

for index,row in map_transaction.iterrows():
            insert_query3 = '''
                INSERT INTO map_Transaction (States, Years, Quarter, District, Transaction_count, Transaction_amount)
                VALUES (%s, %s, %s, %s, %s, %s)

            '''
            values = (
                row['States'],
                row['Years'],
                row['Quarter'],
                row['District'],
                row['Transaction_count'],
                row['Transaction_amount']
            )
            cursor.execute(insert_query3,values)
            mydb.commit() 


#creating a map_user_table and inserting data
create_query4 = '''CREATE TABLE if not exists map_user (States varchar(50),
                                                        Years int,
                                                        Quarter int,
                                                        Districts varchar(50),
                                                        RegisteredUser bigint,
                                                        AppOpens bigint)'''
cursor.execute(create_query4)
mydb.commit()

for index,row in map_user.iterrows():
    insert_query4 = '''INSERT INTO map_user (States, Years, Quarter, Districts, RegisteredUser, AppOpens)
                        values(%s,%s,%s,%s,%s,%s)'''
    values = (row["States"],
              row["Years"],
              row["Quarter"],
              row["Districts"],
              row["RegisteredUser"],
              row["AppOpens"])
    cursor.execute(insert_query4,values)
    mydb.commit()

#creating a top_transaction_table and inserting data
create_query5 = '''CREATE TABLE if not exists top_transaction (States varchar(50),
                                                                Years int,
                                                                Quarter int,
                                                                pincodes int,
                                                                Transaction_count bigint,
                                                                Transaction_amount bigint)'''
cursor.execute(create_query5)
mydb.commit()

for index,row in top_transaction.iterrows():
    insert_query5 = '''INSERT INTO top_transaction (States, Years, Quarter, Pincodes, Transaction_count, Transaction_amount)
                                                    values(%s,%s,%s,%s,%s,%s)'''
    values = (row["States"],
              row["Years"],
              row["Quarter"],
              row["Pincodes"],
              row["Transaction_count"],
              row["Transaction_amount"])
    cursor.execute(insert_query5,values)
    mydb.commit()

#creating a top_user_table and inserting data
create_query6 = '''CREATE TABLE if not exists top_user (States varchar(50),
                                                        Years int,
                                                        Quarter int,
                                                        Pincodes int,
                                                        RegisteredUser bigint
                                                        )'''
cursor.execute(create_query6)
mydb.commit()

for index,row in top_user.iterrows():
    insert_query6 = '''INSERT INTO top_user (States, Years, Quarter, Pincodes, RegisteredUser)
                                            values(%s,%s,%s,%s,%s)'''
    values = (row["States"],
              row["Years"],
              row["Quarter"],
              row["Pincodes"],
              row["RegisteredUser"])
    cursor.execute(insert_query6,values)
    mydb.commit()