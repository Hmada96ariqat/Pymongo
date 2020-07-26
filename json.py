#!/usr/bin/env python
# coding: utf-8

# In[20]:


#Importing all necessary libraries 
import pymongo
import json
from pymongo import MongoClient 
import requests
from requests.exceptions import ConnectionError
import difflib
from diff_match_patch import diff_match_patch as diff


# In[55]:


#Connection string
cluster = MongoClient("mongodb+srv://dbHmada:dbpassword@cluster0.otifb.mongodb.net/test?retryWrites=true&w=majority")
db = cluster['cname']
collection1=db.cnameinfo1
collection2=db.cnameinfo2


# In[59]:


#Handling the network disconnection
try:
    r = requests.get("https://cloud.mongodb.com/v2/5f1436c891d82d3a499a2e43#metrics/replicaSet/5f14382dfe3a7c6fed738f52/explorer/cname")
    r.raise_for_status()
except requests.exceptions.RequestException as err:
    print ("OOps: RequestException",err)
except requests.exceptions.HTTPError as errH:
    print ("Http Error:",errH)
except requests.exceptions.ConnectionError as errC:
    print ("Error Connecting:",errC)
except requests.exceptions.Timeout as errT:
    print ("Timeout Error:",errT) 
print(r.url)


# In[10]:


# #creating all the necessary collections.
######WARNING#######
#this slice of code isn't working!
# cluster = MongoClient("mongodb+srv://dbHmada:dbpassword@cluster0.otifb.mongodb.net/test?retryWrites=true&w=majority")
# db = cluster['cname']
# collection1=db.cnameinfo1
# collection2=db.cnameinfo2
# with open('instances_train.json',"r",  encoding='UTF-8') as json_file:
#     data = json.load(json_file)
#     collections = data.keys()
#     print(collections)
#     for collection in collections:
#         print(f"Inserting data info {collection} collection")
#         curr_coll = cluster.createCollection(collection) # dynamically creating collections
#         curr_coll.insertMany(data[collection])
#         print(f"DONE! Inserting data info {collection} collection")
#######################################################################################################################


# In[16]:


#Reading json files and put them into the MongoDB
with open("instances_train.json", "r",  encoding='UTF-8') as read_it1: 
    data1 = json.load(read_it1)
    collection1.insert_one(data1)
    print(data1)
with open("instances_val.json", "r",  encoding='UTF-8') as read_it2:
    data2 = json.load(read_it2) 
    collection2.insert_one(data2)
    print(data2)
#Check if the files are similar \~^Bool^~/
sorted(data1.items()) == sorted(data2.items())


# In[17]:


#Creating all necessary collections by splitting each Dict to another 
##Data1
for key,value in data1.items():
        print(key)
        print(value)
##Data2
for key,value in data2.items():
        print(key)
        print(value)


# In[7]:


#Removing rhe duplicates from each json file
#First_file
with open('instances_train.json' , "r", encoding = 'UTF-8') as f1:
    d1 = json.load(f1)
uniques = {x[0]: x for x in d1}
with open(r'\Users\hmada\OneDrive\Desktop\WORK\boundlessview\json_files\Duplicates_removed_1.json' ,'w' , encoding = 'UTF-8') as nf1:
    json.dump(uniques, nf1)
#Second_file
with open('instances_val.json' , "r", encoding = 'UTF-8') as f2:
    d2 = json.load(f2)
uniques = {x[0]: x for x in d2}
with open(r'\Users\hmada\OneDrive\Desktop\WORK\boundlessview\json_files\Duplicates_removed_2.json' ,'w' , encoding = 'UTF-8') as nf2:
    json.dump(uniques, nf2)


# In[8]:


sorted(data1.items()) == sorted(data2.items())


# In[9]:


#to index the needed coloumn and easily recall
db.cnameinfo1.create_index([('id', pymongo.ASCENDING)])


# In[21]:


#To know the differences between the records in json and the db collection
file1 = r'instances_train.json'
file2 = r'instances_val.json'
file1_lines= open(file1, encoding = 'UTF-8').readlines()
file2_lines= open(file2, encoding = 'UTF-8').readlines()
difference = difflib.HtmlDiff().make_file(file1_lines,file2_lines,file1,file2)
differences_report = open(r'\Users\hmada\OneDrive\Desktop\WORK\boundlessview\json_files\differences_report.html', "w",  encoding = 'UTF-8')
differences_report.write(difference)
differences_report.close()


# # Blocks that might help for feedback!

# In[60]:


# for key in data1:
#         data1[key]
#         print("The key and value are ({}) = ({})".format(key, value))
#         for i in range (1):
#             if key == i :
#                 with open("instances_train.json", "r",  encoding='UTF-8') as read_it1: 
#                     data1 = json.load(read_it1)
#                     collection1.insert_one(key)
#             print(key)


# In[25]:


#Creating collection and save in it a Dict
# collection1 = db["fields"]
# mydict ={'name': 'ar', 'id': 2}
# x = collection1.insert_one(mydict)


# In[ ]:




