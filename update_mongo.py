#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr  6 10:29:45 2021

@author: sofiapfund
"""

import requests
import json
import re
import csv
from pymongo import MongoClient
client = MongoClient() ### let python connect to mongo
cl = client['progenetix'].publications ### load collection

l = []
with open("/Users/sofiapfund/Desktop/Internship/Scripts/retrieve_publications/publications.txt") as f:
   rd = csv.reader(f, delimiter="\t", quotechar='"')
   for row in rd:
        l.append(row)

########################################################################################
      
def jprint(obj):
    text = json.dumps(obj, sort_keys= True, indent = 4, ensure_ascii = False)
    print(text)

########################################################################################

pub = {}

def get_publications(row):
    
    parameters = {
        "query": row[0], #pmid
        "format": "json",
        "resultType": "core"
        }
    response = requests.get("https://www.ebi.ac.uk/europepmc/webservices/rest/search", params = parameters)
    
    if response.status_code == 200:
        results = response.json()["resultList"]["result"]
        info = results[0]
        
        # Get basic informations:
        abstract = info["abstractText"]
        ID = info["pmid"]
        author = info["authorString"]
        journal = info["journalInfo"]["journal"]["medlineAbbreviation"]
        title = info["title"]
        year = info["pubYear"]
        
        # Make label:
        short_author = re.sub(r'(.{2,32}[\w\.\-]+? \w\-?\w?\w?)(\,| and ).*?$', r'\1 et al.', author)

        if len(title) <= 100:
            label = short_author + f' ({year}) ' + title
        else:
            label = short_author + f' ({year}) ' + ' '.join(title.split(' ')[:12]) + ' ...'
        
        # Remove HTML formatting:
        abstract_no_html = re.sub(r'<[^\>]+?>', "", abstract)
        title_no_html = re.sub(r'<[^\>]+?>', "", title)
        label_no_html = re.sub(r'<[^\>]+?>', "", label)        
        
        # Fill in counts:
        counts = {}
        counts.update({"acgh": int(row[1]),
                        "arraymap": 0,
                        "ccgh": int(row[2]),
                        "genomes": int(row[3]),
                        "ngs": int(row[4]),
                        "progenetix": int(row[5]),
                        "wes": int(row[6]),
                        "wgs": int(row[7])
                        })
        
        pub.update({"abstract": abstract_no_html,
                    "authors": author,
                    "counts": counts,
                    "id": "PMID:" + str(ID),
                    "label": label_no_html, 
                    "journal": journal,
                    "sortid": None, 
                    "title": title_no_html,
                    "year": year
                    })  
        
    # Get geolocation:
    where = {"city": row[8]} #city
    location = requests.get("https://progenetix.org/services/geolocations", params = where)
    coordinates = location.json()["response"]["results"]
    
    for info in coordinates:
        if info["id"] == row[9]: #locationID = heidelberg::germany
            provenance = info
    
    pub.update({"provenance": provenance})
    
    return pub

posts = []
for i, row in enumerate(l):
    post = {}
    if i > 0: # 1st row contains names of columns
        post = get_publications(row)
        post_copy = post.copy()
        posts.append(post_copy)


########################################################################################

# Upload new publications to publication collection on MongoDB:

ids = cl.distinct("id")
#print(ids) #must be in the format : r"[PMID:]\d{8}"

for post in posts:   
        
    if post["id"] in ids:
        print(post["id"], ": this article is already on the progenetix publications collection.")
        #result = cl.update_one({"id": post["id"]}, {"$set": {"sample_types": [{"id": "NCIT:C96963", ...}]}}) #example of an update that could be done
        #result.inserted_id
        
    else:
        print(post["id"], ": this article isn't on the progenetix publications collection yet.")
        result = cl.insert_one(post)
        result.inserted_id














    
