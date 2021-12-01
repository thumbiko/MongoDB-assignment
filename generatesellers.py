#!/usr/bin/env python
# coding: utf-8

# This program takes in a csv file that contains information on school book orders.   It generates javascript that can be run on a MongoDB instance to add documents.  In this case, the structure is:
#  -  A database called bestSeller - hence 'use BestSeller'
#  -  A collection called sellers - hence 'db.sellers.insert'
#  -  Each sellers document has a single Name, Author,Price, custName, Year, Reviews, User rating and  instance and an embedded array of feedbacks ordered in that order.
#  -  In this case, the 'Name' is unique, because we've dropped duplicates.
# 

import pandas as pd
import json

# First, let's format our .mongodb file:
# For the main program, we're reading in the csv file from the 'data' sub-directory and putting it into a Pandas dataframe.
# Then we extract a frame of  unique 'Name' values.
# Then we make documents from the data and write insert statements to a file.

#Read the full csv into df.
df = pd.read_csv('data/bestsellerswcats.csv', sep = ',', delimiter = None,encoding='latin-1')

#If our document is a book order, we want the Name to identify the document, so we extract that, dropping duplicates.
bestorder = df[['Name']].drop_duplicates()

def writeOrderfile(doclist, outfile):
# This writes document inserts to a .mongodb file that can be run on a MongoDB client
    file = open(outfile,'w')
    # It starts by choosing the BestSeller database
    rec = 'use ("BestSeller");\n'
    file.write(rec)
    # For each unique Name...
    for r in doclist[['Name']].itertuples(index=False):
        theserows = (df[(df['Name']==r)])
        # Retrieve the info that will be in the main part of the document
        agginfo = theserows[['User rating','Reviews']].drop_duplicates()
        # Retrieve repeating rows
        tc = theserows[['Author','Price','Year','Genre']]
        #Make up the document
        entries = json.dumps({"Name":r,
                              #"Author":agginfo['Author'].values[0],
                               #"Price": agginfo['Price'].values[0],
                               #"custName": agginfo['custName'].values[0],
                              # "Year": agginfo['Year'].values[0],*/
                              "Reviews": agginfo['Reviews'].values[0],
                               "feedbacks": tc.to_dict('records'),
                        
                               "User rating ": agginfo['User rating'].values[0]
                             })
        #Write the document insert statement to the .mongodb file
        rec = 'db.sellers.insertOne(' + entries + ');\n'
        file.write(rec)
    file.close()
    return()

writeOrderfile(bestorder, 'bests.mongodb')
