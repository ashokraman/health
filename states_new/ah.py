#!/usr/bin/python
import csv
import sys, getopt
import uuid
import copy
from datetime import datetime
from collections import OrderedDict
import MySQLdb
import os
import fnmatch

def read_db(cursor, db, name, level_id):
    # Prepare SQL query to read a record from the database.
    sql = "select address_hierarchy_entry_id from address_hierarchy_entry where (name='%s' and level_id='%d')" % \
       (name, level_id)
    try:
       # Execute the SQL command
       res = cursor.execute(sql)
       if res:
          data = cursor.fetchone()
          return data[0]
    except:
       return None

def write_db(cursor, db, name, level_id, parent_id):
    data = read_db(cursor, db, name, level_id)
    if data:
       return data
    # Prepare SQL query to INSERT a record into the database.
    uuid_str = str(uuid.uuid1())
    if parent_id:
       sql = "INSERT INTO address_hierarchy_entry(name, level_id, parent_id, uuid) VALUES ('%s', '%d', '%d', '%s')" % \
       (name, level_id, parent_id, uuid_str)
    else:
       sql = "INSERT INTO address_hierarchy_entry(name, level_id, uuid) VALUES ('%s', '%d', '%s')" % \
       (name, level_id, uuid_str)
    try:
        # Execute the SQL command
        res = cursor.execute(sql)
        # Commit your changes in the database
        db.commit()
        sql = "select address_hierarchy_entry_id from address_hierarchy_entry where (name='%s' and level_id='%d' and uuid='%s')" % (name, level_id, uuid_str)
        res = cursor.execute(sql)
        data = cursor.fetchone()
        return data[0]
    except:
      # Rollback in case there is any error
      db.rollback()
       
def main(argv):

    inputdir = ''

    try:
       opts, args = getopt.getopt(argv,"hi:",["idir="])
    except getopt.GetoptError:
       print ('address.py -i <inputdir>')
       sys.exit(2)
    if len(opts) <= 0:
       print ('address.py -i <inputdir>')
       sys.exit()

    for opt, arg in opts:
       if opt == '-h':
          print ('address.py -i <inputdir>')
          sys.exit()
       elif opt in ("-i", "--idir"):
          inputdir = arg

    print (' Input directory is ', inputdir)
    # Open database connection
    db = MySQLdb.connect("192.168.33.10","admin","admin","openmrs" )

    # prepare a cursor object using cursor() method
    cursor = db.cursor()
       
    rcount = 0
    wcount = 0
    filecount = 0
    # Set the directory you want to start from
    rootDir = inputdir
    for dirName, subdirList, fileList in os.walk(rootDir):
       print('Found directory: %s' % dirName)
    for fname in fileList:
       if fnmatch.fnmatch(fname, "*.csv"):
          print('\t%s' % fname)
          inputfile = fname
          with open(inputfile) as in_csvfile:
             reader = csv.DictReader(in_csvfile, quotechar='"')
             pParent = ""
             pChild = ""
             pSubChild = ""
             pSubSubChild = ""
             pZip = ""
             for row in reader:
                rcount = rcount+1
                parent  = row['DTName'].strip().replace("'","")
                state  = row['DTCode'].strip().replace("'","")
                zip  = row['SDTCode'].strip().replace("'","")
                tvcode  = row['TVCode'].strip().replace("'","")
                if state == '000':
                    if pParent != parent:
                       state_id = write_db(cursor, db, parent, 3, None)
                       continue

                if zip == '00000':
                    parent_id = write_db(cursor, db, parent, 4, state_id)
                    continue
                    
                child  = row['DTName'].strip().replace("'","")
                subChild  = row['SDTName'].strip().replace("'","")
                subSubChild  = row['Name'].strip().replace("'","")
                if tvcode == '000000':
                    continue
                if pZip != zip:
                    pZip = zip
                    pParent = parent
                    if pChild != child:
                       pChild = child
                       child_id = write_db(cursor, db, child, 4, parent_id)
                    if pSubChild != subChild:
                       psubChild = subChild
                       sub_child_id = write_db(cursor, db, subChild, 5, child_id)
                    if pSubSubChild != subSubChild:
                       pSubSubChild = subSubChild
                       sub_sub_child_id = write_db(cursor, db, subSubChild, 6, sub_child_id)
          

if __name__ == '__main__':
    main(sys.argv[1:])