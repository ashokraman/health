import io
import csv
import sys, getopt
import uuid
import copy
import MySQLdb
from datetime import datetime
from collections import OrderedDict

def date(datestr="", format="%Y%m%d"):
    if not datestr:
        return datetime.today().date()
    return datetime.strptime(datestr, format).date()

def coded(row, writer, name, datatype, wcount, cursor):
    answers = row['answer'].strip().split(',') 
    cl = row['class'] if 'class' in row else 'Misc'    
    reference_term_source  = row['reference-term-source']
    reference_term_code  = row['reference-term-code']
    reference_term_relationship  = row['reference-term-relationship']
    if name != '':
        for answer in answers:
            if answer != '':
                if not check_in_db(cursor, answer):
                    wcount = wcount + 1
                    writer.writerow({'uuid':uuid.uuid1(),'name':answer,'class':cl,'datatype':'N/A'})
        if not check_in_db(cursor, name):
            wcount = wcount + 1
            arow = OrderedDict([('uuid',uuid.uuid1()),('name',name),('class',cl),('datatype',datatype)])
            for i in range(1,len(answers)+1):
                arow['answer.'+str(i)]=answers[i-1]
            writer.writerow(arow)        
    return wcount

def single(row, writer, name, datatype, wcount, cursor):
    units  = row['units']
    High_Normal  = row['High Normal']
    Low_Normal  = row['Low Normal']
    Allow_Decimal  = row['Allow Decimal']
    locale  = row['locale']
    synonym_1  = row['synonym.1']
    reference_term_source  = row['reference-term-source']
    reference_term_code  = row['reference-term-code']
    reference_term_relationship  = row['reference-term-relationship']
    if datatype == '':
        datatype = 'Text'
    if name != '':
        if not check_in_db(cursor, name):
            wcount = wcount + 1
            cl = row['class'] if 'class' in row else 'Misc'
            writer.writerow({'uuid':uuid.uuid1(),'name':name,'class':cl,'datatype':datatype,'High Normal':High_Normal,'Low Normal':Low_Normal})        
    return wcount

def check_in_db(cursor, concept):

    try:
        sql = '''SELECT name FROM concept_name WHERE name = "%s"''' % (concept)
#        sql = "SELECT name FROM concept_name WHERE name = ?" , concept
        data = cursor.execute(sql)
        if data >= 1:
            return True
    except:
        return False

def main(argv):
    inputfile = ''
    outputfile = ''
    concept_dictionaryfile = ''
    try:
        opts, args = getopt.getopt(argv,"hi:o:d:",["ifile=","ofile="])
    except getopt.GetoptError:
        print ('concept_gen.py -i <inputfile> -o <outputfile>')
        sys.exit(2)
    if len(opts) <= 1:
        print ('concept_gen.py -i <inputfile> -o <outputfile>')
        sys.exit()

    for opt, arg in opts:
        if opt == '-h':
            print ('concept_gen.py -i <inputfile> -o <outputfile>')
            sys.exit()
        elif opt in ("-i", "--ifile"):
            inputfile = arg
        elif opt in ("-o", "--ofile"):
            outputfile = arg
    ofile = outputfile.split(".")
    ofile1 = ofile[0]+".csv"
    ofile2 = ofile[0]+"_set.csv"
    print ('Output file is ', ofile1, ' sets: ', ofile2)
    
    # set up DB to compare if name exists
    
    db = MySQLdb.connect(host="192.168.33.10", port=3306, user="admin", passwd="admin", db="openmrs")
    cursor = db.cursor()
    
    csv.register_dialect(
    'mydialect',
    lineterminator = '\n')
    with open(ofile1, 'w') as concepts_csvfile:
        fieldnames = ["uuid","name","description","class","shortname","datatype","units","High Normal","Low Normal","Allow Decimal","locale","synonym.1","reference-term-source","reference-term-code","reference-term-relationship"]
        for i in range(1,65):
            fieldnames.append("answer."+str(i))
        writer = csv.DictWriter(concepts_csvfile, fieldnames=fieldnames, lineterminator='\n')
        writer.writeheader()
        fieldnames = ["uuid","name","description","class","shortname","reference-term-source","reference-term-code","reference-term-relationship"]
        for i in range(1,65):
            fieldnames.append("child."+str(i))
        concepts_set_csvfile = open(ofile2, 'w')
        writer_set = csv.DictWriter(concepts_set_csvfile, fieldnames=fieldnames, lineterminator='\n')
        writer_set.writeheader()
        rcount = 0
        wcount = 0
        with open(inputfile) as in_csvfile:
            reader = csv.DictReader(in_csvfile, quotechar='"')
            block = OrderedDict()
            for row in reader:
                rcount = rcount+1
                Parent  = row['Parent'].strip()
                name  = row['name'].strip()
                datatype  = row['datatype'].strip()
                if Parent != '':
                    if Parent in block:
                        clist = block[Parent]
                        clist.append(name)
                    else:
                        clist = [name]
                        block[Parent] = clist
                    
                if datatype == 'Coded':
                    wcount = coded(row, writer, name, datatype, wcount, cursor)
                elif datatype == 'Block':
                        alist = row['synonym.1'].strip()
                        clist = alist.split(',')
                        if len(clist) > 0:
                            block[name] = clist
                else:
                    if datatype != '':
                        wcount = single(row, writer, name, datatype, wcount, cursor)

                if row['class'] == 'Concept Details':
                    if not check_in_db(cursor, name):                
                        wcount = wcount + 1
                        arow = OrderedDict([('uuid',uuid.uuid1()),('name',name),('class','Concept Details')])
                        for i in range(1,65):
                            if 'child.'+str(i) in row:
                                arow['child.'+str(i)]=row['child.'+str(i)]
                        writer_set.writerow(arow)        
                        
                
            for item in block.items():
#                print("Item: " + str(item))
				if not check_in_db(cursor, item[0]):
					wcount = wcount + 1
					arow = OrderedDict([('uuid',uuid.uuid1()),('name',item[0]),('class','Misc')])
					for i in range(1,65):
						if len(item[1]) >= i:
							arow['child.'+str(i)]=item[1][i-1]
					writer_set.writerow(arow)        
            
            db.close()            
            print ("Read records: " + str(rcount), "Wrote: " + str(wcount))

if __name__ == '__main__':
    main(sys.argv[1:])