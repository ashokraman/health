#
# python concept_gen_dbchecker.py -i all_mapped.csv -o baf
#
# refers to database and checks for existense of concepts 
#
# 22-nov-2017
# to deal with new column Block in csv file
# and mappings
#
# No virtual env, python 2.7
#
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

def create_iad_code(row, iad_code):
    reference_term_source  = row['reference-term-source']
#   if reference_term_source == '':
    row['reference-term-source'] = "IAD"
    iad_code = iad_code + 1
    row['reference-term-code'] = iad_code
    row['reference-term-relationship'] = 'SAME-AS'
#    return iad_code
    return iad_code

def coded(row, writer, writer_ref, name, datatype, wcount, concept_list, cursor, classes, iad_code):
    desc  = ''
    answers = row['answer'].strip().split(',') 
    cl = row['class'] if 'class' in row else 'Misc'    
    units  = row['units']
    High_Normal  = row['High Normal']
    Low_Normal  = row['Low Normal']
    Allow_Decimal  = row['Allow Decimal']
    locale  = row['locale']
    synonym_1  = row['synonym.1']
    reference_term_source  = row['reference-term-source']
    reference_term_code  = row['reference-term-code']
    reference_term_relationship  = row['reference-term-relationship']
    
    if name != '':
        for answer in answers:
            if answer != '':
                if answer not in concept_list:
                    if not check_concept_in_db(cursor, classes, answer, 'Misc'):
                        iad_code = create_iad_code(row, iad_code)
                        wcount = wcount + 1
                        concept_list.append(answer)
                        trow = ([uuid.uuid1(),answer,desc,'Misc','','N/A',units,High_Normal,Low_Normal,'','',synonym_1,row['reference-term-source'],row['reference-term-code'],row['reference-term-relationship'],'','',''])        
                        for i in range(1,65):
                            trow.append("")
                        writer.writerow(trow)
                        writer_ref.writerow([uuid.uuid1(),answer,row['reference-term-source'],row['reference-term-code']])
        if name not in concept_list:
            if not check_concept_in_db(cursor, classes, name, cl):
                wcount = wcount + 1
                concept_list.append(name)
                iad_code = create_iad_code(row, iad_code)
                trow = ([uuid.uuid1(),name,desc,cl,'',datatype,units,High_Normal,Low_Normal,'','',synonym_1,row['reference-term-source'],row['reference-term-code'],row['reference-term-relationship'],reference_term_source,reference_term_code,reference_term_relationship])
                answers = row['answer'].strip().split(',')                 
                for item in answers:
                    trow.append(item)
                for i in range(1,65-len(answers)):
                    trow.append("")
                writer.writerow(trow)
                
                writer_ref.writerow([uuid.uuid1(),name,row['reference-term-source'],row['reference-term-code']])
    return wcount, iad_code, concept_list

def single(row, writer, writer_ref, name, datatype, wcount, concept_list, cursor, classes, iad_code):
    desc  = ''
    cl = row['class'] if 'class' in row else 'Misc'    
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
        if name not in concept_list :
            if not check_concept_in_db(cursor, classes, name, cl):
                wcount = wcount + 1
                concept_list.append(name)
                iad_code = create_iad_code(row, iad_code)
                trow = ([uuid.uuid1(),name,desc,cl,'',datatype,units,High_Normal,Low_Normal,'','',synonym_1,row['reference-term-source'],row['reference-term-code'],row['reference-term-relationship'],reference_term_source,reference_term_code,reference_term_relationship])
                for i in range(1,65):
                    trow.append("")            
                writer.writerow(trow)
                
                writer_ref.writerow([uuid.uuid1(),name,row['reference-term-source'],row['reference-term-code']])        
    return wcount, iad_code, concept_list

def check_class_in_db(cursor, classes, cl):

    if cl in classes:
        return classes[cl]
        
    try:
        sql = '''SELECT concept_class_id FROM concept_class WHERE name = "%s"''' % (cl)
        data = cursor.execute(sql)
        if data >= 1:
            classes[cl] = cursor.fetchone()[0]
            return classes[cl]
    except:
        return None

def check_concept_in_db(cursor, classes, concept, cl):

    try:
        clid = check_class_in_db(cursor, classes, cl)
        if clid:
            sql = '''select name from concept_name inner join concept on concept_name.concept_id=concept.concept_id WHERE name = "%s" and concept.class_id = "%s"''' % (concept, clid)
#        sql = "SELECT name FROM concept_name WHERE name = ?" , concept
            data = cursor.execute(sql)
            if data >= 1:
                return cursor.fetchone()
    except:
        return None

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
    ofile3 = ofile[0]+"_ref.csv"
    print ('Output file is ', ofile1, ' sets: ', ofile2, ' ref: ', ofile3)
    
    # set up DB to compare if name exists
    
    db = MySQLdb.connect(host="192.168.33.10", port=3306, user="admin", passwd="admin", db="openmrs")
    cursor = db.cursor()
    classes = {}
    
    csv.register_dialect('mydialect',lineterminator = '\n')
    concept_list = []
    with open(ofile1, 'w') as concepts_csvfile:
        fieldnames = ["uuid","name","description","class","shortname","datatype","units","High Normal","Low Normal","Allow Decimal","locale","synonym.1","reference-term-source","reference-term-code","reference-term-relationship","reference-term-source","reference-term-code","reference-term-relationship"]
        for i in range(1,65):
            fieldnames.append("answer."+str(i))
        writer = csv.writer(concepts_csvfile, lineterminator='\n')
        writer.writerow(fieldnames)
        fieldnames = ["uuid","name","description","class","shortname","reference-term-source","reference-term-code","reference-term-relationship"]
        for i in range(1,65):
            fieldnames.append("child."+str(i))
        concepts_set_csvfile = open(ofile2, 'w')
        writer_set = csv.DictWriter(concepts_set_csvfile, fieldnames=fieldnames, lineterminator='\n')
        writer_set.writeheader()

        fieldnames = ["uuid","name","source","code"]
        concepts_ref_csvfile = open(ofile3, 'w')
        writer_ref = csv.writer(concepts_ref_csvfile, lineterminator='\n')
        writer_ref.writerow(fieldnames)

        rcount = 0
        wcount = 0
        iad_code = 9000
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
                    wcount, iad_code, concept_list = coded(row, writer, writer_ref, name, datatype, wcount, concept_list, cursor, classes, iad_code)
                elif datatype == 'Block':
                        alist = row['Block'].strip()
                        clist = alist.split(',')
                        if len(clist) > 0:
                            block[name] = clist
                else:
                    if datatype != '':
                        wcount, iad_code, concept_list = single(row, writer, writer_ref, name, datatype, wcount, concept_list, cursor, classes, iad_code)

                if row['class'] == 'Concept Details':
                    aname = check_concept_in_db(cursor, classes, name, row['class'])                
                    if aname == None:                
                        wcount = wcount + 1
                        arow = OrderedDict([('uuid',uuid.uuid1()),('name',name),('class','Concept Details')])
                        for i in range(1,65):
                            if 'child.'+str(i) in row:
#                               dbname = check_concept_in_db(cursor,row['child.'+str(i)])
#                                arow['child.'+str(i)]=dbname[0] if dbname else row['child.'+str(i)]
                                arow['child.'+str(i)]=row['child.'+str(i)]
                        writer_set.writerow(arow)        
                        
                
            for item in block.items():
#               print("Item: " + str(item))
                aname = check_concept_in_db(cursor, classes, item[0], row['class'])
                if aname == None:
                    arow = OrderedDict([('uuid',uuid.uuid1()),('name',item[0]),('class','Misc')])
                    for i in range(1,65):
                        if len(item[1]) >= i:
#                           dbname = check_concept_in_db(cursor,item[1][i-1])
#                           arow['child.'+str(i)]=dbname[0] if dbname else item[1][i-1]
                            arow['child.'+str(i)]=item[1][i-1]
                    writer_set.writerow(arow)        
            
            db.close()            
            print ("Read records: " + str(rcount), "Wrote concepts: " + str(wcount))

if __name__ == '__main__':
    main(sys.argv[1:])