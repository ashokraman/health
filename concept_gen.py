import csv
import sys, getopt
import uuid
import copy
from datetime import datetime
from collections import OrderedDict

def date(datestr="", format="%Y%m%d"):
    if not datestr:
        return datetime.today().date()
    return datetime.strptime(datestr, format).date()

def coded(row, writer, name, datatype, wcount, concept_list, concepts_dict_list):
    answers = row['answer'].strip().split(',') 
    cl = row['class'] if 'class' in row else 'Misc'    
    reference_term_source  = row['reference-term-source']
    reference_term_code  = row['reference-term-code']
    reference_term_relationship  = row['reference-term-relationship']
    if name != '':
        for answer in answers:
            if answer != '':
                if answer not in concepts_dict_list:
                    if answer not in concept_list:
                        wcount = wcount + 1
                        concept_list.append(answer)
                        writer.writerow({'uuid':uuid.uuid1(),'name':answer,'class':cl,'datatype':'N/A'})
        if name not in concept_list:
            wcount = wcount + 1
            concept_list.append(name)
            arow = OrderedDict([('uuid',uuid.uuid1()),('name',name),('class',cl),('datatype',datatype)])
            for i in range(1,len(answers)+1):
                arow['answer.'+str(i)]=answers[i-1]
            writer.writerow(arow)        
    return wcount, concept_list

def single(row, writer, name, datatype, wcount, concept_list, concepts_dict_list):
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
        if name not in concepts_dict_list:
            if name not in concept_list :
                wcount = wcount + 1
                concept_list.append(name)
                cl = row['class'] if 'class' in row else 'Misc'
                writer.writerow({'uuid':uuid.uuid1(),'name':name,'class':cl,'datatype':datatype,'High Normal':High_Normal,'Low Normal':Low_Normal})        
    return wcount, concept_list

def get_concepts_list(dictionary_file_list):
    # file_data is the text of the file, not the filename
    concepts_dict_list = []
    for dictionary_file in dictionary_file_list.split(':'):
        with open(dictionary_file, encoding="utf8") as in_csvfile:
            reader = csv.DictReader(in_csvfile, quotechar='"')    
    #        reader = csv.DictReader(dictionary_file, ('Concept Id','Name','Description','Synonyms','Answers','Set Members','Class','Datatype','Changed By','Creator'))
            try:
                for row in reader:
                    concepts_dict_list.append(row['Name'] if 'Name' in row else row['name'])
            except:
                import pdb; pdb.set_trace()
                print("Oops!  Encode issue... file: " + in_csvfile)

    return concepts_dict_list

def main(argv):
    inputfile = ''
    outputfile = ''
    concept_dictionaryfile = ''
    try:
        opts, args = getopt.getopt(argv,"hi:o:d:",["ifile=","ofile=","dfile="])
    except getopt.GetoptError:
        print ('concept_gen.py -i <inputfile> -o <outputfile> -d <concept_dictionaryfile>')
        sys.exit(2)
    if len(opts) <= 2:
        print ('concept_gen.py -i <inputfile> -o <outputfile> -d <concept_dictionaryfile>')
        sys.exit()

    for opt, arg in opts:
        if opt == '-h':
            print ('concept_gen.py -i <inputfile> -o <outputfile> -d <concept_dictionaryfile>')
            sys.exit()
        elif opt in ("-i", "--ifile"):
            inputfile = arg
        elif opt in ("-o", "--ofile"):
            outputfile = arg
        elif opt in ("-d", "--dfile"):
            concept_dictionaryfile = arg
    print ('Concept Dictionary file is ', concept_dictionaryfile, ' Input file is ', inputfile)
    ofile = outputfile.split(".")
    ofile1 = ofile[0]+".csv"
    ofile2 = ofile[0]+"_set.csv"
    print ('Output file is ', ofile1, ' sets: ', ofile2)
   
    # read in the conceptDictionary as concepts
    concepts_dict_list = get_concepts_list(concept_dictionaryfile)
    
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
        concept_list = []
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
                    wcount, concept_list = coded(row, writer, name, datatype, wcount, concept_list, concepts_dict_list)
                elif datatype == 'Block':
                        alist = row['synonym.1'].strip()
                        clist = alist.split(',')
                        if len(clist) > 0:
                            block[name] = clist
                else:
                    if datatype != '':
                        wcount, concept_list = single(row, writer, name, datatype, wcount, concept_list, concepts_dict_list)

                if row['class'] == 'Concept Details':
                    if name not in concepts_dict_list+concept_list:                
                        wcount = wcount + 1
                        concept_list.append(name)
                        child_1 = row['child.1'].strip() if len(row['child.1']) > 0 else ""
                        child_2 = row['child.2'].strip() if len(row['child.2']) > 0 else ""
                        child_3 = row['child.3'].strip() if len(row['child.3']) > 0 else ""
                        child_4 = row['child.4'].strip() if len(row['child.4']) > 0 else ""
                        child_5 = row['child.5'].strip() if len(row['child.5']) > 0 else ""
                        child_6 = row['child.6'].strip() if len(row['child.6']) > 0 else ""
                        child_7 = row['child.7'].strip() if len(row['child.7']) > 0 else ""
                        child_8 = row['child.8'].strip() if len(row['child.8']) > 0 else ""
                        child_9 = row['child.9'].strip() if len(row['child.9']) > 0 else ""
                        child_10 = row['child.10'].strip() if len(row['child.10']) > 0 else ""
                        writer_set.writerow({'uuid':uuid.uuid1(),'name':name,'class':'Concept Details','child.1':child_1,'child.2':child_2,'child.3':child_3,'child.4':child_4,'child.5':child_5,'child.6':child_6,'child.7':child_7,'child.8':child_8,'child.9':child_9,'child.10':child_10})                        
                
            for item in block.items():
#                print("Item: " + str(item))
                if item[0] not in concepts_dict_list:
                    if item[0] not in concept_list:
                        wcount = wcount + 1
                        concept_list.append(item)
                        child_1 = item[1][0].strip() if len(item[1]) > 0 else ""
                        child_2 = item[1][1].strip() if len(item[1]) > 1 else ""
                        child_3 = item[1][2].strip() if len(item[1]) > 2 else ""
                        child_4 = item[1][3].strip() if len(item[1]) > 3 else ""
                        child_5 = item[1][4].strip() if len(item[1]) > 4 else ""
                        child_6 = item[1][5].strip() if len(item[1]) > 5 else ""
                        child_7 = "" if len(item[1]) <= 6 else item[1][6]
                        child_8 = "" if len(item[1]) <= 7 else item[1][7]
                        child_9 = "" if len(item[1]) <= 8 else item[1][8]
                        child_10 = "" if len(item[1]) <= 9 else item[1][9]
                        writer_set.writerow({'uuid':uuid.uuid1(),'name':item[0],'class':'Misc','child.1':child_1,'child.2':child_2,'child.3':child_3,'child.4':child_4,'child.5':child_5,'child.6':child_6,'child.7':child_7,'child.8':child_8,'child.9':child_9,'child.10':child_10})        
            
            
            print ("Read records: " + str(rcount), "Wrote: " + str(wcount), "Concepts: " + str(len(concept_list)))

if __name__ == '__main__':
    main(sys.argv[1:])