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
                        arow = OrderedDict([('uuid',uuid.uuid1()),('name',name),('class','Concept Details')])
                        for i in range(1,65):
                            if 'child.'+str(i) in row:
                                arow['child.'+str(i)]=row['child.'+str(i)]
                        writer_set.writerow(arow)        
                        
                
            for item in block.items():
#                print("Item: " + str(item))
                if item[0] not in concepts_dict_list:
                    if item[0] not in concept_list:
                        wcount = wcount + 1
                        concept_list.append(item)
                        arow = OrderedDict([('uuid',uuid.uuid1()),('name',item[0]),('class','Misc')])
                        for i in range(1,65):
                            if len(item[1]) >= i:
                                arow['child.'+str(i)]=item[1][i-1]
                        writer_set.writerow(arow)        
            
            
            print ("Read records: " + str(rcount), "Wrote: " + str(wcount), "Concepts: " + str(len(concept_list)))

if __name__ == '__main__':
    main(sys.argv[1:])