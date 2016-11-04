import csv
import sys, getopt
import uuid
import copy
from datetime import datetime
def date(datestr="", format="%Y%m%d"):
    if not datestr:
        return datetime.today().date()
    return datetime.strptime(datestr, format).date()

def coded(row, writer, name, datatype, wcount, concept_list, concepts_dict_list):
    answers  = [row['answer.1'].strip(),row['answer.2'].strip(),row['answer.3'].strip(),row['answer.4'].strip(),row['answer.5'].strip(),row['answer.6'].strip(),row['answer.7'].strip(),row['answer.8'].strip(),row['answer.9'].strip(),row['answer.10'].strip(),row['answer.12'].strip(),row['answer.12'].strip()] 
    answer_1  = row['answer.1']
    answer_2  = row['answer.2']
    answer_3  = row['answer.3']
    answer_4  = row['answer.4']
    answer_5  = row['answer.5']
    answer_6  = row['answer.6']
    answer_7  = row['answer.7']
    answer_8  = row['answer.8']
    answer_9  = row['answer.9']
    answer_10  = row['answer.10']
    answer_11  = row['answer.11']
    answer_12  = row['answer.12']
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
                        writer.writerow({'uuid':uuid.uuid1(),'name':answer,'class':'Misc','datatype':'N/A'})
        if name not in concept_list:
            wcount = wcount + 1
            concept_list.append(name)
            writer.writerow({'uuid':uuid.uuid1(),'name':name,'class':'Misc','datatype':datatype,'answer.1':answer_1,'answer.2':answer_2,'answer.3':answer_3,'answer.4':answer_4,'answer.5':answer_5,'answer.6':answer_6,'answer.7':answer_7,'answer.8':answer_8,'answer.9':answer_9,'answer.10':answer_10})        
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
                writer.writerow({'uuid':uuid.uuid1(),'name':name,'class':'Misc','datatype':datatype,'High Normal':High_Normal,'Low Normal':Low_Normal})        
    return wcount, concept_list

def get_concepts_list(dictionary_file_list):
    # file_data is the text of the file, not the filename
    concepts_dict_list = []
    for dictionary_file in dictionary_file_list.split(':'):
        with open(dictionary_file, encoding="utf8") as in_csvfile:
            reader = csv.DictReader(in_csvfile, quotechar='"')    
    #        reader = csv.DictReader(dictionary_file, ('Concept Id','Name','Description','Synonyms','Answers','Set Members','Class','Datatype','Changed By','Creator'))
            for row in reader:
                concepts_dict_list.append(row['Name'] if 'Name' in row else row['name'])

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
    ofile2 = ofile[0]+"_sets.csv"
    print ('Output file is ', ofile1, ' sets: ', ofile2)
   
    # read in the conceptDictionary as concepts
    concepts_dict_list = get_concepts_list(concept_dictionaryfile)
    
    csv.register_dialect(
    'mydialect',
    lineterminator = '\n')
    with open(ofile1, 'w') as concepts_csvfile:
        fieldnames = ["uuid","name","description","class","shortname","datatype","units","High Normal","Low Normal","Allow Decimal","locale","synonym.1","answer.1","answer.2","answer.3","answer.4","answer.5","answer.6","answer.7","answer.8","answer.9","answer.10","answer.11","answer.12","reference-term-source","reference-term-code","reference-term-relationship"]
        writer = csv.DictWriter(concepts_csvfile, fieldnames=fieldnames, lineterminator='\n')
        writer.writeheader()
        fieldnames = ["uuid","name","description","class","shortname","child.1","child.2","child.3","child.4","child.5","child.6","child.7","reference-term-source","reference-term-code","reference-term-relationship"]
        concepts_set_csvfile = open(ofile2, 'w')
        writer_set = csv.DictWriter(concepts_set_csvfile, fieldnames=fieldnames, lineterminator='\n')
        writer_set.writeheader()
        rcount = 0
        wcount = 0
        concept_list = []
        with open(inputfile) as in_csvfile:
            reader = csv.DictReader(in_csvfile, quotechar='"')
            block = {}
            for row in reader:
                rcount = rcount+1
                Parent  = row['Parent']
                name  = row['name']
                datatype  = row['datatype']
                if Parent != '':
                    if Parent in block:
                        clist = block[Parent]
                        clist.append(name)
                    else:
                        clist = [name]
                        block[Parent] = clist
                    
                    if datatype == 'Coded':
                        wcount, concept_list = coded(row, writer, name, datatype, wcount, concept_list, concepts_dict_list)
                    else:
                        wcount, concept_list = single(row, writer, name, datatype, wcount, concept_list, concepts_dict_list)

            for item in block.items():
                if item[0] not in concepts_dict_list:
                    if item[0] not in concept_list:
                        wcount = wcount + 1
                        concept_list.append(item)
                        child_1 = item[1][0] if len(item[1]) > 0 else ""
                        child_2 = item[1][1] if len(item[1]) > 1 else ""
                        child_3 = item[1][2] if len(item[1]) > 2 else ""
                        child_4 = item[1][3] if len(item[1]) > 3 else ""
                        child_5 = item[1][4] if len(item[1]) > 4 else ""
                        child_6 = item[1][5] if len(item[1]) > 5 else ""
                        child_7 = "" if len(item[1]) <= 6 else item[1][6]
                        writer_set.writerow({'uuid':uuid.uuid1(),'name':item[0],'class':'Misc','child.1':child_1,'child.2':child_2,'child.3':child_3,'child.4':child_4,'child.5':child_5,'child.6':child_6,'child.7':child_7})        
            
            
            print ("Read records: " + str(rcount), "Wrote: " + str(wcount), "Concepts: " + str(len(concept_list)))

if __name__ == '__main__':
    main(sys.argv[1:])