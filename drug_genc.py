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

def single(row, writer, name, datatype, wcount, concept_list):
    if datatype == '':
        datatype = 'N/A'
    if name != '':
        if name not in concept_list :
            wcount = wcount + 1
            concept_list.append(name)
            cl = 'Drug'
            writer.writerow({'name':name,'class':cl,'datatype':datatype})        
    return wcount, concept_list

def main(argv):
    inputfile = ''
    outputfile = ''
    try:
        opts, args = getopt.getopt(argv,"hi:o:",["ifile=","ofile="])
    except getopt.GetoptError:
        print ('drug_gen.py -i <inputfile> -o <outputfile>')
        sys.exit(2)
    if len(opts) <= 1:
        print ('drug_gen.py -i <inputfile> -o <outputfile>')
        sys.exit()

    for opt, arg in opts:
        if opt == '-h':
            print ('drug_gen.py -i <inputfile> -o <outputfile>')
            sys.exit()
        elif opt in ("-i", "--ifile"):
            inputfile = arg
        elif opt in ("-o", "--ofile"):
            outputfile = arg

    ofile = outputfile.split(".")
    ofile1 = ofile[0]+"_concept.csv"
    ofile2 = ofile[0]+"_drug.csv"
    print ('Output concept file is ', ofile1, ' drug: ', ofile2)
       
    csv.register_dialect(
    'mydialect',
    lineterminator = '\n')
    with open(ofile1, 'w') as concepts_csvfile:
        fieldnames = ["uuid","name","description","class","shortname","datatype","synonym.1","synonym.2","synonym.3","answer.1","reference-term-source","reference-term-code","reference-term-relationship"]
        writer = csv.DictWriter(concepts_csvfile, fieldnames=fieldnames, lineterminator='\n')
        writer.writeheader()

        drugs_csvfile = open(ofile2, 'w')
        fieldnames = ["uuid","Name","Generic Name","Strength","Dosage Form","Minimum Dose", "Maximum Dose"]
        writer_drugs = csv.DictWriter(drugs_csvfile, fieldnames=fieldnames, lineterminator='\n')
        writer_drugs.writeheader()
        rcount = 0
        wcount = 0
        dcount = 0
        concept_list = []
        drug_list = []
        with open(inputfile) as in_csvfile:
            reader = csv.DictReader(in_csvfile, quotechar='"')
            block = OrderedDict()
            for row in reader:
                rcount = rcount+1
                name  = row['Name'].strip()
                generic  = row['Generic Name'].strip()

                if (name != ''):
                    wcount, concept_list = single(row, writer, generic, "N/A", wcount, concept_list)

                if name not in drug_list:                
                    dcount = dcount + 1
                    drug_list.append(name)
                    writer_drugs.writerow({'Name':name, 'Generic Name':row['Generic Name'].strip(),'Strength':row['Strength'].strip(),'Dosage Form':row['Dosage Form'].strip()})                                    
            
            print ("Read records: " + str(rcount), "Wrote: " + str(wcount), "Concepts: " + str(len(concept_list)))

if __name__ == '__main__':
    main(sys.argv[1:])