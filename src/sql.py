import sys
import sqlparse
import csv
class sql():

    def __init__(self, query):
        '''Pass query is list format'''
        self.query = query
        self.toks = []
        self.meta ={}
        self.table_field = {}
        self.distinct = False
        self.db_dir = "../files/" 

        self.init_meta()
        self.parser()
    def sum(self,data):
        ret = 0
        for i in data : ret+=i
        return ret
    def avg(self,data):
        return sum(data)/len(data)
    def max(self,data):
        mx = float("-inf")
        for i in data:
            mx = i if i > mx else mx
        return mx
    def min(self,data):
        mn = float("inf")
        for i in data:
            mn = i if i < mn else mn
        return mn
    def init_meta(self):
        ''' inititalize the meta dictionary with metadata txt file '''
        content = open("../files/metadata.txt","r").readlines()
        lines = [i.strip() for i in content ]
        for i in range(len(lines)):
            if lines[i] == "<begin_table>":
                tablename = None
                i += 1
                while i < len(lines) and lines[i] != "<end_table>":
                    if not tablename : 
                        tablename = lines[i]
                        self.meta[tablename] = []
                    else: self.meta[tablename].append(lines[i])
                    i += 1

    def parser(self):
        ''' Parser using pre built sqlparser, Pass query as strign in this '''
        parsedQuery = sqlparse.parse(self.query)[0].tokens
        queryType = sqlparse.sql.Statement(parsedQuery).get_type()
        for i in sqlparse.sql.IdentifierList(parsedQuery).get_identifiers():
            self.toks.append(str(i))
        self.toks = self.toks[1:]
        if queryType != 'SELECT':
            print('query not yet supported.\nUse Select only')
            exit(0)
        else:  
            self.process()

    def process(self):
        '''function where all the magic happens'''
        if len(self.toks) < 3 or len(self.toks) > 5:
            print("\033[91mParser Error!!!\033[00m\n\033[94mCheck your query\033[00m")
            exit(0)
       
        for i in range(len(self.toks)):
            temp = self.toks[i].lower()
            if temp == 'distinct' or temp == 'from' : self.toks[i] = temp
            
        print(self.toks)
        self.distinct = True if self.toks[0] == 'distinct' else False
        tables = self.toks[3].split(',') if self.distinct else self.toks[2].split(',')
        tables = [ i.strip()  for i in tables]
        for i in tables:
            reader = csv.reader(open(self.db_dir +i+ '.csv'))
            temp ={}
            for j in self.meta[i]:
                temp[j] = []
            for row in reader:
                for j in range(len(self.meta[i])):
                    temp[self.meta[i][j]].append(row[j])
            self.table_field[i] = temp
        print(self.table_field)

# Code starts from here
if len(sys.argv)<2 :
    print("Wrong Syntax")
    exit(0)
query = ' '.join(sys.argv[1:])
sql(query)