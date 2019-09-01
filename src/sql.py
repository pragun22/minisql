import sys
import sqlparse
     
class sql():

    def __init__(self, query):
        '''Pass query is list format'''
        self.query = query
        self.toks = []
        self.meta ={}
        self.init_meta()
        self.parser()

    def init_meta(self):
        ''' inititalize the meta dictionary with metadata txt file '''
        content = open("../files/metadata.txt","r").readlines()
        # lines = content
        lines = [i.strip() for i in content ]
        print(lines)
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
        
        if queryType != 'SELECT':
            print('query not yet supported.\nUse Select onle')
            exit(0)
        else:
            pass

    
    
# main code begins from here

if len(sys.argv)<2 :
    print("Wrong Syntax")
    exit(0)
query = ' '.join(sys.argv[1:])
sql(query)