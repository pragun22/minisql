import sys
import sqlparse
import csv
import copy 
class sql():

    def __init__(self, query):
        '''Pass query is list format'''
        self.query = query
        self.toks = []
        self.meta ={}
        self.table_field = {}
        self.distinct = False
        self.where = False
        self.mat = {}
        self.db_dir = "../files/" 
        self.func = ['max','min','avg','sum']

        self.init_meta() 
        self.parser()
    def sum(self,data):
        ret = 0
        for i in data : ret += int(i)
        return ret
    def avg(self,data):
        return float(self.sum(data)/len(data))
    def max(self,data):
        mx = float("-inf")
        for i in data:
            mx = int(i) if int(i) > mx else mx
        return mx
    def min(self,data):
        mn = float("inf")
        for i in data:
            mn = int(i) if int(i) < mn else mn
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
            
        self.distinct = True if self.toks[0] == 'distinct' else False
        if len(self.toks) > (4 if self.distinct else 3):
            self.where = True
        tables = self.toks[3].split(',') if self.distinct else self.toks[2].split(',')
        tables = [ i.strip()  for i in tables]
        try:
            for i in tables:
                reader = csv.reader(open(self.db_dir +i+ '.csv'))
                temp = {}
                for j in self.meta[i]:
                    temp[j] = []
                self.mat[i] = []
                for row in reader:
                    self.mat[i].append(row)
                    for j in range(len(self.meta[i])):
                        temp[self.meta[i][j]].append(row[j])
                self.table_field[i] = temp
        except Exception as e:
            print("Something Wrong with query")
            print("Error: ",e)
            exit()
        # print(self.table_field['table1']['A'],sep='\n')
        fields = self.toks[0].split(',') if not self.distinct else self.toks[1].split(',')
        fields = [ i.strip() for i in fields ]
        func_flag = False
        for i in self.func : 
            if i in fields[0].lower(): func_flag = True
        if len(tables) ==1 :
            tableName = tables[0]
            fields = self.meta[tableName] if fields[0]=='*' else fields
            try:
                Output = []
                entries = len(self.table_field[tableName][fields[0]]) if not func_flag else 1
                row = []
                for i in fields:
                    row.append(tableName+'.'+i)
                Output.append(row)
                if func_flag:
                    row = []
                    for i in fields:
                        fn = i[:3].lower()
                        fld = i[4]
                        # print(fn)
                        if fn == 'max' : val = self.max(self.table_field[tableName][fld])
                        if fn == 'min' : val = self.min(self.table_field[tableName][fld])
                        if fn == 'sum' : val = self.sum(self.table_field[tableName][fld])
                        if fn == 'avg' : val = self.avg(self.table_field[tableName][fld])
                        row.append(val)       
                    Output.append(row)
                else:
                    for i in range(entries):
                        row = []
                        for j in fields:
                            row.append(self.table_field[tableName][j][i])
                        Output.append(row)    
                
                for i in range(len(Output)):
                    for j in Output[i]:
                        print(j,end=(', ' if j != Output[i][-1] else ' '))
                    print("")
            except Exception as e :
                print("\033[91mField Not Found!! check your query\033[00m")
                print("Error: ", e)
        else:
            Output = []
            row = []
            col = []
            if len(tables) != len(set(tables)):
                print("Error : table names can not be repeated")
                exit(0)
            for i in tables:
                for j in self.meta[i]:
                    row.append(i+'.'+j)
                    col.append(True if j in fields else False)
            Output.append(row)
            if(fields[0]=='*'):
                for i in range(len(col)): col[i] = True        
            runtable = copy.deepcopy(self.mat[tables[0]])
            for i in range(1, len(tables)):
                new_mat = []
                for j in runtable:
                    for p in self.mat[tables[i]]:
                        new_mat.append(j+p)    
                runtable = copy.deepcopy(new_mat)
            for i in runtable:
                Output.append(i)
            for i in range(len(Output)):
                flag = False
                for ind, j in enumerate(Output[i]):
                    if col[ind] :
                        print(j,end=(', '))
                        flag = True
                    if flag and ind == len(col)-1 : print("")

# Code begins from here
if len(sys.argv) < 2 :
    print("Wrong Syntax")
    exit(0)
query = ' '.join(sys.argv[1:])
sql(query)



'''Use Later'''
            # sets = [set() for i in range(len(tables))]
            # for ind, key in enumerate(tables):
            #     for j in self.meta[key]:
            #         sets[ind].add(j)
            # common_field = sets[0]
            # all_field = sets[0]
            # for i in range(1, len(tables)):
            #     common_field = common_field & sets[i]
            #     all_field = all_field | sets[i]
            # com_var = [set() for i in range(len(common_field))]
            # for table in tables:
            #     for ind, p in enumerate(common_field):
            #         for j in self.table_field[table][p]:
            #             com_var[ind].add(j)
            # # print(com_var)    
            # fin_table = []
            # row  = []
            # fields = all_field if fields[0]=='*' else fields
            # for i in all_field:
            #     for j in tables:
            #         if i in self.table_field[j]:
            #             row.append(j+'.'+i)
            #             break
            # fin_table.append(row)
            # for i in range(len(com_var[0])):
            #     row = []
            #     for j in all_field:
            #         for p in tables:
            #             if j in self.table_field[p]:
            #                 row.append(self.table_field[p][j][i])
            #                 break
            #     fin_table.append(row)
            # # for i in range(len(fin_table)):
            # #     for j in fin_table[i]:
            # #         print(j,end=(', ' if j != fin_table[i][-1] else ' '))
            # #     print("")
            # Output = []
            # for i in range(len(fin_table)):
            #     row = []
            #     for ind, j in enumerate(fin_table[i]):
            #         if fin_table[0][ind][-1] in fields:
            #             row.append(j)
            #     Output.append(row)
            # for i in range(len(Output)):
            #     for j in Output[i]:
            #         print(j,end=(', ' if j != Output[i][-1] else ' '))
            #     print("")

