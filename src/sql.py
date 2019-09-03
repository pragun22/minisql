import sys
import sqlparse
import csv
import copy 
import re
from collections import defaultdict 
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
        self.operator = ['>=', '<=', '=', '>', '<']
        self.op_flag = 0
        self.arg1 = []
        self.arg2 = []
        self.cond = []
        self.join = []
        self.colnum = {}
        # self.operator = "< > >= <= ="
        self.init_meta() 
        self.parser()
    def joinTable(self,table,field1,field2):
        out = []
        for row in table:
            if row[field1] == row[field2]: 
                # fl = False
                # nwr = []
                # for ind,key in enumerate(row):
                #     if (ind == field1 or ind == field2):
                #         if not fl : nwr.append(key)
                #         fl = True
                #     else:
                #         nwr.append(key)
                out.append(row)        
        return out
    def isInt(self,s):
        try: 
            int(s)
            return True
        except ValueError:
            return False
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
        if self.where:

            try:
                temp = re.split('[<=> ]',self.toks[4][6:]) if self.distinct and self.where else re.split('[<=> ]',self.toks[3][6:])
                # temp = self.toks[4][6:] if self.distinct else self.toks[3][6:].split()
                where_fields = []
                for i in temp:
                    if i=="":continue
                    elif i.lower()=="and": self.op_flag = 1
                    elif i.lower()=="or": self.op_flag = 2
                    else :where_fields.append(i)
                self.arg1.append(where_fields[0])        
                self.arg2.append(where_fields[1])        
                temp = self.toks[4][6:] if self.distinct else self.toks[3][6:].lower()
                if self.op_flag ==0 :
                    for i in self.operator:
                        if i in temp:
                            self.cond.append(i)
                            break
                else:
                    self.arg1.append(where_fields[2])
                    self.arg2.append(where_fields[3])
                    i1 = temp.find("and") if self.op_flag==1 else temp.find("or")
                    for i in self.operator:
                        if i in temp[:i1]:
                            self.cond.append(i)
                            break
                    for i in self.operator:
                        if i in temp[i1+1:]:
                            self.cond.append(i)
                            break
                # print(self.cond)        
                # print(self.arg1,self.arg2)
            except Exception as e:
                print("Error in query")
                print("Error: ",e)
                exit(0)
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
        fields = self.toks[0].split(',') if not self.distinct else self.toks[1].split(',')
        fields = [ i.strip() for i in fields ]
        func_flag = False
        for i in fields:
            if '(' in i:
                func_flag = True
        if func_flag :  
            for i in fields:
                if '(' not in i:
                    print("\033[91mSyntax Error:\033[00m")
                    print("check fields")
                    exit(0)
        Output = []
        row = []
        col = []
        if len(tables) != len(set(tables)):
            print("Error : table names can not be repeated")
            exit(0)
        for i in tables:
            for j in self.meta[i]:
                temp = i + '.' + j
                row.append(temp)
                flag = False
                for fd in fields:
                    if func_flag:
                        id1 = fd.find('(')
                        id2 = fd.find(')')
                        fd = fd[id1+1:id2]
                    if fd in temp : flag = True 
                col.append(flag)
        Output.append(row)
        for ind,key in enumerate(row):
            self.colnum[key] = ind
        if(fields[0]=='*'):
            for i in range(len(col)): col[i] = True
        else:
            for i in fields:
                flg = False
                if func_flag:
                    id1 = i.find('(')
                    id2 = i.find(')')
                    i = i[id1+1:id2]
                for j in Output[0]:
                    if i in j : flg = True
                if not flg:
                    print("Error: Field not present")
                    exit(0)
       
        if not func_flag:
            runtable = copy.deepcopy(self.mat[tables[0]])
            for i in range(1, len(tables)):
                new_mat = []
                for j in runtable:
                    for p in self.mat[tables[i]]:
                        new_mat.append(j+p)    
                runtable = copy.deepcopy(new_mat)
            join_flag = False
            nw_cond = []
            nw_arg1 = []
            nw_arg2 = []
            for ind,key in enumerate(self.arg2):
                if not self.isInt(key):
                    self.join.append(key)
                    self.join.append(self.arg1[ind])
                    col[self.colnum[self.join[1]]] = 0
                    Output[0][self.colnum[self.join[0]]] = self.join[0].split('.')[1]
                    join_flag = True
                else:
                    nw_arg1.append(self.arg1[ind])
                    nw_arg2.append(self.arg2[ind])
                    nw_cond.append(self.cond[ind])
                    
            self.cond = nw_cond        
            self.arg1 = nw_arg1        
            self.arg2 = nw_arg2        
            if join_flag : runtable = self.joinTable(runtable, self.colnum[self.join[0]], self.colnum[self.join[1]])
            for i in runtable:
                if len(self.cond)>0:
                    cond_flg = True if self.op_flag==1 else False
                    exit_flag = True
                    for ind,key in enumerate(i):
                        for ln, args, in enumerate(self.arg1):
                            if args in Output[0][ind]:
                                exit_flag = False
                                if self.cond[ln] == '>' and col[ind]:
                                    if self.op_flag == 1:
                                        cond_flg = cond_flg and (True if int(key) > int(self.arg2[ln]) else False)
                                    else:
                                        cond_flg = cond_flg or (True if int(key) > int(self.arg2[ln]) else False)
                                if self.cond[ln] == '<' and col[ind]: 
                                    if self.op_flag == 1:
                                        cond_flg = cond_flg and (True if int(key) < int(self.arg2[ln]) else False)
                                    else :
                                        cond_flg = cond_flg or (True if int(key) < int(self.arg2[ln]) else False)
                                if self.cond[ln] == '>='and col[ind]: 
                                    if self.op_flag == 1:
                                        cond_flg = cond_flg and (True if int(key) >= int(self.arg2[ln]) else False)
                                    else:
                                        cond_flg = cond_flg or (True if int(key) >= int(self.arg2[ln]) else False)
                                if self.cond[ln] == '<='and col[ind]: 
                                    if self.op_flag == 1:
                                        cond_flg = cond_flg and (True if int(key) <= int(self.arg2[ln]) else False)
                                    else:
                                        cond_flg = cond_flg or (True if int(key) <= int(self.arg2[ln]) else False)
                                if self.cond[ln] == '=' and col[ind]: 
                                    if self.op_flag == 1:
                                        cond_flg = cond_flg and (True if int(key) == int(self.arg2[ln]) else False)
                                    else:
                                        cond_flg = cond_flg or (True if int(key) == int(self.arg2[ln]) else False)
                    
                    if cond_flg: Output.append(i)                    
                    if exit_flag:
                        print("Error: check your Where Query")
                        exit(0)
                else : Output.append(i)
        else:
            flag_bool = defaultdict(lambda : False)
            row = []
            for ind,key in enumerate(Output[0]):
                tb = key.split('.')[0]
                fld = key.split('.')[1]
                fn = ""
                for idx, i in enumerate(fields):
                    if fld in i and flag_bool[i] == False and col[ind]==True:
                        fn = i[:3].lower()
                        flag_bool[i] = True
                        break
                if fn == 'max' : val = self.max(self.table_field[tb][fld])
                if fn == 'min' : val = self.min(self.table_field[tb][fld])
                if fn == 'sum' : val = self.sum(self.table_field[tb][fld])
                if fn == 'avg' : val = self.avg(self.table_field[tb][fld])
                if fn == "" : val = 0
                row.append(val)
            Output.append(row)    
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