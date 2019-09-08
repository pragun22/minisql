import sys
import sqlparse
import csv
import copy 
import re
from collections import defaultdict 
from sqlparse.sql import Where, Comparison, Parenthesis
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
    def joinTable(self,table,field1,field2, cond):
        out = []
        for row in table:
            if cond == '=':
                if int(row[field1]) == int(row[field2]): 
                    out.append(row)
            elif cond == '>':
                if int(row[field1]) > int(row[field2]): 
                    out.append(row)
            elif cond == '<':
                if int(row[field1]) < int(row[field2]): 
                    out.append(row)
            elif cond == '>=':
                if int(row[field1]) >= int(row[field2]): 
                    out.append(row)
            elif cond == '<=':
                if int(row[field1]) <= int(row[field2]): 
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
        if self.toks[-1][-1] != ';':
            print("Syntax error: semicolon missing")
            exit(0)
        try :
            self.toks = self.toks[1:]
            if self.toks[-1][-1]==";":
                self.toks[-1] = self.toks[-1][:-1]
                if self.toks[-1]=="":
                    self.toks = self.toks[:-1]
        except:
            print("Parsing Error\ncheck your query")
            exit(0)
        if queryType != 'SELECT':
            print('query not yet supported.\nUse Select only')
            exit(0)
        else:  
            self.process()

    def process(self):
        # print(self.toks)
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
                where_fields = []
                for i in temp:
                    if i=="":continue
                    elif i.lower()=="and": self.op_flag = 1
                    elif i.lower()=="or": self.op_flag = 2
                    else :where_fields.append(i)
                if len(where_fields) > 4:
                    print("Error in where field check the operator\n")
                    exit(0)
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
            except Exception as e:
                print("Error in query")
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
            print("Error in databases name!!!\n")
            exit(0)
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
        runtable = copy.deepcopy(self.mat[tables[0]])
        for i in range(1, len(tables)):
            new_mat = []
            for j in runtable:
                for p in self.mat[tables[i]]:
                    new_mat.append(j+p)    
            runtable = copy.deepcopy(new_mat)
        for i in self.arg1:
            if not self.isInt(i):
                cnt = 0
                for j in Output[0]:
                    if i==j: cnt+=1
                    elif i==j.split('.')[1] : cnt+=1
                if cnt > 1 or cnt == 0:
                    print("Ambigious/Wrong column ",i)
                    exit(0)  
        for i in self.arg2:
            if not self.isInt(i):
                cnt = 0
                for j in Output[0]:
                    if i==j: cnt+=1
                    elif i==j.split('.')[1] : cnt+=1
                if cnt > 1 or cnt == 0:
                    print("Ambigious/Wrong column ",i)
                    exit(0)    
        for i in runtable:
            if len(self.cond)>0:
                cond_flg = True if self.op_flag==1 else False
                for ln, args in enumerate(self.arg1):
                    if not self.isInt(args):
                        cnt = 0
                        for coln, tb in enumerate(Output[0]):
                            if tb == args:
                                k1 = int(i[coln])
                                cnt += 1
                            elif tb.split('.')[1] == args:
                                cnt += 1
                                k1 = int(i[coln])
                        if cnt>1 or cnt == 0:
                            print("Ambiguos Column")
                            exit(0)
                    else : k1 = int(args)
                    if not self.isInt(self.arg2[ln]):
                        cnt = 0
                        for coln, tb in enumerate(Output[0]):
                            if tb == self.arg2[ln]:
                                k2 = int(i[coln])
                                cnt += 1
                            elif tb.split('.')[1] == self.arg2[ln]:
                                cnt += 1
                                k2 = int(i[coln])
                        if cnt>1 or cnt ==0:
                            print("Wrong Column")
                            exit(0)
                    else : k2 = int(self.arg2[ln])                   
                    if self.cond[ln] == '>' :
                        if self.op_flag == 1:
                            cond_flg = cond_flg and (True if k1 > k2 else False)
                        else:
                            cond_flg = cond_flg or (True if k1 > k2 else False)
                    if self.cond[ln] == '<' : 
                        if self.op_flag == 1:
                            cond_flg = cond_flg and (True if k1 < k2 else False)
                        else :
                            cond_flg = cond_flg or (True if k1 < k2 else False)
                    if self.cond[ln] == '>=': 
                        if self.op_flag == 1:
                            cond_flg = cond_flg and (True if k1 >= k2 else False)
                        else:
                            cond_flg = cond_flg or (True if k1 >= k2 else False)
                    if self.cond[ln] == '<=': 
                        if self.op_flag == 1:
                            cond_flg = cond_flg and (True if k1 <= k2 else False)
                        else:
                            cond_flg = cond_flg or (True if k1 <= k2 else False)
                    if self.cond[ln] == '=' : 
                        if not self.isInt(self.arg2[ln]) and self.arg2[ln] not in fields and self.arg2[ln] != args:
                            col[self.colnum[self.arg2[ln]]] = 0
                        elif not self.isInt(args) and args not in fields and self.arg2[ln] != args:
                            col[self.colnum[args]] = 0
                        if self.op_flag == 1:
                            cond_flg = cond_flg and (True if k1 == k2 else False)
                        else:
                            cond_flg = cond_flg or (True if k1 == k2 else False)
                if cond_flg: Output.append(i)                    
            else : Output.append(i)
        for ind,key in enumerate(fields):
                flag = True if len(tables)==1 else False
                if func_flag:
                    i1 = key.find('(')
                    i2 = key.find(')')
                    key = key[i1+1:i2]
                for i in tables:
                    if key not in self.meta[i] and len(tables) > 1: flag = True 
                if not flag:
                    print("Error: ambigous Column")
                    exit(0)
                
        if func_flag:
            temp = []
            st = []
            for idx1,key in enumerate(fields):
                fn = key[:3].lower()
                i1 = key.find('(')
                i2 = key.find(')')
                fld = key[i1+1:i2]
                st.append(key)
                for i in range(len(Output)):
                    if fld in Output[0][i]:
                        tp = [Output[rw][i] for rw in range(1,len(Output))]
                        if len(tp)==0 : break
                        if fn == 'max' : val = self.max(tp)
                        elif fn == 'min' : val = self.min(tp)
                        elif fn == 'sum' : val = self.sum(tp)
                        elif fn == 'avg' : val = self.avg(tp)
                        else: 
                            print("Function unrecognised!!")
                            exit(0)
                        temp.append(val)
                        break
            for i in st:
                print(i,end=", ")
            print("")
            for i in temp:
                print(i,end=", ")
            print("")
            exit(0)
        if self.distinct:
            fake = []
            for i in range(len(Output)):
                temp = []
                for ind, j in enumerate(Output[i]):
                    if col[ind] :
                        temp.append(j)
                if temp not in fake:
                    fake.append(temp)
            for i in fake:
                for j in i:
                    print(j,end=", ")
                print("")
        else:
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