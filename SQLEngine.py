import sys
import re

join_cndns=[]
def main():
    tables = {}
    tables = readMetaData(tables)
    tables = readFiles(tables)
    #tables['table2']['head'][0] = 'F'
    print tables
    sqlQuery(tables)

def readMetaData(tables):
    flag = 0
    f = open('./metadata.txt','r')
    for line in f:
        if line.strip() == "<begin_table>":
            flag = 1
        elif line.strip() == "<end_table>":
            flag = 0
        elif flag == 1:
            tname = line.strip()
            tables[tname] = {};
            tables[tname]['head'] = []
            flag = 2
        elif flag == 2:
        	tables[tname]['head'].append(line.strip());
    return tables

def readFiles(tables):
    for table in tables:
        tables[table]['data'] = []
        tables[table]['name'] = table
        with open('./' + table + '.csv','r') as f:
            for line in f:
                tables[table]['data'].append([int(field.strip('"')) for field in line.strip().split(',')])

    return tables

def sqlQuery(tables):
    query = str(sys.argv[1])
    #print(query)
    query = query.strip('"').strip()
    query = query.replace("SELECT ", "select ")
    query = query.replace("DISTINCT ", "distinct ")
    query = query.replace("FROM ", "from ")
    query = query.replace("WHERE ", "where ")
    query = query.replace("AND ", "and ")
    query = query.replace("OR ", "or ")
    query = query.replace("MIN", "min")
    query = query.replace("MAX", "max")
    query = query.replace("AVG", "avg")
    query = query.replace("SUM", "sum")
    parse(query, tables)

def display(table):
    print '\n'
    print ','.join(table['head'])
    for row in table['data']:
#        for value in row:
        print ','.join([str(value) for value in row])

def make_result(table, projections, d_flag, a_flag, s_flag):
    result = {}
    result['head'] = []
    result['data'] = []
    new = []
    #print "table",table
    if a_flag is None:
        indices = []

        if projections[0]=='*':
            for row in table['head']:
                new.append(row)
            projections = new
#            print "result projections",projections
            for pair in join_cndns:
                new = []
                for each in projections:
                    new.append(each)

                projections = new
        result['head'] = result['head'] + projections
        print "result in project",result
        for each in projections:
            #print each
            ind = table['head'].index(each)
            indices.append(ind)

        for each in table['data']:
            res = []
            for i in indices:
                res.append(each[i])
            result['data'].append(res)

        if d_flag:
            temp = sorted(result['data'])
            result['data'][:] = []
            for i in range(len(temp)):
                if i==0:
                    result['data'].append(temp[i])
                elif temp[i]!=temp[i-1]:
                    result['data'].append(temp[i])

        return result

    else:
        result['head'].append(a_flag + "(" + projections[0] + ")")
        ind = table['head'].index(projections[0])
        temp = []
        #print type(table['data'])
        for row in table['data']:
            #print type(row)
            temp.append(row[ind])

        value = 0
        if a_flag=='sum':
            value = sum(temp)
        elif a_flag=='avg':
            value = sum(temp)/len(temp)
        elif a_flag=='max':
            value = max(temp)
        elif a_flag=='min':
            value = min(temp)

        result['data'].append([value])

    return result


def semicolon_error(query, n):
    if query[n-1] != ';':
    	return 1
    else:
        return 0

def query_formatting_error(query):
    if bool(re.match('^select.*from.*', query)) is not True:
    	return 1
    else:
        return 0

def too_many_fields_error(a_flag, n):
    if a_flag=='sum' or a_flag=='avg' or a_flag=='min' or a_flag=='max':
        if n>1:
            return 1
    else:
        return 0

def table_existence_error(table, table_dict):
    if table not in table_dict:
        return 1
    else:
        return 0
#problem here
def field_existence_error(projections, tables, table_dict):
    # print projections
    # print tables
    #print table_dict
    for each in  projections:
        if '.' in each:
            each_one = each.split('.').pop(0)
            each_two = each.split('.').pop()
            each = each_two
        each_flag=0
        for table in tables:
            #print table_dict[table]['head'].index(each)
            if each in table_dict[table]['head']:
                each_flag = each_flag + 1
        if each_flag==0:
            return 1
    return 0

    #print each_flag
    if each_flag == len(projections):
        return 0
    else:
        return 1

def select(tables, cond, table_dict):
    #print cond
    result = {}
    n = len(tables)
    temp = []
    joined_table = {}
    joined_table['head'] = []
    joined_table['data'] = []

    if n==1:
        for each in table_dict[tables[0]]['head']:
            if len(each.split('.'))==1:
                temp.append(table_dict[tables[0]]['name'] + '.' + each)
            else:
                temp.append(each)
        joined_table['head'] = joined_table['head'] + temp
        for row in table_dict[tables[0]]['data']:
            joined_table['data'].append(row)


    if n==2:
        temp1=[]
        temp2=[]
        for each in table_dict[tables[0]]['head']:
            if len(each.split('.'))==1:
                temp1.append(table_dict[tables[0]]['name'] + '.' + each)
            else:
                temp1.append(each)

        for each in table_dict[tables[1]]['head']:
            if len(each.split('.'))==1:
                temp2.append(table_dict[tables[1]]['name'] + '.' + each)
            else:
                temp2.append(each)

        joined_table['head'] = joined_table['head'] + temp1 + temp2
        for row1 in table_dict[tables[0]]['data']:
            for row2 in table_dict[tables[1]]['data']:
                joined_table['data'].append(row1 + row2)

    #print "joined_table",joined_table
    result['head'] = []
    for i in joined_table['head']:
        result['head'].append(i)
    cond = re.sub('>=','>--',cond)
    cond = re.sub('<=','<--',cond)
    cond = re.sub('=','==',cond)


    conditions = cond.replace(' and ',",")
    conditions = conditions.replace(' or ',",")
    conditions = conditions.split(",")
    #print conditions
    # for join---------------- only
    for condition in conditions:
        x = re.match('.*==.*[a-zA-Z]+.*', condition.strip())
        #print x
    	if bool(x):
            #print 11111111
            temp1 = condition.strip()

            temp1 = temp1.split('==')[0]
            temp1 = temp1.strip()

            # temp1 = temp1.split('>')[0]
            # temp1 = temp1.strip()

            temp2 = condition.strip()
            temp2 = temp2.split('==')[1]
            temp2 = temp2.strip()

            # temp2 = temp2.split('>')[1]
            # temp2 = temp2.strip()
            # #print temp1, temp2
            join_cndns.append((temp1, temp2))

    cond = re.sub('>--','>=', cond)
    cond = re.sub('<--','<=', cond)

    for each in joined_table['head']:
    	cond = cond.replace(each, 'row[' + str(joined_table['head'].index(each)) + ']')

    result['data'] = []

    #print "cond",cond

    for row in joined_table['data']:
    	if eval(cond):
    		result['data'].append(row)
    #print "select result",result
    return result
    #print conditions
    #print result



def parse(query, table_dict):
    n = len(query)
    if semicolon_error(query, n):
        sys.exit('Semicolon missing')

    query = query[:n-1]

    if query_formatting_error(query):
        sys.exit('Invalid Query')

    projections = query.split('from')
    projections = projections[0]
    projections = projections.replace('select','').strip()
    #print projections

    a_flag = None
    aggregate = re.match('^(max|min|sum|avg)\(.*\)',projections)
    if aggregate:
        a_flag = projections.split('(')
        a_flag = a_flag[0]
        projections = projections.replace(a_flag,'').strip().strip('()')
    #print a_flag

    d_flag = 0
    distinct = re.match('^distinct\(.*\)',projections)
    if distinct:
        #temp = projections.split('(')
        #temp = temp[1
        projections = projections.replace('distinct(','').strip().strip('()')
        projections = projections.replace(')','')
        d_flag = 1
    #print d_flag

    projections = projections.split(',')
    proj_len = len(projections)

    for each in range(proj_len):
    	projections[each] = projections[each].strip()

    if too_many_fields_error(a_flag, proj_len):
        sys.exit('Too many fields')

    print "fields",projections
    s_flag = 0
    if proj_len==1 and projections[0]=='*':
        s_flag = 1

    tables = query.split('from')
    tables = tables[1]
    tables = tables.split('where')
    tables = tables[0]

    tables = tables.strip().split(',')
    #tables = tables.strip()
    tables_len = len(tables)

    for i in range(len(tables)):
        tables[i] = tables[i].strip()
    print "tables",tables

    for table in tables:
        if table_existence_error(table, table_dict):
            sys.exit('Non existent table -> ' + table)
    #print s_flag

    format = re.match('^select.*from.*where.*', query)

    if format:
        if s_flag==0:
            if field_existence_error(projections, tables, table_dict):
                sys.exit('Non existant field(s)')
        cond = query.split('where')[1].strip()
        temp = cond.replace(' and ',' ').replace(' or ',' ')
        #print temp
        cols = re.findall(r"[a-zA-Z][\w\.]*", temp)
        cols = list(set(cols))
        print "conditions",cols

        if field_existence_error(cols, tables, table_dict):
            sys.exit('Non existant field(s) in where condition')

        if s_flag==0:
            for i in range(proj_len):
                #if len(projections[i].split('.'))==1:
                for table in tables:
                    if projections[i] in table_dict[table]['head']:
                        projections[i] = table + '.' + projections[i]
                        break
                    else:
                        continue
        #print "projections",projections

        for each in cols:
            #if(len(each.strip('.'))==1):
            for table in tables:
                if each in table_dict[table]['head']:
                    cond = re.sub(each,table + '.' + each, ' ' + cond)
                    cond = cond.strip(' ')
                else:
                    continue
        print "condition",cond
        select_result = select(tables, cond, table_dict)
        result = make_result(select_result, projections, d_flag, a_flag, s_flag)
        display(result)

    else:
        if tables_len >= 2:
            sys.exit("Too many tables")

        if s_flag==0:
            for each in projections:
                if each in table_dict[tables[0]]['head']:
                    continue
                else:
                    sys.exit("Invalid field -> " + each)
        result = make_result(table_dict[tables[0]], projections, d_flag, a_flag, s_flag)
        display(result)

if __name__ == "__main__":
    main()
