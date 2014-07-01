import urllib2, json, unicodedata, difflib, datetime
import xml.etree.ElementTree as ET
import pandas as pd
import numpy as np
# import pandas.io.sql as psql

#from link import Link
from link import lnk

Testing = False

b = '/home/babraham/oa_etl/'

if Testing:
    #lnk = Link(b + 'link2.config')
    creds_file = b + 'pass2.config' 
else:
    #lnk = Link(b + 'link.config')
    creds_file = b + 'pass.config'

fields_file = b + 'field.config'

with open(creds_file) as f:
    creds = json.load(f)
    creds = creds['credentials']

# SET OPEN AIR LOGIN PARAMS

# this value sets size limit of each query
# OpenAir max query row size is 1000
batch_rowsize = 1000

# query xml templates
# to be combined into a single xml query
auth_header = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
auth_header += '<request API_ver="1.0" client="test app" client_ver="1.1" '
auth_header += 'namespace="'+ creds['namespace'] +'" key="'+ creds['key'] +'">'

api_url = creds['url']
login_request = '<Auth><Login><company>'+ creds['company'] +'</company>'
login_request += '<user>'+ creds['user'] +'</user>'
login_request += '<password>'+ creds['password'] +'</password></Login></Auth>'

object_request_template = '<Read type="%s" enable_custom="1" method="all" limit="%s,%s" %s/>'

auth_footer = "</request>"

def pull_object_data(oa_object, include_fields=None):
    """ Function to make a call to OA service for a given object name. Download the XML guide for a full list of objects here: 
    https://corpwiki.appnexus.com/download/attachments/53090297/NetSuiteOpenAirXMLAPIGuide.pdf?version=1&modificationDate=1387477324239&api=v2
    
    Params:
    oa_object=name of the object to pull in (case sensitive). Example: Project, User, Task
    fields=list of fields to return. If none (default), returns all fields.
    """
    
    start_rec = 0
    # list of dicts holds result data
    result_rows = []
    
    while True:
        # set select query batch limit (0,249; 250,499; etc.)
        end_rec = start_rec + batch_rowsize - 1
            
        return_fields = ''
        field_filter = ''
        if include_fields and False:
            for field in include_fields:
                field_filter += '<'+ field +'/>'
            return_fields = '<_Return>'+ field_filter +'</_Return>'

        # make request over web
        req = urllib2.Request(api_url)
        object_request = (object_request_template % 
            (oa_object, start_rec, batch_rowsize, return_fields))
        complete_request = auth_header + login_request + object_request + auth_footer
        
        req.add_data(complete_request)
        response = urllib2.urlopen(req)
        
        # parse XML into a node tree
        xml_response = response.read()
        #print xml_response
        tree = ET.fromstring(xml_response)
        nodes = tree[1]
        
        # if this query returns nothing, we're at end of result set
        if len(nodes) < 1:
            break
            
        stophere = False
    
        # each single row of data
        for row_object in nodes:
            row_dict = {}
    
            for element in row_object:
                if include_fields is None or element.tag in include_fields:
                    
                    if len(element) == 0 and element.text:
                        value = element.text.strip()
                    # handle date and datatime objects
                    elif len(element) == 1 and element[0].tag == 'Date':
                        # 'YYYY-MM-DD HH:MM:SS' in mysql

                        # year
                        value = element[0][6].text+'-' if element[0][6].text else '1900-'
                        # month
                        value += element[0][4].text+'-' if element[0][4].text else '00-'
                        # day
                        value += element[0][5].text+' ' if element[0][5].text else '00 '
                        # hour
                        value += element[0][0].text+':' if element[0][0].text else '00:'
                        # minute
                        value += element[0][1].text+':' if element[0][1].text else '00:'
                        # second
                        value += element[0][2].text if element[0][2].text else '00'

                        if not (element[0][0].text or element[0][1].text or element[0][2].text or 
                            element[0][4].text or element[0][5].text or element[0][6].text):
                            value = None
                        elif (element[0][6].text == '0000'):
                            value = None
                        else:
                            try:
                                tmp = datetime.datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                            except ValueError:
                                value = None
                    elif len(element) > 0:
                        value = element[0].text.strip()
                    else:
                        value = element.text

                    row_dict[element.tag] = value
                if stophere:
                    break
            result_rows.append(row_dict)
    
        # advance pointer to start of next select query batch
        start_rec = end_rec + 1
    
    df = pd.DataFrame(result_rows)
    return df


def create_table_schema(df, table_name):
    """Generates a SQL create table statement based off the columns (and their dtypes) in a Pandas dataframe"""
    sql_obj_mapping={'string':'VARCHAR (255)',
                     'integer':'INT',
                     'float':'FLOAT',
                     'datetime':'DATETIME'
                    }
    import datetime
    create_table_string="""CREATE TABLE %s (""" % table_name
    
    mapping_dict={}
    
    for col_name in df:
        # check if col is a datetime obj
        if df[col_name].apply(lambda x: isinstance(x, datetime.datetime)).any():
            py_obj_type='datetime'
        # check if col is a string
        elif df[col_name].str.contains('-\d').dropna().all():
            py_obj_type='date'
        elif df[col_name].str.contains('[a-z]').dropna().any():
            py_obj_type='string'
        #check if col is a float or integer
        else:
            try:
                df[col_name].dropna().apply(lambda x: int(x))
                py_obj_type='integer'
            except ValueError:
                py_obj_type='float'
        
        sql_object=sql_obj_mapping[py_obj_type]
         
        mapping_dict.update({col_name:sql_object})
        #print "%s: %s: %s" % (col_name, py_obj_type,sql_object)
        create_table_string+=col_name+' '
        create_table_string+=sql_object
        create_table_string+=','
        
    create_table_string=create_table_string[:-1]+');'
    
    return create_table_string


# based off of David Blaikie's fiba script library, dfutils
def insert_df(df, table_name, chunksize=1000, dbconnection=None, doinsert=False, do_manual_sqlescape=False, encoding='latin-1'):
    """
    Given a dataframe, table name and link db connection, insert data or return insert SQL. 
    Custom module created by David Blaikie.

    Args:
        df:  dataframe to be inserted
        table_name: name of table
        chunksize:  size of SQL insert chunk
        dbconnection:  available db connection object
        doinsert:  execute insert quer(ies).  If false, returns SQL statement or list of SQL statements
        do_manual_sqlescape:  do not use the db connection object to escape strings for SQL -- module handles it
        encoding:  only if a column's input data is unicode, encode to a string using this encoding

    Returns:
        string query, or list of string queries (if chunksize)
    """
    return_chunks = False
    df = df.copy()
    if doinsert and not dbconnection:
        exit("dataframe2insertsql:  when doinsert=True, must supply dbconnection object")

    def stringify_for_sql(i, this_type=None):
        """stringify, SQL-escape an object, adding quotes if it is a string"""
        if isinstance(i, basestring):
            if isinstance(i, unicode):
                try:
                    i = i.encode(encoding)
                except UnicodeEncodeError:
                    i = unicodedata.normalize('NFKD', i).encode(encoding, 'ignore')

            if dbconnection and not do_manual_sqlescape:
                i = dbconnection.escape_string(i)
            else:
                import MySQLdb
                MySQLdb.escape_string(SQL)

        if isinstance(i, basestring):
            i = "'" + str(i) + "'"
        else:
            try:
                if i is None or np.isnan(i):
                    i = 'NULL'
            except TypeError:
                pass
            i = str(i)

        return i


    # string escape every element in the dataframe
    df = df.applymap(stringify_for_sql)

    if chunksize:
        return_chunks = True

    if (not chunksize) or (chunksize >= len(df)):
        chunksize = len(df)

    sql_chunk_queries = []
    errant_queries = []
    headers = df.columns.values.tolist()

    for chunk in ( df[x:x+chunksize] for x in xrange(0, len(df), chunksize) ):
        query = "INSERT INTO %s\n" % table_name
        query += "(" + ','.join(headers) + ")"
        query += " VALUES \n"

        chunk = chunk.to_records(index=False)
        for tuples in chunk:
            row = "(" + ','.join(tuples) + "),\n"
            query += row
        query = query[:-2] + "\n"
        query += " ON DUPLICATE KEY UPDATE \n"

        for col in headers:
            query += "%s=VALUES(%s), " % (col, col)
        query = query[:-2]
        query += ";"

        if doinsert:
            # execute insert query
            # if it fails for whatever reason, split into individual queries and execute 
            try:
                dbconnection.execute(query)
            except Exception, e:
                splt = query.splitlines()
                foqueries = splt[2: len(splt) - 2]
                beginning = ''.join( splt[0: 2] )
                ending = ''.join( splt[len(splt) - 2: len(splt)] )
                print 'chunk query failed:  %s queries retrieved' % len(foqueries)
                print 'chunk query error:  \n%s' % str(e)
                print
                for foquery in foqueries:
                    try:
                        foquery = foquery.rstrip(',')
                        foquery = beginning + " " + foquery + " " + ending
                        dbconnection.execute(foquery)
                    except Exception, e:
                        errant_queries.append((foquery, str(e)))

            dbconnection.commit()

        else:
            sql_chunk_queries.append(query)

    if errant_queries:
        print "permanently failed queries: ", len(errant_queries)
        for query, this_exception in errant_queries:
            print query
            print this_exception
            print

    if doinsert:
        return True
    else:
        if len(sql_chunk_queries) > 1 or return_chunks:
          return sql_chunk_queries
        return sql_chunk_queries[0]



if  __name__ =='__main__':
    print 'Starting: ', str(datetime.datetime.now())

    # sf data
    #sfdb = lnk.dbs.fiba_sf
    sfdb_prod = lnk.dbs.fiba_sf_prod
    # oa data
    # oadb = lnk.dbs.fiba_oa
    oadb_prod = lnk.dbs.fiba_oa_prod

    # Set up fields to return for each OA object
    with open(fields_file) as f:
        fields = json.load(f)
        fields = fields['fields']

    # create dataframe tables for each OpenAir object. to be inserted into db
    # The order matters due to foreign key dependencies. ie All the Projects have to exist
    #    before you can add a Task.
    oa_objects = ['User', 'Customer', 'Project', 'Projectassign', 'Projecttask', 'Projecttaskassign', 
                  'Task', 'Booking', 'BookingByDay', 'BookingType', 'Jobcode', 'Projectstage']
    for oa_object in oa_objects:
        oa_df = pull_object_data(oa_object, fields[oa_object])
        # insert_df(oa_df, doinsert=True, table_name=oa_object, dbconnection=oadb)
        insert_df(oa_df, doinsert=True, table_name=oa_object, dbconnection=oadb_prod)
    del oa_df

    ### these objects require extra processing ###
    
    # Aggregate total hours worked in the Project table -------------
    project_df = pull_object_data('Project', fields['Project'])
    # project_df = oadb_prod.select_dataframe("SELECT * FROM Project")
    task_df = pull_object_data('Task', fields['Task'])
    # task_df = oadb_prod.select_dataframe("SELECT * FROM Task")

    # aggregate the task data by projectid and then merge total hours worked with project_df 
    for ind in task_df.index:
        task_df['hours'][ind] = float(task_df['hours'][ind])

    tasks_grouped = task_df.groupby('projectid')
    task_hours = pd.DataFrame(tasks_grouped.hours.sum())
    project_df = project_df.merge(task_hours,left_on='id',right_index=True,sort=False,how='left')
    project_df = project_df.rename(columns={'hours':'total_hours_worked'})
    project_df = project_df.where(pd.notnull(project_df), 0.0)

    # insert_df(project_df, doinsert=True, table_name='Project', dbconnection=oadb)
    insert_df(project_df, doinsert=True, table_name='Project', dbconnection=oadb_prod)
    del task_df
    del project_df


    # Add member id to Customer table ---------------
    customer_df = pull_object_data('Customer', fields['Customer'])
    customer_df['member_id'] = customer_df.member_sf_id__c.copy()
    # customer_df = oadb_prod.select_dataframe("SELECT * FROM Customer")

    # pull in the Member ID field from the SF database and merge it with the customer table
    sf_data = sfdb_prod.select_dataframe("SELECT * FROM member ORDER BY account_id_case_sensitive ASC, created_date DESC;")
    mapping = {}
    old_account_id = 'aa'
    for ind in sf_data.index:
        # eliminate duplicate accounts
        if sf_data['account_id_case_sensitive'][ind] == old_account_id:
            continue
        else:
            old_account_id = sf_data['account_id_case_sensitive'][ind]

        # OA stupidly only saves the first 15 digits of the SF account id
        sf_id_15 = sf_data['account_id_case_sensitive'][ind]
        sf_id_15 = sf_id_15[0:15]

        data = {'id':sf_data['appnexus_member_id'][ind], 'name':sf_data['appnexus_member_name'][ind]}
        if sf_id_15 in mapping.keys():
            mapping[ sf_id_15 ].append( data )
        else:
            mapping[ sf_id_15 ] = [ data ]

    for ind in customer_df.index:
        if customer_df['customer_sf_id__c'][ind] in mapping.keys():
            member_set = mapping[ customer_df['customer_sf_id__c'][ind] ]
            if len(member_set) == 1:
                customer_df['member_sf_id__c'][ind] = member_set[0]['id']
                customer_df['member_id'][ind] = member_set[0]['id']
            else:
                # name matching
                names = [d['name'] for d in member_set]
                answer = difflib.get_close_matches( customer_df['company'][ind], names, n=1, cutoff=0)
                answer = answer[0]
                for d in member_set:
                    if d['name'] == answer:
                        customer_df['member_sf_id__c'][ind] = d['id']
                        customer_df['member_id'][ind] = d['id']
        else:
            customer_df['member_sf_id__c'][ind] = None
            customer_df['member_id'][ind] = None

    # insert_df(customer_df, doinsert=True, table_name='Customer', dbconnection=oadb)
    insert_df(customer_df, doinsert=True, table_name='Customer', dbconnection=oadb_prod)
    del customer_df


