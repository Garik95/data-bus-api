# coding=utf-8
import sqlalchemy as sa
import pyodbc
import pandas as pd
import sys
from datetime import date as dt

driver = 'ODBC Driver 17 for SQL Server'

targetpy = pyodbc.connect("Driver={"+driver+"};Server=HDESKNEW;Database=QueueMngmnt;uid=sa;pwd=Passw0rd!")
targetsa = sa.create_engine("mssql+pyodbc://sa:Passw0rd!@HDESKNEW/QueueMngmnt?driver=ODBC+Driver+17+for+SQL+Server&charset=utf8",echo=False)

# Данные главного сервера
# С главного сервера выйдем на остальные сервера
HQ = '10.10.12.24\sqlexpress'
DB = 'cds_main'
username = 'analytics'
password = '9764analytics'

#source = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};""Server="+HQ+";""Database="+DB+";""uid="+username+";pwd="+password)
source = pyodbc.connect("Driver={"+driver+"};""Server="+HQ+";""Database="+DB+";""uid="+username+";pwd="+password)

#date = '2021-02-01'
#date = sys.argv[1]

if len(sys.argv) > 1:
    date = sys.argv[1]
else:
    today = dt.today()
    date = today.strftime("%Y-%m-%d")


print(date)

branches_sql = 'SELECT [branch_id],[branch_name],[branch_host],[branch_ip],[branch_db_name],[branch_db_username],[branch_db_password] FROM [cds_main].[dbo].[cds_branch]'
# выгружаем все филиалы
branches = pd.read_sql_query(branches_sql,source)

# initialize empty dataframes
cds_service_name = pd.DataFrame([])
cds_deskuser = pd.DataFrame([])
cds_ticket = pd.DataFrame([])
cds_task = pd.DataFrame([])

for ind in branches.index:
    try:
        branch_id = branches.loc[ind]['branch_id']
        cn = pyodbc.connect("Driver={"+driver+"};""Server="+branches.loc[ind]['branch_host']+";""Database="+branches.loc[ind]['branch_db_name']+";""uid="+branches.loc[ind]['branch_db_username']+";pwd="+branches.loc[ind]['branch_db_password'])
        # full loads
        cds_service_name = cds_service_name.append(pd.read_sql_query('select * from cds_service_name where service_name_language_id = 3 and service_name_type = 0 and branch_id = ' + str(branch_id),cn),ignore_index=True)
        cds_deskuser = cds_deskuser.append(pd.read_sql_query('select * from cds_deskuser where branch_id = ' + str(branch_id),cn),ignore_index = True)
        # deltas
        cds_ticket = cds_ticket.append(pd.read_sql_query("SELECT b.service_name_text,a.*,substring(convert(varchar,a.ticket_date,104),7,4) + substring(convert(varchar,a.ticket_date,104),4,2) + substring(convert(varchar,a.ticket_date,104),1,2) as cd FROM cds_ticket a left join (SELECT * FROM cds_service_name where service_name_language_id = 3 and service_name_type = 0) b on a.branch_id = b.branch_id and a.ticket_service_id = b.service_name_service_id where a.branch_id = " + str(branch_id) + " and a.ticket_date > '" + date + " 00:00:00' order by ticket_get_time desc",cn),ignore_index=True)
        cds_task = cds_task.append(pd.read_sql_query("select b.service_name_text,c.deskuser_name,a.task_id,a.task_finished,a.branch_id,a.task_date,a.task_wait_time,a.task_exec_time from cds_task a left join (SELECT * FROM cds_service_name where service_name_language_id = 3 and service_name_type = 0) b on a.branch_id = b.branch_id and a.task_service_id = b.service_name_service_id left join cds_deskuser c on a.branch_id = c.branch_id and a.task_deskuser_id = c.deskuser_id where a.task_finished = 1 and a.branch_id = " + str(branch_id) + " and a.task_date = '" + date.replace('-','.') + "' order by task_date desc",cn),ignore_index=True)
    except:
        print('cannot connect to ',branches.loc[ind]['branch_host'])

def sqlcol(dfparam):    
    dtypedict = {}
    for i,j in zip(dfparam.columns, dfparam.dtypes):
        if "object" in str(j):
            dtypedict.update({i: sa.types.NVARCHAR(length=255)})
        if "float" in str(j):
            dtypedict.update({i: sa.types.Float(precision=3, asdecimal=True)})
        if "int" in str(j):
            dtypedict.update({i: sa.types.INT()})
    return dtypedict


# due to nvarchar(max) had to delete these columns
del cds_deskuser['deskuser_code']
del cds_deskuser['deskuser_nt_auth_name']
del cds_deskuser['deskuser_webdesk_state']
del cds_deskuser['deskuser_charid']
del cds_deskuser['deskuser_name_reporting']

branches.to_sql('branches',con=targetsa,if_exists='replace',index=False,dtype=sqlcol(branches))
cds_service_name.to_sql('cds_service_name',con=targetsa,if_exists='replace',index=False,dtype=sqlcol(cds_service_name))
cds_deskuser.to_sql('cds_deskuser',con=targetsa,if_exists='replace',index=False,dtype=sqlcol(cds_deskuser))

targetpy.cursor().execute("DELETE FROM cds_ticket where ticket_date >'" + date + " 00:00:00'").commit()
cds_ticket.to_sql('cds_ticket',con=targetsa,if_exists='append',index=False,dtype=sqlcol(cds_ticket))

targetpy.cursor().execute("DELETE FROM cds_task where task_date = '" + date.replace('-','.') + "'").commit()
cds_task.to_sql('cds_task',con=targetsa,if_exists='append',index=False,dtype=sqlcol(cds_task))
