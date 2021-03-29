import psycopg2

def create_conn():
    config = {
        'dbname' : '',
        'host' : '',
        'port' : '',
        'user' : '',
        'password' : ''
    }
    try:
        conn=psycopg2.connect(dbname = config['dbname'],
                            host = config['host'],
                            port = config['port'],
                            user = config['user'],
                            password = config['password'])
    except Exception as err:
        print(err)
    return conn
 

conn = create_conn()
cur = conn.cursor()

cur.execute(
'''
select
  l.pid
-- count(distinct pid)
from pg_locks l
join pg_catalog.pg_class c ON c.oid = l.relation
join pg_catalog.pg_stat_activity a ON a.procpid = l.pid
where l.pid <> pg_backend_pid()
-- AND a.usename = 'five11_tactical_looker'
;
''')

pid = cur.fetchall()
pid_list = []

for p in pid:
    p = str(p).replace("(", "'")
    p = p.replace(",)", "'")
    pid_list.append(p)

for p in pid_list:
    print(f"SELECT pg_terminate_backend({p});")
    cur.execute(f"SELECT pg_terminate_backend({p});")
    
    
conn.close()
