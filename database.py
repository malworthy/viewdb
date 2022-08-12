import sqlite3
import MySQLdb
import os

class DatabaseList:
    def __init__(self):
        self.db = sqlite3.connect("queryhistory.db")
        
    def add_mysql_database(self, database, server, username, password, desc):
        sql = "INSERT INTO Database(Database, Server, UserName, Password, Name, DbType) VALUES(?,?,?,?,?,'MYSQL')"
        curs = self.db.cursor()
        print database, server, username, password, desc
        curs.execute(sql, [database, server, username, password, desc])
        dbid = curs.lastrowid 
        self.db.commit()
        return dbid
        
    def get_db_conn(self, databaseId):
        curs = self.db.cursor()
        curs.execute("SELECT  Server, Database, UserName, Password, DbType, Uri, Name FROM Database WHERE DatabaseId = ?", [databaseId]);
        row = curs.fetchone()
        if row[4] == "MYSQL":
            return MyDatabase(row[0], row[1], row[2], row[3]);
        else:
            return Database(row[5])

class QueryHistory:
    def __init__(self):
        db = sqlite3.connect("queryhistory.db")
        self.pointer = 0
        self.maxpointer = 0
        try:
            db.execute("Create Table History(Id integer primary key, Query text, DatabaseId int, Date text)")
            db.execute("Create table Database(DatabaseId integer primary key, Uri text, Name text)")
        except sqlite3.OperationalError:
            pass
        
    def get_dbname(self, dburi):
        if dburi == ":memory:":
            return "Memory"
        return os.path.basename(dburi)
    
    def get_databaseId(self, database):
        db = sqlite3.connect("queryhistory.db")
        curs = db.cursor()
        curs.execute("select DatabaseId from Database where uri = ?", [database])
        result = curs.fetchone()
        if result == None:
            curs.execute("Insert Into Database (Uri, Name) Values(?,?)", [database, self.get_dbname(database)])
            databaseId = curs.lastrowid
        else:
            databaseId = int(result[0])
        db.commit()
        return databaseId
    
    def add(self, query, databaseId = 0):
        db = sqlite3.connect("queryhistory.db")
        sql = "Insert Into History(Query, DatabaseId, Date) Values(?, ?, Date('Now'))"
        curs = db.cursor()
        #databaseId = self.get_databaseId(database)
        curs.execute(sql, [query, databaseId])
        
        db.commit()
        self.pointer = curs.lastrowid
        self.maxpointer = self.pointer
        
    def get_history(self, id):
        db = sqlite3.connect("queryhistory.db")
        curs = db.cursor()
        curs.execute("select query from History where Id = ?", [id])
        row = curs.fetchone()
        if row == None:
            return ""
        else:
            return row[0]
        
    
    def get_last_used(self):
        db = sqlite3.connect("queryhistory.db")
        curs = db.cursor()
        curs.execute("select DB.Name, Query, Id, H.DatabaseId, DB.Uri from \
            History H INNER JOIN Database DB on H.DatabaseId = DB.DatabaseId \
            order by id desc limit 1")
        result = curs.fetchone()
        if result == None:
            self.uri = ":memory:"
            self.dbname = "Memory"
            self.databaseId = 0
            self.query = ""
            self.pointer = 0
            self.maxpointer = 0
            return
        
        self.pointer = int(result[2])
        self.maxpointer = self.pointer
        
        self.dbname = result[0]
        self.query = result[1]
        self.databaseId = result[3]
        self.uri = result[4]
    
    def list_databases(self):
        db = sqlite3.connect("queryhistory.db")
        curs = db.cursor()
        curs.execute("select Name, DatabaseId, Uri from Database")
        return curs.fetchall()
    
    def prev(self):
        if self.pointer > 1:
            self.pointer -= 1
        return self.get_history(self.pointer)
        
    def next(self):
        if self.pointer < self.maxpointer:
            self.pointer += 1
        return self.get_history(self.pointer)
        
        
        

class Database:
    def __init__(self, dbname):
        self.left_quote = '['
        self.right_quote = ']'
        try:
            self.dbname = dbname
            self.conn = sqlite3.connect(dbname)
        except:
            self.dbname = ":memory:"
            self.conn = sqlite3.connect(self.dbname)
            
    def rowcount(self):
        return self.curs.rowcount
        
    def execute(self, sql):
        #qh = QueryHistory()
        #qh.add(sql, self.dbname)
        self.curs = self.conn.cursor()
        self.curs.execute(sql)
        self.conn.commit()
        
    def column_names(self):
        if self.curs.description == None:
            return None
        col_name_list = [tuple[0] for tuple in self.curs.description]
        return col_name_list
        
    def result(self):
        return self.curs #.fetchall()
        
    def tables(self, detail = False):
        tblcurs = self.conn.cursor()
        tblcurs.execute("select * from sqlite_master where type = 'table'")
        if detail == True:
            return tblcurs.fetchall()
        else:
            return [tuple[1] for tuple in tblcurs.fetchall()]
    
    def fields(self, tableName, detail = False):
        tblcurs = self.conn.cursor()
        tblcurs.execute("pragma table_info(%s)" % tableName)
        if detail == True:
            return tblcurs.fetchall()
        else:
            return [tuple[1] for tuple in tblcurs.fetchall()]


class MyDatabase:
    def __init__(self, server, dbname, username, password):
        self.left_quote = '`'
        self.right_quote = '`'
        self.conn = MySQLdb.connect(user=username,passwd=password,db=dbname, host=server,charset = "utf8", use_unicode = True)
        self.dbname = server + "/" + dbname
            
    def rowcount(self):
        return self.curs.rowcount
        
    def execute(self, sql):
        #qh = QueryHistory()
        #qh.add(sql, self.dbname)
        self.curs = self.conn.cursor()
        self.curs.execute(sql)
        if self.curs.description == None and self.curs.rowcount != 0:
            self.conn.commit()
        
    def column_names(self):
        if self.curs.description == None:
            return None
        col_name_list = [tuple[0] for tuple in self.curs.description]
        return col_name_list
        
    def result(self):
        return self.curs #.fetchall()
        
    def tables(self, detail = False):
        tblcurs = self.conn.cursor()
        tblcurs.execute("select table_name from Information_schema.tables where table_schema=%s order by table_name", (self.dbname,))
        if detail == True:
            return tblcurs.fetchall()
        else:
            return [tuple[0] for tuple in tblcurs.fetchall()]
    
    def fields(self, tableName, detail = False):
        tblcurs = self.conn.cursor()
        tblcurs.execute("select column_name from Information_schema.columns where table_schema=%s and table_name = %s order by ordinal_position", (self.dbname,tableName))
        if detail == True:
            return tblcurs.fetchall()
        else:
            return [tuple[0] for tuple in tblcurs.fetchall()]

