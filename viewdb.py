#!/usr/bin/env python

import sys
import database
from  mysqldialog import MySqlDialog
import sqlite3
import re
from datetime import datetime
import pygtk
import gtk
    
class MainWindow:
    
    def load_tables(self):
        self.tsTables.clear()
        tbls = self.db.tables()
        for table in tbls:
            iter = self.tsTables.append(None,[table])
            fields = self.db.fields(table)
            for field in fields:
                self.tsTables.append(iter, [field])
                
    def clear_grid_columns(self):
        for delcol in self.tvResult.get_columns():
            self.tvResult.remove_column(delcol)
                
    def run_sql(self, sql):
        # cell renderer
        cellrend = gtk.CellRendererText()
        start_time = datetime.today()
        try:
            self.history.add(sql, self.databaseId)
            self.db.execute(sql)
        #except sqlite3.OperationalError as err:
        except Exception as err:
            print err
            self.tvResult.set_model(None)
            self.clear_grid_columns()
            tvcolumn = gtk.TreeViewColumn("SQL Error")
            tvcolumn.pack_start(cellrend, True)
            tvcolumn.set_attributes(cellrend, text=0)
            self.tvResult.append_column(tvcolumn)
            liststore = gtk.ListStore(str)
            liststore.append([err])
            self.tvResult.set_model(liststore)
            return
            
            
        colnames = self.db.column_names()
        if colnames == None:
            time_taken = datetime.today() - start_time
            status_text = "%s rows changed in %d.%d seconds" % (self.db.rowcount(), time_taken.seconds, time_taken.microseconds)
            self.statusbar.push(self.context_id, status_text)
            return
        
        liststore = gtk.ListStore(*([str] * len(colnames)))
        rowcount = 0
        timeout = ""
        time_taken = datetime.today() - start_time
        for row in self.db.result():
            liststore.append(row)
            rowcount += 1
            time_taken = datetime.today() - start_time
            if time_taken.seconds > 30:
                timeout = "+"
                break
        status_text = "%s%s rows in %d.%d seconds" % (rowcount, timeout, time_taken.seconds, time_taken.microseconds)
        self.update_statusbar(status_text)
        #self.statusbar.push(self.context_id, status_text)
        self.tvResult.set_model(None)
       
        # remove all columns
        self.clear_grid_columns()
        
        # add columns for sql
        colnum = 0
        for col in colnames:
            tvcolumn = gtk.TreeViewColumn(col)
            tvcolumn.pack_start(cellrend, True)
            tvcolumn.set_attributes(cellrend, text=colnum)
            colnum +=1
            self.tvResult.append_column(tvcolumn)
            
        self.tvResult.set_model(liststore)
        
    def update_statusbar(self, text = "Database:"):
        status_text = "%s  %s" % (text, self.db.dbname)
        self.statusbar.push(self.context_id, status_text)
        
    def set_database_combo(self, databaseId):
        index = 0
        model = self.cboDbList.get_model()
        for item in model:
            print "id?", item[1]
            if item[1] == databaseId:
                self.cboDbList.set_active(index)
                return
            index += 1

        
    def set_database(self, dbname, databaseId):
        self.dbname = dbname
        self.databaseId = databaseId
        db_list = database.DatabaseList()
        self.db = db_list.get_db_conn(databaseId)

        self.load_tables()
        self.update_statusbar()
        if (databaseId > 0):
            self.set_database_combo(databaseId)
            
    def format_query(self, sql):
        sql = sql.replace('\n',' ')
        sql = re.sub(r'([\s]+)', ' ', sql)
        words = ['from', 'left join', 'right join', 'inner join', 'where', 'set', 'order by', 'group by', 'union', 'and', 'or']
        for word in words:
            sql = re.sub(r'(?i)\s' + word + '[\s\n]', '\n' + word.upper() + ' ', sql )
        return sql
    
    def get_selected_text(self):
        buf = self.txtQuery.get_buffer()
        bounds = buf.get_selection_bounds()
        if len(bounds) == 0:
            text = ""
        else:
            start,end = bounds
            text = buf.get_text(start, end)
        if len(text) < 6:
            start,end = buf.get_bounds()
            text = buf.get_text(start, end)
        return text
    
    def on_butFormat_clicked(self, widget, data=None):
        text = self.get_selected_text() 
        self.set_query_text(self.format_query(text))
        
    def on_menuOpenDb_activate(self, widget, data=None):
        print "open menu clicked"
        chooser = gtk.FileChooserDialog(title=None,action=gtk.FILE_CHOOSER_ACTION_OPEN,
                                        buttons = (gtk.STOCK_CANCEL,gtk.RESPONSE_CANCEL,
                                        gtk.STOCK_OPEN,gtk.RESPONSE_OK))
        response = chooser.run()
        if response == gtk.RESPONSE_OK:
            # TODO: Get databaseId, if none found add to combo and reload
            dbname = chooser.get_filename()
            db_id = self.history.get_databaseId(dbname)
            self.refresh_db_list()
            self.set_database(dbname, db_id)
        else:
            print "cancel pressed"
        chooser.destroy()
        
    def on_menuOpenMyDb_activate(self, widget, data=None):
        mysql_dlg = MySqlDialog()
        mysql_dlg.dialog.run()
        if mysql_dlg.databaseId > 0:
            print "DatabaseID: ", mysql_dlg.databaseId
            self.refresh_db_list()
            self.set_database("", mysql_dlg.databaseId)
        
        mysql_dlg.dialog.destroy()
        print "after dialog shown"

    def set_query_text(self, text):
        buf = self.txtQuery.get_buffer()
        buf.set_text(text)

    def on_butRun_clicked(self, widget, data=None):
        text = self.get_selected_text()        
        self.run_sql(text)
        
    def on_butPrev_clicked(self, widget, data=None):
        text = self.history.prev()
        if text != "":
            self.set_query_text(text)
        
    def on_butNext_clicked(self, widget, data=None):
        text = self.history.next()
        if text != "":
            self.set_query_text(text)
        
    def on_tvTables_row_activated(self, treeview, path, view_column):
        print "double click tables"
        treeselection = treeview.get_selection()
        model, iter = treeselection.get_selected()

        parent_iter = model.iter_parent(iter) 
        if parent_iter == None:
            tablename = model.get_value(iter, 0)
            sql = "SELECT * FROM %s%s%s" % (self.db.left_quote, tablename, self.db.right_quote)
        else:
            tablename = model.get_value(parent_iter, 0)
            fieldname = model.get_value(iter, 0) 
            sql = "SELECT * FROM %s ORDER BY %s" % (tablename,fieldname)
        buf = self.txtQuery.get_buffer()
        buf.set_text(sql)
        self.run_sql(buf.get_text(buf.get_start_iter(), buf.get_end_iter()))
        
    def get_selected_dburi(self):
        model = self.cboDbList.get_model()
        active = self.cboDbList.get_active()
        if active < 0:
            return None
        return model[active][2]
        
    def get_selected_dbid(self):
        model = self.cboDbList.get_model()
        active = self.cboDbList.get_active()
        if active < 0:
            return None
        return model[active][1]
    
    def on_cboDbList_changed(self, widget, data=None):
        dbname = self.get_selected_dburi()
        dbid = self.get_selected_dbid()
        self.set_database(dbname, dbid)
        #if dbname != None:
            

    def on_butExit_clicked(self, widget, data=None):
        self.tvResult.set_model(None)
        gtk.main_quit()
        
    def on_entry_filter_changed(self, widget, data=None):
        print "on entry filter changed"
        self.tvTables.set_model(None)
        self.filter_text = self.entry_filter.get_text()
        self.tables_filter.refilter()
        self.tvTables.set_model(self.tables_filter)
    
    def refresh_db_list(self):
        self.dbliststore.clear()
        for row in self.history.list_databases():
            self.dbliststore.append(row)
            
    def apply_filter(self, model, iter, data):
        filtertext = self.entry_filter.get_text()
        value = model.get_value(iter, 0)
        #print value
        if filtertext == "" or value == None:
            return True
        else:
            return filtertext.upper() in value.upper()

    def __init__(self):
        self.history = database.QueryHistory()
        
        #Set the Glade file
        builder = gtk.Builder()
        builder.add_from_file("viewdb.glade")  
        
        self.tvTables = builder.get_object("tvTables")
        self.tvResult = builder.get_object("tvResult")
        self.txtQuery = builder.get_object("txtQuery")
        self.statusbar = builder.get_object("statusbar")
        self.context_id = self.statusbar.get_context_id("viewdb")
        self.cboDbList = builder.get_object("cboDbList")
        self.entry_filter = builder.get_object("entry_filter")
        
        # treestore for table list
        self.filter_text = ""
        self.tsTables = gtk.TreeStore(str)
        self.tables_filter = self.tsTables.filter_new()
        self.tables_filter.set_visible_func(self.apply_filter, self.filter_text)
        
        # columns
        self.colTables = gtk.TreeViewColumn('Tables')
        
        # cell renderer
        self.tableCellRend = gtk.CellRendererText()
        
        # add cell rederer to column
        self.colTables.pack_start(self.tableCellRend, True)
        
        # set the cell attributes to the appropriate liststore column
        self.colTables.set_attributes(self.tableCellRend, text=0)
        
        self.tvTables.set_model(self.tables_filter)
        self.tvTables.append_column(self.colTables)
        self.history.get_last_used()
        
        # set last query used.
        self.set_query_text(self.history.query)
        
        #fill database combo
        self.dbliststore = builder.get_object("dbliststore")
        cell = gtk.CellRendererText()
        self.cboDbList.pack_start(cell, True)
        self.cboDbList.add_attribute(cell, 'text', 0) 
        self.refresh_db_list()
    
        #Get the Main Window, and connect the "destroy" event
        self.window = builder.get_object("MainWindow")
        builder.connect_signals(self)
        self.window.show_all()
        self.set_database(self.history.uri, self.history.databaseId)
        if (self.window):
            self.window.connect("destroy", self.on_butExit_clicked)
        else:
            print "some error creating window"
            
if __name__ == "__main__":
    hwg = MainWindow()
    gtk.main()
