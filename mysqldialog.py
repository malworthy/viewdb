import pygtk
import gtk
import database

class MySqlDialog:
    def __init__(self):
        #Set the Glade file
        builder = gtk.Builder()
        builder.add_from_file("viewdb.glade") 
        self.databaseId = 0
        self.dialog = builder.get_object("MySqlDialog")
        self.databaseEntry = builder.get_object("databaseEntry")
        self.serverEntry = builder.get_object("serverEntry")
        self.userEntry = builder.get_object("userEntry")
        self.passwordEntry = builder.get_object("passwordEntry")
        self.descriptionEntry = builder.get_object("descriptionEntry")
        builder.connect_signals(self)
        
    def on_okButton_clicked(self, widget, data=None):
        print "OK Button Clicked"
        dl = database.DatabaseList()
        self.databaseId = dl.add_mysql_database(self.databaseEntry.get_text(), \
            self.serverEntry.get_text(), \
            self.userEntry.get_text(), \
            self.passwordEntry.get_text(), \
            self.descriptionEntry.get_text())
