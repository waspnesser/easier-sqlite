import sqlite3 as sl3

class sql_functions():
    def __init__(self,database_name):
        self.connect_database(database_name)

    def connect_database(self,database_name):#Girilen veritabanı konumu ile veritabanına bağlanılır.
        self.connection = sl3.connect(str(database_name))
        self.cursor = self.connection.cursor()

    def disconnect_from_database(self):#Veritabanından bağlantı kesilir.
        self.connection.close()

    def create_table(self,table_name,*column_information):
        #Veritabanında tablo yaratmaya yarar.Parametre girdisi şöyle olmalıdır
        #(tablo_ismi,(1.Kolon adı,1.Kolonun veri tipi),(2.Kolon adı,2.Kolonun veri tipi),(3.Kolon adı,3.Kolonun veri tipi) ...)

        if len(column_information) == 0:

            print("Column information is not entered!")
        else:

            column_info=""
            for i,j in column_information:
                column_info =column_info + str(i) + " " + str(j).upper() + ","
            column_info = column_info[:-1]

            query =  "CREATE TABLE IF NOT EXISTS '{}' ({})".format(table_name,column_info)
            self.cursor.execute(query)

    def insert_data(self,table_name,*information):
        # Veritabanına bilgi girişi yapar.Parametre girdisi şöyle olmalıdır.
        # (tablo_ismi,1.kolonun değeri,2.kolonun değeri,3.kolonun değeri,...)
        if len(information) == 0:
            print("Information is not entered.Procces has been killed...")
            return None
        else:
            info = ""
            for i in information:
                if i == None:
                    info += "\'\'"+","
                if isinstance(i,int):
                    info += str(i)+","
                elif isinstance(i,str):
                    info += "\'{}\'".format(i)+","

                #info += str(i)+","
            info = info[:-1]
            query = "INSERT INTO '{}' VALUES({})".format(table_name,info)
            self.cursor.execute(query)
            self.connection.commit()

    def update_data(self,table_name,*information,get_null = 0,print_query = 0):
        # Veritabanındaki veriyi günceller.Kolon isimlerinin başına "*" getirilmelidir.4 farklı parametre giriş şekli vardır.
        # 1-(tablo_ismi,Null olacak verinin kolon ismi,Null olacak veri,get_null=1)
        # 2-(tablo_ismi,güncellenecek verinin kolonu,verinin güncellendikten sonraki değeri,verinin güncellemeden önceki değeri(şu anki değeri))
        # 3-(tablo_ismi,Null olacak verinin kolon ismi,Null olacak veri,Başka herhangi bir kolon adı,şecilen kolonun güncellenecek verinin satırındaki değeri,get_null = 1)
        # 4-(tablo_ismi,Güncellenecek kolon,Diğer kolon,Güncellenecek kolonun yeni değeri,diğer kolonun değeri,get_null = 1)

        # 1,2'nci şekildeki parametreler eğer tabloda günncellenecek değer eşsiz ise kullanılır. Ama aynı kolonda aynı değere sahip birden fazla değer var ise 1,2'nci 
        # şeklin kullanılması uygun değildir.Kullanılırsa güncelleme istenen değere değilde bütün anı değerlere olacaktır ve bu karışıklığa sebep olacaktır.Bu sebepten
        # aynı kolonda tekrar eden değerler var ise 3,4'ncü parametre şekli kullanılmalıdır. 
        if len(information) == 0:
            print("Information is not entered.Update is not working without information!!")
            return None
        else:
            info = list()

            for i in information:
                if type(i) == int:
                    info.append(str(i))
                elif type(i) == str:
                    if i[0] == "*":
                        info.append(i[1:])
                    else:
                        info.append("\'{}\'".format(i))

                else:print("Invaild argument!!");return None

            if len(info) == 2 and get_null==1:#+

                print("# WARNING: \n\tYou are working in same column.\n\tSuch as your data if has same value in same collumn \n\tChange will affect the all same values. ")
                query = "UPDATE {} SET {} = NULL WHERE {} = {} ".format(table_name,info[0],info[0],info[1])
            elif len(info) == 3:#?

                print("# WARNING: \n\tYou are working in same column.\n\tSuch as your data if has same value in same collumn \n\tChange will affect the all same values. ")
                query = "UPDATE {} SET {} = {} WHERE {} = {} ".format(table_name,info[0],info[1],info[0],info[-1])
            elif len(info) == 4:#++
                if get_null == 1:

                    query =  "UPDATE {} SET {} = NULL WHERE {} = {} ".format(table_name,info[0],info[-2],info[-1])
                else:

                    query = "UPDATE {} SET {} = {} WHERE {} = {} ".format(table_name,info[0],info[-2],info[1],info[-1])
            else:print("Invalid Input!");return None
            if print_query == 1:print(query)
            
            self.cursor.execute(query)
            self.connection.commit()

    def delete_row(self,table_name,*information):
        # Bu fonksiyon belirli satırı siler.Parametre girişinde kolon isimleri "*" ile başlamalıdır.Parametreler şöyle olmalıdır:
        # (tablo_ismi,silinicek verinin kolon adı,silinicek veri)
        info = list()
        for i in information:
            if type(i) == int:
                    info.append(str(i))
            elif type(i) == str:
                if i[0] == "*":
                    info.append(i[1:])#Column names must be start with *
                else:
                    info.append("\'{}\'".format(i))
            else:print("Invaild argument!!");return None
        #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$4
        
        info[0] = info[0].replace("'","")
        query = "DELETE FROM \'{}\' WHERE {} = {}".format(table_name,info[0],info[1])


        self.cursor.execute(query)
        self.connection.commit()

    def get_columns(self,table_name):
        # Kullanıcıya belirli tablodaki bütün kolonları döndürür.
        query = "SELECT sql FROM sqlite_master WHERE tbl_name = '{}' AND type = 'table'".format(table_name)
        self.cursor.execute(query)
        create_table_query_listed = self.cursor.fetchall()
        if len(create_table_query_listed):
        
            create_table_query = create_table_query_listed[0][0]
            create_table_query_column_information = create_table_query[create_table_query.index("(")+1:-1]
            column_informations = create_table_query_column_information.split(",")
            column_information = [tuple(i.split(" ")) for i in column_informations]

            columns = [i[0] for i in column_information]
            column_types = [j[1] for j in column_information ]

            return columns
        
        else:
            return create_table_query_listed

    def get_whole_table(self,table_name):
        # Belirli bir tablodaki bütün değerleri getirir.

        query = "SELECT * FROM \'{}\'".format(table_name)
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def get_column_by_name(self,table_name,*columns):
        # Belirli bir tablodaki seçili kolonları döndürür.Parametre girdisi şöyledir.
        # (tablo_ismi,kolon1isim,kolon2isim,kolon3isim ... )

        cols = ""
        for i in columns:cols += i + ","
        cols = cols[:-1]

        query = "SELECT {} FROM \'{}\' ".format(cols,table_name)
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def find_column(self,table_name,specific_data):
        # Belirli bir tabloda belirli bir bilginin olduğu kolonu döndürür.

        query = "SELECT '{} FROM '{}'"
        for i in self.get_columns(table_name):
            query_passer = query.format(i,table_name)
            self.cursor.execute(query_passer)
            column = self.cursor.fetchall()
            for j in column:
                if specific_data in j:
                    return i

    def get_row(self,table_name,columnName,value):
        # Belirli bir tabloda belirli bir satırı siler.
        if type(value) == int:
            query = "SELECT * FROM {} WHERE {} = {}".format(table_name,columnName,value)
        else:
            query = "SELECT * FROM {} WHERE {} = {}".format(table_name,columnName,"\'{}\'".format(value))
        self.cursor.execute(query)
        row = self.cursor.fetchall()
        try:
            return list(row[0])
        except IndexError:
            return list()

    def get_row_count(self,table_name):
        # Belirli bir tablonun satır sayısını döndürür.Index Finding
        try:
            data = len(self.get_column_by_name(table_name,self.get_columns(table_name)[0]))
        except IndexError:
            data = 0
            
        return data

    def tableList(self):
        # Veritabanındaki tabloların listesini döndürür.
        query = "SELECT name FROM sqlite_master WHERE type='table';"
        self.cursor.execute(query)
        res = self.cursor.fetchall()
        return [name for name in res]

    def tableInformation(self,table_name):
        # Belirli tablonun bilgisini döndürür.
        query = "SELECT * FROM sqlite_master WHERE type='table';"
        self.cursor.execute(query)
        res = self.cursor.fetchall()    
        return res
    
    def removeTable(self,table_name):
        # Belirli bir tabloyu siler.
        query = "DROP TABLE IF EXISTS \'{}\' "
        self.cursor.execute(query.format(table_name))
        self.connection.commit()
    
    def getTableInfo(self,table_name):
        # tableInformation fonksiyonun başka şekilde elde edilmiş versiyonudur.
        query = "PRAGMA table_info(\'{}\')"
        self.cursor.execute(query.format(table_name))
        res = self.cursor.fetchall()
        return res

    def tableConf(self,table_name):
        # tableInformation,getTableInfo'nun daha farklı bir versiyonudur.
        res = self.getTableInfo(table_name)
        conf = []
        for a,name,dtype,d,e,f in res:
            conf.append((name,dtype))
        
        return conf

    def addColumn(self,table_name,columnInfo):
        #Oluşturulmuş bir tabloya yeni bir kolon ekler.
        wholeTable = self.get_whole_table(table_name)
        conf = self.tableConf(table_name)
        if columnInfo not in conf:
            conf.append(columnInfo)

            self.removeTable(table_name)
            self.create_table(table_name,*conf)

            for i in wholeTable:
                i = list(i)
                i.append(None)
                self.insert_data(table_name,*i)
        else:
            print("[EXSISTENCE ERROR]Specified Column Already Exists....")

    def get_row_with_ID(self,table_name,ID):#Update_1.1 # 13.01.2018 #d.2
        if self.get_row_count(table_name):# Eğer satır var ise kolon da var demektir.
            firstColumn = self.get_columns(table_name)[0]
            column = self.get_column_by_name(table_name,firstColumn)#İlk kolona göre inceleme yapılır.
            try:
                support_Data = column[ID][0]#Belirtilen ID'deki ilk kolondaki 
                requested_Data = self.get_row(table_name,firstColumn,support_Data)
                return requested_Data
                
            except IndexError:
                raise IndexError("Specified ID is out of range.")
        else:
            raise IndexError("Specified table has no record.")