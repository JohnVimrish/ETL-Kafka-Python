import Connection_configuration as c_config
import  psycopg2  
import append_data_to_lists as app_d
import class_definition as cls 

target_connection = c_config.DatabaseConnection()

def execute_many(table,cursor,query ,records,load_type)  :
    lenofrecords  = int(len(records))
    rows_inserted = 0 
    actual_insert = 0 
    for position in range(lenofrecords) :
        try:
            records_to_be_inserted= records[position]
            cursor.execute(query,records_to_be_inserted)
            rows_inserted +=1
            actual_insert +=1 
            if ((actual_insert== lenofrecords) or  (rows_inserted  == 1000)) :
                    target_connection.commit()
            if (rows_inserted  >= 1000)  :
                rows_inserted=1                   
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            target_connection.rollback()
            target_connection.close()
    print( "{} Record inserted successfully into Table -{} using {} Query.".format(lenofrecords,table,load_type))
    


def load_data_to_tables ( topic_name ) :
       query_json            = cls.Configuration.load_json('C:\Kafka Test CDC(Change Data Capture)\Kafka Test CDC(Change Data Capture)\Queries.json')   
       postgres_connection   = target_connection.cursor()

       if  topic_name == 'DESKTOP_9JC68P1.C__DBZUSER.CONTACTS' :
          table_name    = query_json.contacts.target_table_name
          insert_query  = query_json.contacts.insert_query
                  
          if len(app_d.contacts_insert_tuples) != 0  :
            load_type = 'INSERT'
            execute_many(table_name,postgres_connection,insert_query,app_d.contacts_insert_tuples,load_type)
            app_d.contacts_insert_tuples.clear()
    
       elif topic_name == 'DESKTOP_9JC68P1.C__DBZUSER.COUNTRIES' :
          table_name    = query_json.countries.target_table_name
          insert_query  = query_json.countries.insert_query
          
          if len(app_d.countries_insert_tuples) != 0  :
             load_type = 'INSERT'
             execute_many(table_name,postgres_connection,insert_query,app_d.countries_insert_tuples,load_type)
             app_d.countries_insert_tuples.clear()

       elif topic_name == 'DESKTOP_9JC68P1.C__DBZUSER.CUSTOMERS' :  
            table_name    = query_json.customers.target_table_name
            insert_query  = query_json.customers.insert_query
        
            if len(app_d.customers_insert_tuples) != 0  :
               load_type = 'INSERT'
               execute_many(table_name,postgres_connection,insert_query,app_d.customers_insert_tuples,load_type)    
               app_d.customers_insert_tuples.clear()

       elif topic_name == 'DESKTOP_9JC68P1.C__DBZUSER.EMPLOYEES' :       
           table_name    = query_json.employees.target_table_name
           insert_query  = query_json.employees.insert_query
           delete_query  = query_json.employees.delete_query
       
           if len(app_d.employees_insert_tuples) != 0  :
                     load_type = 'INSERT'
                     execute_many(table_name,postgres_connection,insert_query,app_d.employees_insert_tuples,load_type)  
                     app_d.employees_insert_tuples.clear()
           if len(app_d.employees_del_tuples) != 0  :
                     load_type = 'DELETE'
                     execute_many(table_name,postgres_connection,delete_query,app_d.employees_del_tuples,load_type)  
                     app_d.employees_del_tuples.clear()

       elif topic_name == 'DESKTOP_9JC68P1.C__DBZUSER.INVENTORIES' :      
           table_name    = query_json.inventories.target_table_name
           insert_query  = query_json.inventories.insert_query
        
           if len(app_d.invent_insert_tuples) != 0  :
             load_type = 'INSERT'
             execute_many(table_name,postgres_connection,insert_query,app_d.invent_insert_tuples,load_type)
             app_d.invent_insert_tuples.clear()

       elif topic_name == 'DESKTOP_9JC68P1.C__DBZUSER.KAFKA_TEST' :      
          table_name    = query_json.kafka_test.target_table_name
          insert_query  = query_json.kafka_test.insert_query
        
          if len(app_d.k_test_insert_tuples) != 0  :
             load_type = 'INSERT'
             execute_many(table_name,postgres_connection,insert_query,app_d.k_test_insert_tuples,load_type)  
             app_d.k_test_insert_tuples.clear()

       elif topic_name == 'DESKTOP_9JC68P1.C__DBZUSER.KAFKA_TEST2' :  
            table_name    = query_json.kafka_test2.target_table_name
            insert_query  = query_json.kafka_test2.insert_query
        
            if len(app_d.k_test2_insert_tuples) != 0  :
             load_type = 'INSERT'
             execute_many(table_name,postgres_connection,insert_query,app_d.k_test2_insert_tuples,load_type) 
             app_d.k_test2_insert_tuples.clear()

       elif topic_name == 'DESKTOP_9JC68P1.C__DBZUSER.LOCATIONS' :  
            table_name    = query_json.locations.target_table_name
            insert_query  = query_json.locations.insert_query
       
            if len(app_d.loc_insert_tuples) != 0  :
                load_type = 'INSERT'
                execute_many(table_name,postgres_connection,insert_query,app_d.loc_insert_tuples,load_type) 
                app_d.loc_insert_tuples.clear()

       elif topic_name == 'DESKTOP_9JC68P1.C__DBZUSER.WAREHOUSES' :     
            table_name    = query_json.warehouses.target_table_name
            insert_query  = query_json.warehouses.insert_query
       
            if len(app_d.warehouses_insert_tuples) != 0  :
               load_type = 'INSERT'
               execute_many(table_name,postgres_connection,insert_query,app_d.warehouses_insert_tuples,load_type) 
               app_d.warehouses_insert_tuples.clear()

       elif topic_name == 'DESKTOP_9JC68P1.C__DBZUSER.REGIONS' :      
             table_name    = query_json.regions.target_table_name
             insert_query  = query_json.regions.insert_query
       
             if len(app_d.regions_insert_tuples) != 0  :
              load_type = 'INSERT'
              execute_many(table_name,postgres_connection,insert_query,app_d.regions_insert_tuples,load_type)
              app_d.regions_insert_tuples.clear()

       elif topic_name == 'DESKTOP_9JC68P1.C__DBZUSER.PRODUCT_CATEGORIES' :     
             table_name    = query_json.product_categories.target_table_name
             insert_query  = query_json.product_categories.insert_query
       
             if len(app_d.prod_cat_insert_tuples) != 0  :
                load_type = 'INSERT'
                execute_many(table_name,postgres_connection,insert_query,app_d.prod_cat_insert_tuples,load_type)   
                app_d.prod_cat_insert_tuples.clear()

       elif topic_name == 'DESKTOP_9JC68P1.C__DBZUSER.PRODUCTS' : 
            table_name    = query_json.products.target_table_name
            insert_query  = query_json.products.insert_query
       
            if len(app_d.prod_insert_tuples) != 0  :
              load_type = 'INSERT'
              execute_many(table_name,postgres_connection,insert_query,app_d.prod_insert_tuples,load_type)   
              app_d.prod_insert_tuples.clear()

       elif topic_name == 'DESKTOP_9JC68P1.C__DBZUSER.ORDER_ITEMS' :   
            table_name    = query_json.order_items.target_table_name
            insert_query  = query_json.order_items.insert_query
                   
            if len(app_d.ord_itm_insert_tuples) != 0  :
                load_type = 'INSERT'
                execute_many(table_name,postgres_connection,insert_query,app_d.ord_itm_insert_tuples,load_type)  
                app_d.ord_itm_insert_tuples.clear()

       elif topic_name == 'DESKTOP_9JC68P1.C__DBZUSER.ORDERS' :
            table_name    = query_json.orders.target_table_name
            insert_query  = query_json.orders.insert_query
       
            if len(app_d.ord_insert_tuples) != 0  :
               load_type = 'INSERT'
               execute_many(table_name,postgres_connection,insert_query,app_d.ord_insert_tuples,load_type) 
               app_d.ord_insert_tuples.clear()
       
       elif topic_name =='DESKTOP_9JC68P1.C__DBZUSER.MILLION_RECORD_TEST' :
            table_name    = query_json.million_record_test.target_table_name
            insert_query  = query_json.million_record_test.insert_query
       
            if len(app_d.million_test_ins_tuples) != 0  :
               load_type = 'INSERT'
               execute_many(table_name,postgres_connection,insert_query,app_d.million_test_ins_tuples,load_type) 
               app_d.million_test_ins_tuples.clear()

       elif topic_name =='DESKTOP_9JC68P1.C__DBZUSER.DATA_TYPE_TEST' :
            table_name    = query_json.data_type_test.target_table_name
            insert_query  = query_json.data_type_test.insert_query
       
            if len(app_d.data_type_test_ins_tuples) != 0  :
               load_type = 'INSERT'
               execute_many(table_name,postgres_connection,insert_query,app_d.data_type_test_ins_tuples,load_type) 
               app_d.data_type_test_ins_tuples.clear()
            

       else :
           print("Code not writtern for Topic :",topic_name)
         

