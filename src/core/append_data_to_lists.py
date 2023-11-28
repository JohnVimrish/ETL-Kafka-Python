import class_definition as c_d



contacts_insert_tuples    = list()
countries_insert_tuples   = list()
customers_insert_tuples   = list()
employees_insert_tuples   = list()
invent_insert_tuples      = list()
loc_insert_tuples         = list()
ord_insert_tuples         = list()
ord_itm_insert_tuples     = list()
prod_cat_insert_tuples    = list()
prod_insert_tuples        = list()
regions_insert_tuples     = list()
warehouses_insert_tuples  = list()
contacts_del_tuples       = list()
countries_del_tuples      = list()
customers_del_tuples      = list()
employees_del_tuples      = list()
invent_del_tuples         = list()
loc_del_tuples            = list()
ord_del_tuples            = list()
ord_itm_del_tuples        = list()
prod_cat_del_tuples       = list()
prod_del_tuples           = list()
regions_del_tuples        = list()
warehouses_del_tuples     = list()
million_test_ins_tuples   = list()
million_test_del_tuples   = list()
data_type_test_ins_tuples = list()
data_type_test_del_tuples = list()

def append_data(topic_name ,json_message) :
     kafkavalues = c_d.DataExtraction.load_json(json_message)  
     load_flg    = ['u','c','r']
     delete_flg  = ['d']

     if topic_name == 'DESKTOP_9JC68P1.C__DBZUSER.CONTACTS' :

          load_type       = 'I' if  kafkavalues.payload.op in load_flg else kafkavalues.payload.op
          del_flg         = 'Y' if kafkavalues.payload.op in delete_flg else 'N'

          if load_type    == 'I':
              contact_id      = c_d.convbase64toint(kafkavalues.payload.after.CONTACT_ID.value,kafkavalues.payload.after.CONTACT_ID.scale)
              first_name      = kafkavalues.payload.after.FIRST_NAME
              last_name       = kafkavalues.payload.after.LAST_NAME
              email           = kafkavalues.payload.after.EMAIL
              phone           = kafkavalues.payload.after.PHONE
              customer_id     = c_d.convbase64toint(kafkavalues.payload.after.CUSTOMER_ID.value,kafkavalues.payload.after.CUSTOMER_ID.scale)
              # appending all columns in form of tuple to the list 
              contacts_insert_tuples.append((contact_id,first_name,last_name,email,phone,customer_id))
          if del_flg == 'Y'  :
              contact_id      = c_d.convbase64toint(kafkavalues.payload.before.CONTACT_ID.value,kafkavalues.payload.before.CONTACT_ID.scale) if kafkavalues.payload.before is not None  else None
              contacts_del_tuples.append((contact_id))

     elif topic_name == 'DESKTOP_9JC68P1.C__DBZUSER.COUNTRIES' :
               
          load_type       = 'I' if  kafkavalues.payload.op in load_flg else kafkavalues.payload.op
          del_flg         = 'Y' if kafkavalues.payload.op in delete_flg else 'N'

          if load_type    == 'I':
              country_id      = kafkavalues.payload.after.COUNTRY_ID
              county_name     = kafkavalues.payload.after.COUNTRY_NAME
              region_id       = c_d.convbase64toint(kafkavalues.payload.after.REGION_ID.value,kafkavalues.payload.after.REGION_ID.scale) 
              # appending all columns in form of tuple to the list 
              countries_insert_tuples.append((country_id,county_name,region_id))
          if del_flg == 'Y'  :
              country_id      = kafkavalues.payload.before.COUNTRY_ID if kafkavalues.payload.before is not None  else None
              countries_del_tuples.append((country_id))
        
     elif topic_name == 'DESKTOP_9JC68P1.C__DBZUSER.CUSTOMERS' :
         
          load_type       = 'I' if  kafkavalues.payload.op in load_flg else kafkavalues.payload.op
          del_flg         = 'Y' if kafkavalues.payload.op in delete_flg else 'N'

          if load_type    == 'I':
              customer_id     = c_d.convbase64toint(kafkavalues.payload.after.CUSTOMER_ID.value,kafkavalues.payload.after.CUSTOMER_ID.scale)
              name            = kafkavalues.payload.after.NAME
              address         = kafkavalues.payload.after.ADDRESS
              website         = kafkavalues.payload.after.WEBSITE
              credit_limit    = c_d.convbase64toint(kafkavalues.payload.after.CREDIT_LIMIT,2)
              # appending all columns in form of tuple to the list 
              customers_insert_tuples.append((customer_id,name,address,website,credit_limit))

          if del_flg == 'Y'  :
              customer_id     = c_d.convbase64toint(kafkavalues.payload.before.CUSTOMER_ID.value,kafkavalues.payload.before.CUSTOMER_ID.scale) if kafkavalues.payload.before is not None  else None
              customers_del_tuples.append((customer_id))
              

     elif topic_name == 'DESKTOP_9JC68P1.C__DBZUSER.EMPLOYEES' :
          
       load_type             = 'I' if  kafkavalues.payload.op in load_flg else kafkavalues.payload.op
       del_flg               = 'Y' if kafkavalues.payload.op in delete_flg else 'N'
         
       if load_type          == 'I':
          emp_id                = c_d.convbase64toint(kafkavalues.payload.after.EMPLOYEE_ID.value,kafkavalues.payload.after.EMPLOYEE_ID.scale)
          first_name            = kafkavalues.payload.after.FIRST_NAME
          last_name             = kafkavalues.payload.after.LAST_NAME
          email                 = kafkavalues.payload.after.EMAIL
          phone                 = kafkavalues.payload.after.PHONE
          hire_dt               = c_d.convert_epochsec_to_dt(kafkavalues.payload.after.HIRE_DATE)
          mng_id                = kafkavalues.payload.after.MANAGER_ID if kafkavalues.payload.after.MANAGER_ID is not None else None
          job_title             = kafkavalues.payload.after.JOB_TITLE
          
          # appending all columns in form of tuple to the list 
          employees_insert_tuples.append((emp_id,first_name,last_name,email,phone,hire_dt,mng_id,job_title))
       if del_flg == 'Y'  :
            emp_id_del            = c_d.convbase64toint(kafkavalues.payload.before.EMPLOYEE_ID.value,kafkavalues.payload.before.EMPLOYEE_ID.scale) if kafkavalues.payload.before is not None  else None
            employees_del_tuples.append([emp_id_del])

           
     elif topic_name == 'DESKTOP_9JC68P1.C__DBZUSER.INVENTORIES' :

        load_type       = 'I' if  kafkavalues.payload.op in load_flg else kafkavalues.payload.op
        del_flg         = 'Y' if kafkavalues.payload.op in delete_flg else 'N'

        if load_type    == 'I':
              prod_id         = kafkavalues.payload.after.PRODUCT_ID
              warehouse_id    = kafkavalues.payload.after.WAREHOUSE_ID
              quantity        = kafkavalues.payload.after.QUANTITY

              # appending all columns in form of tuple to the list 
              invent_insert_tuples.append((prod_id,warehouse_id,quantity))
        if del_flg == 'Y'  :
              prod_id         = kafkavalues.payload.before.PRODUCT_ID if kafkavalues.payload.before is not None  else None
              warehouse_id    = kafkavalues.payload.before.WAREHOUSE_ID if kafkavalues.payload.before is not None  else None
              invent_del_tuples.append((prod_id,warehouse_id))

     elif topic_name == 'DESKTOP_9JC68P1.C__DBZUSER.LOCATIONS' :

          load_type       = 'I' if  kafkavalues.payload.op in load_flg else kafkavalues.payload.op
          del_flg         = 'Y' if kafkavalues.payload.op in delete_flg else 'N'

          if load_type    == 'I':
              loc_id          = c_d.convbase64toint(kafkavalues.payload.after.LOCATION_ID.value,kafkavalues.payload.after.LOCATION_ID.scale)
              address         = kafkavalues.payload.after.ADDRESS
              postal_code     = kafkavalues.payload.after.POSTAL_CODE
              city            = kafkavalues.payload.after.CITY
              state           = kafkavalues.payload.after.STATE if kafkavalues.payload.after.STATE is not None else None
              country_id      = kafkavalues.payload.after.COUNTRY_ID
              # appending all columns in form of tuple to the list 
              loc_insert_tuples.append((loc_id,address,postal_code,city,state,country_id))

          if del_flg == 'Y'  :
              loc_id          = c_d.convbase64toint(kafkavalues.payload.before.LOCATION_ID.value,kafkavalues.payload.before.LOCATION_ID.scale) if kafkavalues.payload.before is not None  else None
              loc_del_tuples.append((loc_id))
                       
     elif topic_name == 'DESKTOP_9JC68P1.C__DBZUSER.WAREHOUSES' :

          load_type       = 'I' if  kafkavalues.payload.op in load_flg else kafkavalues.payload.op
          del_flg         = 'Y' if kafkavalues.payload.op in delete_flg else 'N'

          if load_type    == 'I':
            warh_id         = c_d.convbase64toint(kafkavalues.payload.after.WAREHOUSE_ID.value,kafkavalues.payload.after.WAREHOUSE_ID.scale)
            warh_name       = kafkavalues.payload.after.WAREHOUSE_NAME
            loc_id          = kafkavalues.payload.after.LOCATION_ID
            # appending all columns in form of tuple to the list 
            warehouses_insert_tuples.append((warh_id,warh_name,loc_id))

          if del_flg == 'Y'  :
            warh_id         = c_d.convbase64toint(kafkavalues.payload.before.WAREHOUSE_ID.value,kafkavalues.payload.before.WAREHOUSE_ID.scale) if kafkavalues.payload.before is not None  else None
            warehouses_del_tuples.append((warh_id))
     elif topic_name == 'DESKTOP_9JC68P1.C__DBZUSER.REGIONS' :

          load_type       = 'I' if  kafkavalues.payload.op in load_flg else kafkavalues.payload.op
          del_flg         = 'Y' if kafkavalues.payload.op in delete_flg else 'N'

          if load_type    == 'I':
            reg_id          = c_d.convbase64toint(kafkavalues.payload.after.REGION_ID.value,kafkavalues.payload.after.REGION_ID.scale)
            region_name     = kafkavalues.payload.after.REGION_NAME
            # appending all columns in form of tuple to the list 
            regions_insert_tuples.append((reg_id,region_name))

          if del_flg == 'Y'  :
              reg_id          = c_d.convbase64toint(kafkavalues.payload.before.REGION_ID.value,kafkavalues.payload.before.REGION_ID.scale)
              regions_del_tuples.append((reg_id))
     elif topic_name == 'DESKTOP_9JC68P1.C__DBZUSER.PRODUCT_CATEGORIES' :
     
          load_type       = 'I' if  kafkavalues.payload.op in load_flg else kafkavalues.payload.op
          del_flg         = 'Y' if kafkavalues.payload.op in delete_flg else 'N'

          if load_type    == 'I':
              cat_id          = c_d.convbase64toint(kafkavalues.payload.after.CATEGORY_ID.value,kafkavalues.payload.after.CATEGORY_ID.scale)
              cat_name        = kafkavalues.payload.after.CATEGORY_NAME
              # appending all columns in form of tuple to the list 
              prod_cat_insert_tuples.append((cat_id,cat_name))
          if del_flg == 'Y'  :
              cat_id          = c_d.convbase64toint(kafkavalues.payload.before.CATEGORY_ID.value,kafkavalues.payload.before.CATEGORY_ID.scale)
              prod_cat_del_tuples.append((cat_id))  
              
     elif topic_name == 'DESKTOP_9JC68P1.C__DBZUSER.PRODUCTS' :
          load_type       = 'I' if  kafkavalues.payload.op in load_flg else kafkavalues.payload.op
          del_flg         = 'Y' if kafkavalues.payload.op in delete_flg else 'N'

          if load_type    == 'I':
              prod_id         = c_d.convbase64toint(kafkavalues.payload.after.PRODUCT_ID.value,kafkavalues.payload.after.PRODUCT_ID.scale)
              prod_name       = kafkavalues.payload.after.PRODUCT_NAME
              description     = kafkavalues.payload.after.DESCRIPTION
              stand_cost      = c_d.convbase64toint(kafkavalues.payload.after.STANDARD_COST,2)
              list_price      = c_d.convbase64toint(kafkavalues.payload.after.LIST_PRICE,2)
              cat_id          = c_d.convbase64toint(kafkavalues.payload.after.CATEGORY_ID.value,kafkavalues.payload.after.CATEGORY_ID.scale)
              # appending all columns in form of tuple to the list 
              prod_insert_tuples.append((prod_id,prod_name,description,stand_cost,list_price,cat_id))

          if del_flg == 'Y'  :
              prod_id         = c_d.convbase64toint(kafkavalues.payload.before.PRODUCT_ID.value,kafkavalues.payload.before.PRODUCT_ID.scale)
              prod_del_tuples.append((prod_id))

     elif topic_name == 'DESKTOP_9JC68P1.C__DBZUSER.ORDER_ITEMS' :

          load_type       = 'I' if  kafkavalues.payload.op in load_flg else kafkavalues.payload.op
          del_flg         = 'Y' if kafkavalues.payload.op in delete_flg else 'N'

          if load_type    == 'I':
              itm_id          = kafkavalues.payload.after.ITEM_ID
              ord_id          = kafkavalues.payload.after.ORDER_ID
              prod_id         = kafkavalues.payload.after.PRODUCT_ID
              quantity        = c_d.convbase64toint(kafkavalues.payload.after.QUANTITY,2)
              unit_price      = c_d.convbase64toint(kafkavalues.payload.after.UNIT_PRICE,2)
              # appending all columns in form of tuple to the list 
              ord_itm_insert_tuples.append((ord_id,itm_id,prod_id,quantity,unit_price))

          if del_flg == 'Y'  :
              itm_id          = kafkavalues.payload.before.ITEM_ID
              ord_id          = kafkavalues.payload.before.ORDER_ID
              ord_itm_del_tuples.append((itm_id,ord_id))
     elif topic_name == 'DESKTOP_9JC68P1.C__DBZUSER.ORDERS' :
    
          load_type       = 'I' if  kafkavalues.payload.op in load_flg else kafkavalues.payload.op
          del_flg         = 'Y' if kafkavalues.payload.op in delete_flg else 'N'

          if load_type    == 'I':
              ord_id          = c_d.convbase64toint(kafkavalues.payload.after.ORDER_ID.value,kafkavalues.payload.after.ORDER_ID.scale)
              cust_id         = kafkavalues.payload.after.CUSTOMER_ID
              status          = kafkavalues.payload.after.STATUS
              salesman_id     = kafkavalues.payload.after.SALESMAN_ID
              ord_dt          = c_d.convert_epochsec_to_dt(kafkavalues.payload.after.ORDER_DATE)
              # appending all columns in form of tuple to the list 
              ord_insert_tuples.append((ord_id,cust_id,status,salesman_id,ord_dt))

          if del_flg == 'Y'  :
               ord_id          = c_d.convbase64toint(kafkavalues.payload.before.ORDER_ID.value,kafkavalues.payload.before.ORDER_ID.scale)  
               ord_del_tuples.append((ord_id))
     elif topic_name =='DESKTOP_9JC68P1.C__DBZUSER.MILLION_RECORD_TEST' :
                
          load_type       = 'I' if  kafkavalues.payload.op in load_flg else kafkavalues.payload.op
          del_flg         = 'Y' if kafkavalues.payload.op in delete_flg else 'N'

          if load_type    == 'I':
              id_million  = c_d.convbase64toint(kafkavalues.payload.after.ID.value,kafkavalues.payload.after.ID.scale)
              type_text   = kafkavalues.payload.after.TYPE
              # appending all columns in form of tuple to the list 
              million_test_ins_tuples.append((id_million,type_text))

          if del_flg == 'Y'  :
              id_million  = c_d.convbase64toint(kafkavalues.payload.before.ID.value,kafkavalues.payload.before.ID.scale)
              million_test_del_tuples.append((id_million))    
     elif topic_name =='DESKTOP_9JC68P1.C__DBZUSER.DATA_TYPE_TEST' :
                
          load_type       = 'I' if  kafkavalues.payload.op in load_flg else kafkavalues.payload.op
          del_flg         = 'Y' if kafkavalues.payload.op in delete_flg else 'N'

          if load_type    == 'I':
                array_test              = kafkavalues.payload.after.ARRAY_TEST if kafkavalues.payload.after.ARRAY_TEST is not None else None
                nvarchar_test           = kafkavalues.payload.after.NVARCHAR_TEST if kafkavalues.payload.after.NVARCHAR_TEST is not None else None
                json_test               = (kafkavalues.payload.after.JSON_TEST).replace('\\n', '\n').replace('\\t', '\t') if kafkavalues.payload.after.JSON_TEST is not None else None
                char_test               = kafkavalues.payload.after.CHAR_TEST if kafkavalues.payload.after.CHAR_TEST is not None else None
                nchar_test              = kafkavalues.payload.after.NCHAR_TEST if kafkavalues.payload.after.NCHAR_TEST is not None else None
                num_pre_scale_test      = float(kafkavalues.payload.after.NUM_PRE_SCALE_TEST) if kafkavalues.payload.after.NUM_PRE_SCALE_TEST is not None else None
                binary_double_test      = kafkavalues.payload.after.BINARY_DOUBLE_TEST if kafkavalues.payload.after.BINARY_DOUBLE_TEST is not None else None
                binary_float_test       = kafkavalues.payload.after.BINARY_FLOAT_TEST if kafkavalues.payload.after.BINARY_FLOAT_TEST is not None else None
                real_test               = float(kafkavalues.payload.after.REAL_TEST) if kafkavalues.payload.after.REAL_TEST is not None else None
                smallint_test           = int(kafkavalues.payload.after.SMALLINT_TEST) if kafkavalues.payload.after.SMALLINT_TEST is not None else None
                float_test              = float(kafkavalues.payload.after.FLOAT_TEST)  if kafkavalues.payload.after.FLOAT_TEST is not None else None
                double_precision_test   = float(kafkavalues.payload.after.DOUBLE_PRECISION_TEST) if kafkavalues.payload.after.DOUBLE_PRECISION_TEST is not None else None
                decimal_test            = float(kafkavalues.payload.after.DECIMAL_TEST) if kafkavalues.payload.after.DECIMAL_TEST is not None else None
                date_test               = c_d.convert_epochsec_to_dt(kafkavalues.payload.after.DATE_TEST) if kafkavalues.payload.after.DATE_TEST is not None else None
                tst_frac_pre_test       = c_d.convert_epochsec_to_timestamp(kafkavalues.payload.after.TST_FRAC_PRE_TEST) if kafkavalues.payload.after.TST_FRAC_PRE_TEST is not None else None
                tst_tz_frac_pre_test    = c_d.convert_timestamp_tz_frm_str(kafkavalues.payload.after.TST_TZ_FRAC_PRE_TEST) if kafkavalues.payload.after.TST_TZ_FRAC_PRE_TEST is not None else None
                #int_yr_mon_test         = c_d.convert_epochsec_to_dt(kafkavalues.payload.after.INT_YR_MON_TEST) if kafkavalues.payload.after.INT_YR_MON_TEST is not None else None
                #int_day_mon_test        = c_d.convert_epochsec_to_dt(kafkavalues.payload.after.INT_DAY_MON_TEST) if kafkavalues.payload.after.INT_DAY_MON_TEST is not None else None
                int_day_mon_test        = None 
                int_yr_mon_test         = None 
                # appending all columns in form of tuple to the list 
                data_type_test_ins_tuples.append((array_test,nvarchar_test,json_test,char_test,nchar_test,num_pre_scale_test,binary_double_test,binary_float_test,real_test,smallint_test,float_test,double_precision_test,decimal_test,date_test,tst_frac_pre_test,tst_tz_frac_pre_test))

          if del_flg == 'Y'  :
              smallint_test           = c_d.convbase64toint(kafkavalues.payload.before.SMALLINT_TEST.value,0)
              data_type_test_del_tuples.append((smallint_test))      
     else :
      print("Code not writtern for Topic :",topic_name)