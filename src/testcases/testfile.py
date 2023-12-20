import datetime  
# import append_data_to_lists as app_d
# import ETL_to_PostgresDB as etl 
# import Connection_configuration as conn 
# import time 
import re 



# print('Start Time of Kafka Broker',datetime.datetime.now())
# try :
#     client = KafkaClient(hosts='localhost:9093')
#     topic_name = [str(topic_names.decode()) for (topic_names,y) in client.topics.items()]
# except NoBrokersAvailableError as Broker_error:
#     print('Error :',Broker_error)
# print('Time taken by  Kafka Broker',datetime.datetime.now())
# count_triggers = 0 
# while True :
#     dt = datetime.datetime.now()
#     #lenoftopic = int(len(topic_name))
#     lenoftopic = 1
#     load_type = 'FULL' if count_triggers < 1 else 'INCREMENTAL'
#     for position in range(lenoftopic) :
#         #ind_topic = topic_name[position] 
#         ind_topic  = 'DESKTOP_9JC68P1.C__DBZUSER.DATA_TYPE_TEST'
#         topic = client.topics [ind_topic]
#         consumer_group = ind_topic
#         consumer_id    = ind_topic +"_"+str(dt)
#         if  (ind_topic == 'kafkapoc_oracletopostgres' or ind_topic == 'DESKTOP_9JC68P1' ) :
#             continue 
#         else:  
#             print("Enabling Kafka Consumer ,",datetime.datetime.now())
#             print("Load Type :",load_type)
#             if load_type == 'FULL' :
#                consumer = topic.get_balanced_consumer(consumer_group=consumer_group, auto_offset_reset=OffsetType.EARLIEST,reset_offset_on_start=True,consumer_timeout_ms=5000)
#             if load_type == 'INCREMENTAL' :
#                consumer = topic.get_balanced_consumer(consumer_group=consumer_group, auto_offset_reset=OffsetType.LATEST,reset_offset_on_start=False,consumer_timeout_ms=5000)
#                consumer.commit_offsets()
#             print("Kafka Consumer initiated ,",datetime.datetime.now())
#             try :
#               print("printing topic :",ind_topic)
#               print ("Starting to append data to variables of topic-{}  : {} .".format(ind_topic,datetime.datetime.now()))
#               for msg in consumer :
#                  if  msg.value is not None :
#                      message = str(msg.value.decode())
#                      app_d.append_data(ind_topic,message)
#                      print(msg.offset)
#                  else :
#                      continue 
#               print ("Completed appending  data to variables,Starting with loading data into Database of topic-{}  : {} .".format(ind_topic,datetime.datetime.now()))
#               etl.load_data_to_tables (ind_topic)
#               consumer.stop()
#               print ("Completed  loading data into Database of topic-{}  : {} .".format(ind_topic,datetime.datetime.now()))
#             except (SocketDisconnectedError) as e:
#                print('Error Message :',e)
#                #use either the above method or the following:
#                consumer.stop()
#                consumer.start()
#     count_triggers +=1

#     target_connection = conn.DatabaseConnection()
#     target_connection.close()
    
#     time.sleep(1)

match = re.search(r'(?<=\.)(\w+)$','DESKTOP-TNM4E66.ORA_CONNECT.DEPARTMENTS')
print(match)
if match:
    table_name = match.group(1)
    print(table_name)


a= '24000.00'
print(type(a))
a= float(a)
print(type(a))