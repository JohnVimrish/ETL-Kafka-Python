# ETL-Kafka-Python

This code is focused on creating Extract, Transform, Load (ETL) pipelines using Python with Kafka and Postgres Database .This repository has codes of transporting and transforming messages collected by kafka to databases using python . The primary objective was to extract data from databases utilizing the KAFKA CDC (Change Data Capture) method. The POC specifically aimed to capture real-time data from an Oracle Database, facilitating instantaneous events and data transfer. In this process, we employed the Log-based Kafka CDC approach, utilizing Debezium. The collected data are  then transported using the Confluent Kafka library in Python and subsequently loaded into our Data Warehouse (DWH).

    Change Data Capture (CDC):
        Utilizing the Log-based CDC approach with Debezium for capturing real-time data changes in the Oracle Database.

    Kafka:
        Using Kafka as a distributed streaming platform for collecting and transporting data.
        Leveraging the Confluent Kafka library in Python to interact with Kafka.

    Python:
        Writing Python code for interacting with Kafka and implementing the ETL pipeline.

    Postgres Database:
        Loading the transformed data into a Postgres Database.

    Data Warehouse (DWH):
        Storing the collected and transformed data in a Data Warehouse for analytical purposes.

Key steps in the process might include:

    Configure Debezium for Oracle Database
         Follow the Debezium documentation to set up Debezium for Oracle: https://debezium.io/documentation/reference/2.4/connectors/oracle.html
    Debezium capturing changes in the Oracle Database and producing messages to Kafka topics.
    Python scripts consuming these Kafka messages, transforming them if necessary, and loading them into the Postgres Database.
    The Postgres Database serving as a staging area or a part of the Data Warehouse.

WorkFLow Diagram :

                                +---------------------+                  +------------------------+           
                                |                     |                  |                        |           
                                |    Kafka Server     |                  |   Debezium Connector   |          
                                |                     |                  |     with Oracle DB     |          
                                +---------------------+                  +------------------------+           
                                        |                                         |                                  
                                        |                                         |                                 
                                        |                                         |                                  
                                        v                                         v                                    
                                +---------------------+                    +------------------------+     
                                |                     | <----------------- |                        |     
                                | Kafka Topic (Oracle)|   Kafka Topic      |    Debezium Change     |     
                                |                     | -----------------> |    Data Capture (CDC)  |     
                                +---------------------+                    |                        |     
                                        |                                +------------------------+              
                                        |                                                                        
                                        |                                                                        
                                        v                                                                       
                                +-------------------------+              +--------------------------+            
                                |                         |              |                          |               
                                | Python ETL Script       |              |   PostgreSQL Database    |
                                | (Extract and Transform  | ------------>|                          |
                                |    Oracle Data )        |              |                          |
                                |                         |              +--------------------------+                       
                                +-------------------------+                                              


## Directory Structure

```
/
README.md
config/
    configuration.json
    connection_parameters.json
    kafka_configuration.json
    log_configuration.json
    ├── table_config/
        │  - employees.json
        │  - regions.json
kafka_instructions /
    codes_start_n_configure_kafkaanddebezium.txt
    connect-standalone.properties
    consumer.properties
    consumer.properties
    oracleconnector.properties
    producer.properties
    server.properties
    zookeeper.properties
log/
    ├── 202312202122/(sample log sturucture )
        │  - DESKTOP-TNM4E66ORA_CONNECTEMPLOYEES.log
        │  - DESKTOP-TNM4E66ORA_CONNECTREGIONS.log
        │  - ETLProgram.log
src/
    __init__.py
    ├── core/
        │  - kafkaconnect.py
        │  - kafkaetlprocess.py
        │  - sqlexecution.py
        │  - threadexecution.py
        │  - __init__.py
    ├── database/
        │  - dbinitialisers.py
        │  - postgresconnectionpool.py
        │  - postgresconnector.py
        │  - __init__.py
    ├── jsoncustom/
        │  - configparameters.py
        │  - jsontagvariables.py
        │  - jsonvalueextract.py
        │  - __init__.py
    ├── root/
        │  - commonvariables.py
        │  - main.py
        │  - __init__.py
    ├── testcases/
        │  - testfile.py
        │  - __init__.py
    ├── util/
        │  - emailsender.py
        │  - fileutility.py
        │  - loggingutility.py
        │  -  stringutil.py
        │  - __init__.py
```
