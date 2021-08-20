Big Data Pipeline for Covid 19 Analysis

Implementing an end-to-end data pipeline on AWS using real-time streaming, Pyspark and HDFS to analyse trends and impacts with COVID-19 dataset.  

Below are the steps in building this data pipeline.  
1. Extract streaming data from APIs at a certain interval of time using NiFi.  
2. Parse the data using Nifi which pushes the data into Kafka.  
3. Extract the data from Kafka into PySpark and process it.  
4. Write the streaming output data into another Kafka topic using NiFi.Put the processed streaming data into HDFS.  
5. Create external table in Hive.  
6. Query the data from Hive.  
7. Orchestrate the process.  

Overview of Data Architecture used:  
![Snapshot of Architecture](https://github.com/gmadhu89/covid19-analysis-pyspark/blob/main/architecture.jpg?raw=true "Snapshot of pipeline architecture")
