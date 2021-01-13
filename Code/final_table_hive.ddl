CREATE EXTERNAL TABLE IF NOT EXISTS corona_final_table
(
Country_Name string,
Country_Total_Deaths integer,
Extracted_Time string,
Percent_Total_Deaths float,
Percent_Total_Recovered float)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
location '/dezyre/dezyre_out/final/';
