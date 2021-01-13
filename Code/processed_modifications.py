import csv
#country_codes_dict = {}

## Creating a dictionary with Country codes and names
with open('parsed_data.csv') as country_codes:
    country_code_data = csv.reader(country_codes,delimiter=',')

    '''
    for row in country_code_data:
        if row[1] in (' Islamic Republic of',' Republic of China',' United Republic of',' SAR China',' Republic of'):
            country_codes_dict[row[0]] = row[2]
        else:
            country_codes_dict[row[0]] = row[1]
    '''
    country_codes_dict = {row[0]:(row[2] if row[1] in (' Islamic Republic of',' Republic of China',' United Republic of',' SAR China',' Republic of') else row[1]) for row in country_code_data}

## Appending country codes to the processed data
new_data = []
with open('processed_data.csv') as processed_data:
    data = csv.reader(processed_data,delimiter=',')

    for row in data:
        if row[0] != '':
            row[-1:] = [country_codes_dict[row[0]]]
            new_data.append(row)

##Writing data to a csv file along with country codes
file = open('processed_data_final.csv','w+',newline = '')

with file:
    write = csv.writer(file)
    write.writerows(new_data)

#Copying data to hdfs
#hdfs dfs -copyFromLocal /home/ubuntu/processed_data_final.csv /dezyre/dezyre_out/
