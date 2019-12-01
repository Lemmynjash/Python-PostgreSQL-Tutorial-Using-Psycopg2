#library for calling GET Requests
import urllib.request
#library for mapping JSON Requests
import json
#library for hadling postgreSQl
import psycopg2


#this one calls the main function
if __name__ == '__main__':
    #this is the main function
    main()
 
#this is the main function    
def main():
    #define the Url here in your case utakuwa na three urls
    #run this url through Postman like how i taught you and see the JSON view for you to understand
    urlData = "http://vocab.nic.in/rest.php/states/json"
    #call the getResponse function passing urlData parameter the function iko chini ya this function
    jsonData = getResponse(urlData)
    
    #print the JSON response
    print(jsonData['data']['type'])
    
    # this is where i was telling you about looping through the JSON records
    # i have decided to use for loop instead while loop instead
    # for a successfull loop it calls the function conectToPostGres passing some parameters
    # such as householdNumber,safeWater,treatedWater
    for i in jsonData['data']['attributes']:
        conectToPostGres(i["householdNumber"],i["safeWater"],i["treatedWater"])

#this function handles a successful connection of the url 
def getResponse(url):
    #opens the url
    operUrl = urllib.request.urlopen(url)
    #cheks if the GET response is successfull
    if(operUrl.getcode()==200):
        #opens the url and reads the content
        data = operUrl.read()
        #decods the JSON content
        jsonData = json.loads(data)
    else:
        print("Error receiving data", operUrl.getcode())
    return jsonData

#this function handles successfull connection and storing of data to postgreSQL 
def conectToPostGres(householdNumber,safeWater,treatedWater):
   try:
        #handles connection
        #in your case you will change the password and database name umeshika!           
        connection = psycopg2.connect(user="postgres",
                                        password="123",
                                        host="127.0.0.1",
                                        port="5432",
                                        database="members")
        #this calls the cursor
        cursor = connection.cursor()

        #this query helps in storying values to the table
        #in this case you will just change the household name to your table name and its column names
        postgres_insert_query = """ INSERT INTO public.household("householdNumber", "safeWater", "treatedWater") VALUES (%s,%s,%s)"""
        #define here the json objects and store them
        record_to_insert = (int(householdNumber), safeWater, treatedWater)
        #execute the cursor
        cursor.execute(postgres_insert_query, record_to_insert)
        #commit the connection
        connection.commit()
        #this counts the rows
        count = cursor.rowcount
        #this prints them if its successfull
        print (count, "Record inserted successfully into member table")
   
   #this handles errors if it occurs     
   except(Exception, psycopg2.Error) as error:
       if(connection):
           print("Failed to insert record into the table",error)
           
   finally:
       #closing database connection
       if(connection):
           cursor.close()
           connection.close()
           print("PostgresQl connection is closed")
 
 


