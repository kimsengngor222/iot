import paho.mqtt.client as mqtt
import mysql.connector
from time import gmtime, strftime
import time

mydb = mysql.connector.connect(
  host="hsm.vkirirom.com",
  user="root",
  passwd="root12345",
  database="spring_hsm"
)
mycursor = mydb.cursor()

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("KITEscott")

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    message = str(msg.payload)
    data = message.split()
    sql = "FROM Daily_Usage where room_ID = %s AND DATE dmy = %s"
    adr = (data[0],data[3])
    try:
        sql_room_id = "SELECT * FROM board where MAC= %s"
        mac = (data[0],)
        mycursor.execute(sql_room_id , mac)
        room_field = mycursor.fetchall()
        sql = "SELECT * FROM daily_usage where room_ID = %s AND DATE (dmy) = %s"
        adr_room = (room_field[0][5],strftime("%Y-%m-%d", gmtime()))
        mycursor.execute(sql, adr_room)
        myresult = mycursor.fetchall()

        if myresult != []:
            print("update")
            print(myresult[0][0])
            power = myresult[0][3] + float(data[1])
            water = myresult[0][4] + float(data[2])
            sql_power = "UPDATE daily_usage SET power = %s WHERE ID = %s"
            value_power = (power,myresult[0][0])
            sql_water = "UPDATE daily_usage SET water = %s WHERE ID = %s"
            value_water = (water,myresult[0][0])
            sql_update_time = "UPDATE daily_usage SET updated_at = %s WHERE ID = %s"
            value_update_time = (strftime("%Y-%m-%d %H:%M:%S", gmtime()),myresult[0][0])
            mycursor.execute(sql_power,value_power)
            mycursor.execute(sql_water,value_water)
            mycursor.execute(sql_update_time,value_update_time)
            mydb.commit()
            print("done")
        else:
            print("insert")
            sql = "INSERT INTO daily_usage (room_ID,dmy,power,water,created_at) VALUES (%s,%s,%s,%s,%s)"
            val = (room_field[0][5],strftime("%Y-%m-%d", gmtime()),data[1],data[2],strftime("%Y-%m-%d %H:%M:%S", gmtime()))
            mycursor.execute(sql,val)
            mydb.commit()
        
    except e:
        print("catch")
        print (e)
    
    
    
    


client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message


client.connect("hsm.vkirirom.com", 1883, 60)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
client.loop_forever()

