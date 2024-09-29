import mysql.connector
db = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="Password",
    database="grocerydatabase"
    )
mycursor = db.cursor()
#mycursor.execute("CREATE TABLE bill (serialNum INT, item VARCHAR(50), quantity INT, toalCost DOUBLE)")
#mycursor.execute("INSERT INTO grocery (item, quantity, cost) VALUES ('Chicken', 20, 100)")
#db.commit()
mycursor.execute("SELECT * FROM grocery")

print(f'{"Sr.no":<6} {"Item":<7}  {"Quantity":<10} {"Cost/Item":<6}')
print(f'{"------":<6} {"------":<7}  {"--------":<10} {"------":<6}')

results = mycursor.fetchall()

for row in results:
    print(f'{row[0]:<6} {row[1]:<6}  {row[2]:<10} {row[3]:<6}')

flag = 'y'
while flag == 'y':
    choice = int(input("What do you want to purchase?"))
    quantity = int(input(f"How many {results[choice-1][1]} packets you want to purchase: "))
    if results[choice-1][2] < quantity:
        print(f'Available quantity of {results[choice-1][1]} is {results[choice-1][2]}:')
    else:
        currentVal = results[choice-1][2] - quantity
        mycursor.execute("UPDATE grocery SET quantity = %s WHERE serialNum = %s", (currentVal, choice))
        mycursor.execute("INSERT INTO bill (serialNum, item, quantity, toalCost) VALUES (%s, %s, %s, %s)", (results[choice-1][0],results[choice-1][1], quantity,results[choice-1][3] * quantity))
        db.commit()
    flag = input("Do you want to coninue shopping? Y/N ")
name = input("Enter your name : ")
address =input("Enter you address: ")
distance = int(input("Enter distance from store 5/10/15/30: "))

distanceCost = 0
if(distance <= 15):
    distanceCost = 50
elif(15 < distance < 30):
    distanceCost = 100
else:
    distanceCost = -1

print("Delivery charge: 100 Rs will be levied for distance between 15 and 30 km\n")
mycursor.execute("SELECT * FROM bill")
resultsBill = mycursor.fetchall()
print("----------------Bill--------------------")
grandTotalCost = 0
print(f'{"Sr.no":<6} {"Item":<7}  {"Quantity":<10} {"Cost/Item":<6}')
for row in resultsBill:
    print(f'{row[0]:<6} {row[1]:<6}  {row[2]:<10} {row[3]:<6}')
for row in resultsBill:
    grandTotalCost = grandTotalCost + row[3]
    
print(f"Total items cost: {grandTotalCost}\n")
print(f"Total Bill Amount: Total items cost + Delivery Charge is: {grandTotalCost+distanceCost}", )
print(f"{name}: \n{address}: \nHave a nice day!!")

mycursor.execute("DELETE FROM bill")
db.commit()
mycursor.execute("SELECT * FROM grocery")
results = mycursor.fetchall()
print("----------------Remaining Quantity In Store--------------------")
print(f'{"Sr.no":<6} {"Item":<7}  {"Quantity":<10} {"Cost/Item":<6}')
for row in results:
    print(f'{row[0]:<6} {row[1]:<6}  {row[2]:<10} {row[3]:<6}')
