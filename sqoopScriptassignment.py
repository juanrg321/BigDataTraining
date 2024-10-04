import psycopg2
import subprocess
import os
# Establish the database connection
db = psycopg2.connect(
    host="ec2-18-132-73-146.eu-west-2.compute.amazonaws.com",
    port="5432",
    user="consultants",
    password="WelcomeItc@2022",
    database="testdb"  
)


# Create a cursor object
mycursor = db.cursor()
mycursor.execute("SELECT * FROM juanemp1;")

# Verify the current working directory

results = results = mycursor.fetchall()

print("Current Working Directory:", os.getcwd())

for row in results:
    print(f'{row[0]:<6} {row[1]:<6}  {row[2]:<10} {row[3]:<6}')

with open('myfile3.txt', 'w') as file:
    for row in results:
        file.write(f'{row[0]:<6} {row[1]:<6}  {row[2]:<10} {row[3]:<6}\n')

copy_command = ["hdfs", "dfs", "-put","-f", "myfile3.txt", "ukussept/juan/emp1/"]
cat_command = ["hadoop", "fs", "-cat", "ukussept/juan/emp1/myfile3.txt"]

#Execute the copy command
copy_result = subprocess.run(copy_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)


if copy_result.returncode == 0:
    #If the copy was successful, run the cat command
    cat_result = subprocess.run(cat_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    print("Output:", cat_result.stdout)
    print("Error:", cat_result.stderr)
else:
    print("Error during copy:", copy_result.stderr)

