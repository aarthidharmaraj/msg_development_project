"""This module creates the database, table and insert employee details in the database"""
import mysql.connector

class AddEmployeeDetails:
    def __init__(self,my_cursor):
        """This is the init method of the class"""
        self.cursor=my_cursor
    def Add_Employ(self):
        """This method is to add the details of employees"""
        Id = input("Enter Employ Id : ")
        Name = input("Enter Employ Name : ")
        Post = input("Enter Employ Post : ")
        Salary = input("Enter Employ Salary : ")
        Contact=input("Enter the contact number:")
        Date_of_joining=input("Enter the date of joining of employee:")
        data = (Id, Name, Post, Salary,Contact,Date_of_joining)

        sql = 'insert into employee values(%s,%s,%s,%s,%s,%s)'
        self.cursor.execute(sql, data)
        print("Employ Added Successfully")
        
def main():
    """This is the main method for the class"""
    mydb=mysql.connector.connect(host="localhost",user="***",passwd=" ***",database='Employee_payroll')

    my_cursor=mydb.cursor()
    # my_cursor.execute("CREATE DATABASE Employee_Payroll")
    # my_cursor.execute("DROP TABLE employee")
    # my_cursor.execute("CREATE TABLE employee(emp_id integer PRIMARY KEY NOT NULL,emp_Name VARCHAR(100),Post VARCHAR(100),Salary integer,Contact VARCHAR(20),Date_of_Joining DATE)")
    add_employee=AddEmployeeDetails(my_cursor)
    add_employee.Add_Employ()
    mydb.commit()

if __name__ == "__main__":
    main()