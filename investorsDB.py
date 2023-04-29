import mysql.connector

db = mysql.connector.connect(user='itc6107', password='itc6107', host='127.0.0.1');

cursor = db.cursor()

cursor.execute(
    """
    DROP DATABASE IF EXISTS InvestorsDB;
    CREATE DATABASE InvestorsDB;
    USE InvestorsDB;
    
    CREATE TABLE Investors (
        id CHAR(20) NOT NULL primary key,
        Name CHAR(20),
        City CHAR(20)
    );
    
    CREATE TABLE Portfolios (
        id CHAR(20) NOT NULL primary key,
        Name CHAR(20),
        Cumulative TINYINT(1) NOT NULL
    );
    
    CREATE TABLE Investors_Portfolios (
        iid CHAR(20),
        FOREIGN KEY (iid) REFERENCES Investors(id),
        pid CHAR(20),
        FOREIGN KEY (pid) REFERENCES Portfolios(id),
        PRIMARY KEY(iid,pid)
    );
    
    CREATE TABLE Inv1_P11 (
        id INT AUTO_INCREMENT PRIMARY KEY,
        Evaluation DOUBLE,
        Difference DOUBLE,
        Percentage DOUBLE,
        Timestamp TIMESTAMP
    );
    
    CREATE TABLE Inv1_P12 (
        id INT AUTO_INCREMENT PRIMARY KEY,
        Evaluation DOUBLE,
        Difference DOUBLE,
        Percentage DOUBLE,
        Timestamp TIMESTAMP
    );
    
    CREATE TABLE Inv2_P21 (
        id INT AUTO_INCREMENT PRIMARY KEY,
        Evaluation DOUBLE,
        Difference DOUBLE,
        Percentage DOUBLE,
        Timestamp TIMESTAMP
    );
    
    CREATE TABLE Inv2_P22 (
        id INT AUTO_INCREMENT PRIMARY KEY,
        Evaluation DOUBLE,
        Difference DOUBLE,
        Percentage DOUBLE,
        Timestamp TIMESTAMP
    ); 
    
    CREATE TABLE Inv3_P31 (
        id INT AUTO_INCREMENT PRIMARY KEY,
        Evaluation DOUBLE,
        Difference DOUBLE,
        Percentage DOUBLE,
        Timestamp TIMESTAMP
    ); 
    
    CREATE TABLE Inv3_P32 (
        id INT AUTO_INCREMENT PRIMARY KEY,
        Evaluation DOUBLE,
        Difference DOUBLE,
        Percentage DOUBLE,
        Timestamp TIMESTAMP
    );      
    
    """
)

cursor.close()
db.close()
