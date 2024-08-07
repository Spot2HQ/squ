CREATE DATABASE IF NOT EXISTS testdb;
USE testdb;

CREATE TABLE IF NOT EXISTS test_table (
    id INT AUTO_INCREMENT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    age INT
);

INSERT INTO test_table (first_name, last_name, age) VALUES
('John', 'Doe', 30),
('Jane', 'Doe', 25),
('Alice', 'Smith', 35),
('Bob', 'Johnson', 40),
('Charlie', 'Brown', 28),
('David', 'Wilson', 32),
('Eve', 'Davis', 45),
('Frank', 'Miller', 38),
('Grace', 'Clark', 29),
('Hannah', 'Martinez', 33);
