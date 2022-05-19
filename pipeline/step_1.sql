use anabig11429;

drop table if exists titles;
CREATE TABLE titles(title_id VARCHAR(20) NOT NULL, title VARCHAR(50), PRIMARY KEY(title_id));

load data local infile '/home/anabig11429/titles.csv'
into table titles
fields terminated by ','
ignore 1 rows;

drop table if exists dept_emp;
CREATE TABLE dept_emp(emp_no int NOT NULL, dept_no VARCHAR(50));

load data local infile '/home/anabig11429/dept_emp.csv'
into table dept_emp
fields terminated by ','
ignore 1 rows;

drop table if exists dept_manager;
CREATE TABLE dept_manager(dept_no VARCHAR(50) NOT NULL, emp_no int);

load data local infile '/home/anabig11429/dept_manager.csv'
into table dept_manager
fields terminated by ','
ignore 1 rows;

drop table if exists departments;
CREATE TABLE departments(dept_no VARCHAR(50) NOT NULL, dept_name varchar(50), Primary Key(dept_no));

load data local infile '/home/anabig11429/departments.csv'
into table departments
fields terminated by ','
ignore 1 rows;

drop table if exists salaries;
CREATE TABLE salaries(emp_no int NOT NULL, salary int, Primary Key(emp_no));

load data local infile '/home/anabig11429/salaries.csv'
into table salaries
fields terminated by ','
ignore 1 rows;

drop table if exists employees;
CREATE TABLE employees(emp_no int NOT NULL,emp_title_id VARCHAR(10),birth_date VARCHAR(12),first_name VARCHAR(15),last_name VARCHAR(15),sex CHAR(2),hire_date VARCHAR(12),no_of_projects int,Last_performance_rating VARCHAR(5),left_ VARCHAR(10),last_date VARCHAR(12),PRIMARY KEY(emp_no));

load data local infile '/home/anabig11429/employees.csv'
into table employees
fields terminated by ','
ignore 1 rows;
