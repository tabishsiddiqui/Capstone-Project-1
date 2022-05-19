drop database if exists tabish_project;
create database tabish_project;
use tabish_project;

drop table if exists employees;
CREATE EXTERNAL TABLE employees STORED AS AVRO LOCATION '/user/anabig11429/capstone/employees' TBLPROPERTIES ('avro.schema.url'='/user/anabig11429/schema/employees.avsc');

drop table if exists titles;
CREATE EXTERNAL TABLE titles STORED AS AVRO LOCATION '/user/anabig11429/capstone/titles' TBLPROPERTIES ('avro.schema.url'='/user/anabig11429/schema/titles.avsc');

drop table if exists departments;
CREATE EXTERNAL TABLE departments STORED AS AVRO LOCATION '/user/anabig11429/capstone/departments' TBLPROPERTIES ('avro.schema.url'='/user/anabig11429/schema/departments.avsc');

drop table if exists salaries;
CREATE EXTERNAL TABLE salaries STORED AS AVRO LOCATION '/user/anabig11429/capstone/salaries' TBLPROPERTIES ('avro.schema.url'='/user/anabig11429/schema/salaries.avsc');

drop table if exists dept_manager;
CREATE EXTERNAL TABLE dept_manager STORED AS AVRO LOCATION '/user/anabig11429/capstone/dept_manager' TBLPROPERTIES ('avro.schema.url'='/user/anabig11429/schema/dept_manager.avsc');

drop table if exists dept_emp;
CREATE EXTERNAL TABLE dept_emp STORED AS AVRO LOCATION '/user/anabig11429/capstone/dept_emp' TBLPROPERTIES ('avro.schema.url'='/user/anabig11429/schema/dept_emp.avsc');