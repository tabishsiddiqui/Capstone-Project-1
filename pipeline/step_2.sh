hdfs dfs -rm -r /user/anabig11429/capstone/employees/*;
hdfs dfs -rm -r /user/anabig11429/capstone/titles/*;
hdfs dfs -rm -r /user/anabig11429/capstone/departments/*;
hdfs dfs -rm -r /user/anabig11429/capstone/salaries/*;
hdfs dfs -rm -r /user/anabig11429/capstone/dept_manager/*;
hdfs dfs -rm -r /user/anabig11429/capstone/dept_emp/*;

hdfs dfs -rm -r /user/anabig11429/capstone/employees/;
hdfs dfs -rm -r /user/anabig11429/capstone/titles/;
hdfs dfs -rm -r /user/anabig11429/capstone/departments/;
hdfs dfs -rm -r /user/anabig11429/capstone/salaries/;
hdfs dfs -rm -r /user/anabig11429/capstone/dept_manager/;
hdfs dfs -rm -r /user/anabig11429/capstone/dept_emp/;

hdfs dfs -rm -r /home/anabig11429/employees.avsc;
hdfs dfs -rm -r /home/anabig11429/titles.avsc;
hdfs dfs -rm -r /home/anabig11429/departments.avsc;
hdfs dfs -rm -r /home/anabig11429/salaries.avsc;
hdfs dfs -rm -r /home/anabig11429/dept_manager.avsc;
hdfs dfs -rm -r /home/anabig11429/dept_emp.avsc;

hdfs dfs -rm -r /user/anabig11429/schema/*;
hdfs dfs -rm -r /user/anabig11429/schema;

sqoop import --connect jdbc:mysql://ip-10-1-1-204.ap-south-1.compute.internal:3306/anabig11429 --username anabig11429 --password Bigdata123 --table employees --compression-codec=snappy --as-avrodatafile --target-dir /user/anabig11429/capstone/employees --m 1 --driver com.mysql.jdbc.Driver;
sqoop import --connect jdbc:mysql://ip-10-1-1-204.ap-south-1.compute.internal:3306/anabig11429 --username anabig11429 --password Bigdata123 --table titles --compression-codec=snappy --as-avrodatafile --target-dir /user/anabig11429/capstone/titles --m 1 --driver com.mysql.jdbc.Driver;
sqoop import --connect jdbc:mysql://ip-10-1-1-204.ap-south-1.compute.internal:3306/anabig11429 --username anabig11429 --password Bigdata123 --table departments --compression-codec=snappy --as-avrodatafile --target-dir /user/anabig11429/capstone/departments --m 1 --driver com.mysql.jdbc.Driver;
sqoop import --connect jdbc:mysql://ip-10-1-1-204.ap-south-1.compute.internal:3306/anabig11429 --username anabig11429 --password Bigdata123 --table salaries --compression-codec=snappy --as-avrodatafile --target-dir /user/anabig11429/capstone/salaries --m 1 --driver com.mysql.jdbc.Driver;
sqoop import --connect jdbc:mysql://ip-10-1-1-204.ap-south-1.compute.internal:3306/anabig11429 --username anabig11429 --password Bigdata123 --table dept_manager --compression-codec=snappy --as-avrodatafile --target-dir /user/anabig11429/capstone/dept_manager --m 1 --driver com.mysql.jdbc.Driver;
sqoop import --connect jdbc:mysql://ip-10-1-1-204.ap-south-1.compute.internal:3306/anabig11429 --username anabig11429 --password Bigdata123 --table dept_emp --compression-codec=snappy --as-avrodatafile --target-dir /user/anabig11429/capstone/dept_emp --m 1 --driver com.mysql.jdbc.Driver;

hadoop fs -mkdir /user/anabig11429/schema;

hadoop fs -put /home/anabig11429/employees.avsc /user/anabig11429/schema/employees.avsc;
hadoop fs -put /home/anabig11429/titles.avsc /user/anabig11429/schema/titles.avsc;
hadoop fs -put /home/anabig11429/departments.avsc /user/anabig11429/schema/departments.avsc;
hadoop fs -put /home/anabig11429/salaries.avsc /user/anabig11429/schema/salaries.avsc;
hadoop fs -put /home/anabig11429/dept_manager.avsc /user/anabig11429/schema/dept_manager.avsc;
hadoop fs -put /home/anabig11429/dept_emp.avsc /user/anabig11429/schema/dept_emp.avsc;

hadoop fs -chmod +rw /user/anabig11429/schema