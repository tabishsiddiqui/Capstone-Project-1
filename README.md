# Capstone-Project-1

How to use the files to run the pipeline:

•	Log in to cloudera

•	Open web shell

•	Run the commands in sequence:
  1.	mysql -u anabig11429  -pBigdata123 "anabig11429" < “step_1.sql”;
  2.	sh step_2.sh;
  3.	hive -f step_3.sql;
  4.	impala-shell -f step_4.sql;
  5.	spark-submit step_5.py;
  6.	spark-submit step_6.py;

The step files are present in pipeline folder.
First 3 steps are mandatory in order to migrate the data from rdbms to hive.
