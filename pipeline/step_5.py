from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("capstone").config('hive.metastore.uris','thrift://ip-10-1-2-24.ap-south-1.compute.internal:9083/anabig11429').enableHiveSupport().getOrCreate()

emp = spark.sql("select * from tabish_project.employees")

emp1 = emp.withColumn('birth_date', regexp_replace('birth_date', '-', '/')).withColumn("birth_date", to_date("birth_date", "M/d/yyyy"))
emp1 = emp1.withColumn('hire_date', regexp_replace('hire_date', '-', '/')).withColumn("hire_date", to_date("hire_date", "M/d/yyyy"))
emp1 = emp1.withColumn('last_date', regexp_replace('last_date', '-', '/')).withColumn("last_date", to_date("last_date", "M/d/yyyy"))

emp1.createOrReplaceTempView('emp1')

titles = spark.sql("select * from tabish_project.titles")
titles.createOrReplaceTempView('titles')

departments = spark.sql("select * from tabish_project.departments")
departments.createOrReplaceTempView('departments')

salaries = spark.sql("select * from tabish_project.salaries")
salaries.createOrReplaceTempView('salaries')

dept_manager = spark.sql("select * from tabish_project.dept_manager")
dept_manager.createOrReplaceTempView('dept_manager')

dept_emp = spark.sql("select * from tabish_project.dept_emp")
dept_emp.createOrReplaceTempView('dept_emp')

spark.sql("select t1.emp_no, t1.first_name, t1.last_name, t1.sex, t2.salary from emp1 t1 join salaries t2 on t1.emp_no=t2.emp_no").show(5)

spark.sql("select first_name, last_name, hire_date from emp1 where year(hire_date) = '1986' limit 5").show()

spark.sql("select t1.dept_no, t1.dept_name, t3.emp_no, t3.first_name, t3.last_name from departments t1 left join dept_manager t2 on t1.dept_no=t2.dept_no left join employees t3 on t2.emp_no=t3.emp_no").show(24)

spark.sql("select t1.emp_no, t1.first_name, t1.last_name, t3.dept_name from emp1 t1 join dept_emp t2 on t1.emp_no=t2.emp_no join departments t3 on t2.dept_no=t3.dept_no").show(5)

spark.sql("select first_name, last_name, sex from emp1 where first_name = 'Hercules' and last_name like 'B%'").show(5)

spark.sql("""select t1.emp_no, t1.first_name, t1.last_name, t3.dept_name from emp1 t1 join dept_emp t2 on t1.emp_no=t2.emp_no join departments t3 on t2.dept_no=t3.dept_no where t3.dept_name='"Sales"'""").show(5)

spark.sql("""select t1.emp_no, t1.first_name, t1.last_name, t3.dept_name from emp1 t1 join dept_emp t2 on t1.emp_no=t2.emp_no join departments t3 on t2.dept_no=t3.dept_no where t3.dept_name = '"Sales"' or t3.dept_name = '"development"'""").show(5)

spark.sql("select last_name, count(last_name) as freq from emp1 group by last_name order by freq desc").show(5)

spark.sql("select emp_no, cast((datediff(lastdate, hire_date))/365.4 as int) as tenure from (select *, if(last_date is null, to_date('2000-01-28'), last_date) as lastdate from emp1)").show(5)

spark.sql("select no_of_projects, count(emp_no) from emp1 group by no_of_projects order by count(emp_no) desc").show()

spark.sql("select last_performance_rating, count(emp_no) from emp1 group by last_performance_rating order by count(emp_no) desc").show()

spark.sql("select t1.dept_name, count(t2.emp_no) from departments t1 join dept_emp t2 on t1.dept_no=t2.dept_no group by t1.dept_name").show()

spark.sql("select t1.dept_name, sum(t3.salary) from departments t1 join dept_emp t2 on t1.dept_no=t2.dept_no join salaries t3 on t2.emp_no=t3.emp_no group by t1.dept_name order by sum(t3.salary) desc").show()

spark.sql("select emp_no, cast(datediff(to_date('2000-12-31'), birth_date)/365.4 as int) as age from emp1").show(5)
