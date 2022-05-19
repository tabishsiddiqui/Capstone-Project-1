from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import RegressionEvaluator

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

data = spark.sql("select t2.*, t1.title, t3.salary, t5.dept_name, cast(datediff(to_date('2000-12-31'), t2.birth_date)/365.4 as int) as age from titles t1 join emp1 t2 on t1.title_id=t2.emp_title_id join salaries t3 on t2.emp_no=t3.emp_no join dept_emp t4 on t3.emp_no=t4.emp_no join departments t5 on t4.dept_no=t5.dept_no")

data = data.withColumn('label', data.left_.cast('int'))

stage_1 = StringIndexer(inputCol='sex', outputCol='sex_idx')
stage_2 = StringIndexer(inputCol='last_performance_rating', outputCol='last_performance_rating_idx')
stage_3 = StringIndexer(inputCol='title', outputCol='title_idx')
stage_4 = StringIndexer(inputCol='dept_name', outputCol='dept_name_idx')


stage_5 = OneHotEncoder(inputCol='sex_idx', outputCol='sex_ohe')
stage_6 = OneHotEncoder(inputCol='last_performance_rating_idx', outputCol='last_performance_rating_ohe')
stage_7 = OneHotEncoder(inputCol='title_idx', outputCol='title_ohe')
stage_8 = OneHotEncoder(inputCol='dept_name_idx', outputCol='dept_name_ohe')

stage_9 = VectorAssembler(inputCols=['sex_ohe','last_performance_rating_ohe','title_ohe','dept_name_ohe'] + continuous_features, outputCol="features")
                          
stage_10 = LogisticRegression(featuresCol='features',labelCol='label')

pipeline = Pipeline( stages= [stage_1, stage_2, stage_3, stage_4, stage_5, stage_6, stage_7, stage_8, stage_9, stage_10])

sample_data_train = pipeline.fit(data).transform(data)

sample_data_train.select('features', 'label', 'rawPrediction', 'probability', 'prediction').show(5)