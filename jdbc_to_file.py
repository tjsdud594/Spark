from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

mysql_ip = "ryu-mysql.c9iezgjnbwtd.ap-northeast-2.rds.amazonaws.com"
mysql_port = "3306"
mysql_db = "employees"
mysql_driver = "com.mysql.cj.jdbc.Driver"
user = "spark"
password = "spark"

mysql_table_employee = "employees"
mysql_table_emp_dept = "dept_emp"
mysql_table_dept = "departments"

write_db = "examples"
write_table = "emp_work"

spark = SparkSession.builder\
        .appName("mysql_make_datamart")\
        .config("spark.driver.extraClassPath", "C:/repository/Spark/files/mysql-connector-j-8.0.33.jar")\
        .getOrCreate()


employee_df = spark.read.format("jdbc")\
                    .option("url","jdbc:mysql://{0}:{1}/{2}".format( mysql_ip, mysql_port, mysql_db ) )\
                    .option("driver", mysql_driver)\
                    .option("dbtable", mysql_table_employee)\
                    .option("user", user)\
                    .option("password", password)\
                    .load()

emp_dept_df = spark.read.format("jdbc")\
                    .option("url","jdbc:mysql://{0}:{1}/{2}".format( mysql_ip, mysql_port, mysql_db ) )\
                    .option("driver", mysql_driver)\
                    .option("dbtable", mysql_table_emp_dept)\
                    .option("user", user)\
                    .option("password", password)\
                    .load()

dept_df = spark.read.format("jdbc")\
                    .option("url","jdbc:mysql://{0}:{1}/{2}".format( mysql_ip, mysql_port, mysql_db ) )\
                    .option("driver", mysql_driver)\
                    .option("dbtable", mysql_table_dept)\
                    .option("user", user)\
                    .option("password", password)\
                    .load()


employee_df.createOrReplaceTempView("employees")
emp_dept_df.createOrReplaceTempView("dept_emp")
dept_df.createOrReplaceTempView("departments")


new_df = spark.sql("""
select e.emp_no, e.first_name, e.last_name, e.gender, de.dept_no, de.dept_name, de.dept_work_years, de.work_years
from employees e 
left join (
	select * 
        , round(DATEDIFF(case when to_date='9999-01-01' 
                        then current_timestamp() 
                        else to_date
                        end,
                        from_date
                        ) / 365.0, 2) as work_years
    from (
        select deyear.emp_no 
            , array_join(collect_list(deyear.dept_no), ', ') as dept_no
            , array_join(collect_list(deyear.dept_name), ', ') as dept_name
            , array_join(collect_list(deyear.dept_years), ', ') as dept_work_years
            , max(deyear.to_date) as to_date
            , min(deyear.from_date) as from_date
        from (select de.emp_no as emp_no
                    , de.dept_no as dept_no
                    , d.dept_name as dept_name
                    , de.to_date as to_date
                    , de.from_date as from_date
                    , round(DATEDIFF(case when max(de.to_date)='9999-01-01' 
                        then current_timestamp() 
                        else de.to_date
                        end, min(de.from_date))/365.0, 2) as dept_years 
                from dept_emp de
                left join departments d on de.dept_no = d.dept_no 
                group by de.emp_no, d.dept_name, de.dept_no, de.to_date, de.from_date
            ) deyear
        group by deyear.emp_no
    )
) de on e.emp_no = de.emp_no
""")


new_df = new_df.withColumn("create_time", current_timestamp().cast("timestamp"))

new_df.printSchema()
new_df.show()

new_df.write \
    .mode("overwrite")\
    .format("jdbc")\
    .option("url","jdbc:mysql://{0}:{1}/{2}".format( mysql_ip, mysql_port, write_db ) )\
    .option("driver", mysql_driver)\
    .option("dbtable", write_table)\
    .option("user", user)\
    .option("password", password)\
    .save()
