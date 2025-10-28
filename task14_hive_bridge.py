from pyspark import SparkContext
from pyspark.sql import HiveContext
sc = SparkContext(appName="Task14_Hive_Bridge")
hive = HiveContext(sc)
hive.sql("USE default")
print("=== Data from HBase through Hive ===")
df = hive.sql("SELECT * FROM employees")
df.show()
print("=== Employees per Department ===")
hive.sql("""
  SELECT dept, COUNT(*) AS cnt
  FROM employees
  GROUP BY dept
  ORDER BY cnt DESC
""").show()
sc.stop()