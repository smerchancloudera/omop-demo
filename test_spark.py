from pyspark.sql import SparkSession

# Configurar la autenticación (Kerberos o usuario/contraseña)
print ("* Creando la sesion Spark ...")
spark = (SparkSession\
    .builder.appName("Spark-SQL-Test")\
    .enableHiveSupport()
    .getOrCreate())


print ("* Sesion creada.")

print ("* Lanzando consulta sparkSQL ...")

#SQL_sentence="select * from death"
SQL_sentence="SHOW DATABASES"
resultados = spark.sql(SQL_sentence)

print ("* Mostrando resultados ...")

resultados.show (5)
print ("* Cerrando sesion spark...")

spark.stop
