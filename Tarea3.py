from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# Inicialización de la sesión de Apache Spark
spark = SparkSession.builder.appName('Analisis_Saber11_Batch').getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Carga del conjunto de datos desde HDFS
file_path = 'hdfs://localhost:9000/Tarea3/Saber11/saber11.csv'

print("\n=======================================================")
print(">>> FASE 1: INGESTA Y ESQUEMA ESTRUCTURAL DESDE HDFS <<<")
print("=======================================================")

df = spark.read.format('csv') \
    .option('header', 'true') \
    .option('inferSchema', 'true') \
    .load(file_path)

# Total de registros cargados
total_registros = df.count()
print(f"\n[OK] Total de registros ingestados en el cluster: {total_registros:,} filas")

# Limpieza y tipado de variables analíticas
# Se limpian valores nulos en los puntajes y se convierten a enteros
df_clean = df.filter(F.col("PUNT_MATEMATICAS").isNotNull()) \
    .withColumn("PUNT_MATEMATICAS", F.col("PUNT_MATEMATICAS").cast(IntegerType())) \
    .withColumn("PUNT_INGLES", F.col("PUNT_INGLES").cast(IntegerType()))

# RESPUESTA AL INTERROGANTE: DIAGNÓSTICO ESTADÍSTICO GENERAL
print("\n=======================================================")
print(">>> DIAGNÓSTICO ESTADÍSTICO DE PUNTAJES <<<")
print("=======================================================")

# Medidas de tendencia central y dispersión mediante .summary()
df_clean.select('PUNT_MATEMATICAS', 'PUNT_INGLES').summary().show()

# RESPUESTA AL INTERROGANTE: AISLAMIENTO DE LA EXCELENCIA ACADÉMICA
print("\n=======================================================")
print(">>> EXCELENCIA ACADÉMICA (PUNTAJE >= 70) <<<")
print("=======================================================")

umbral_excelencia = 70
destacados = df_clean.filter(F.col('PUNT_MATEMATICAS') >= umbral_excelencia)

total_destacados = destacados.count()
porcentaje_excelencia = (total_destacados / total_registros) * 100

print(f"\nEstudiantes con excelencia en Matematicas (>= 70 pts): {total_destacados:,} evaluados")
print(f"Representan el {porcentaje_excelencia:.2f}% de la poblacion total evaluada.\n")

print("Distribucion de los estudiantes destacados por Area Geografica:")

destacados.groupBy('COLE_AREA_UBICACION') \
    .agg(F.count('*').alias('Estudiantes_Destacados')) \
    .withColumn(
        "Porcentaje",
        F.round((F.col("Estudiantes_Destacados") / total_destacados) * 100, 2)
    ) \
    .show(truncate=False)

# RESPUESTA AL INTERROGANTE: BRECHAS CONTEXTUALES
print("\n=======================================================")
print(">>> BRECHA TERRITORIAL (URBANO VS RURAL) <<<")
print("=======================================================")

# Comparación de medias según el área de ubicación del colegio
df_clean.groupBy('COLE_AREA_UBICACION') \
    .agg(
        F.count('*').alias('Total_Evaluados'),
        F.round(F.avg('PUNT_MATEMATICAS'), 2).alias('Prom_Matematicas'),
        F.round(F.avg('PUNT_INGLES'), 2).alias('Prom_Ingles')
    ).show(truncate=False)

print("\n=======================================================")
print(">>> BRECHA INSTITUCIONAL (OFICIAL VS NO OFICIAL) <<<")
print("=======================================================")

# Comparación de medias según la naturaleza de la institución
df_clean.groupBy('COLE_NATURALEZA') \
    .agg(
        F.count('*').alias('Total_Evaluados'),
        F.round(F.avg('PUNT_MATEMATICAS'), 2).alias('Prom_Matematicas'),
        F.round(F.avg('PUNT_INGLES'), 2).alias('Prom_Ingles')
    ).show(truncate=False)

# CONSULTA ADICIONAL: RANKING TERRITORIAL POR MUNICIPIOS
print("\n=======================================================")
print(">>> TOP 10 MUNICIPIOS CON MEJOR DESEMPEÑO PROMEDIO <<<")
print("=======================================================")

# Filtramos municipios con muestra representativa (mínimo 50 evaluados) y ordenamos descendente
df_clean.groupBy('COLE_MCPIO_UBICACION') \
    .agg(
        F.count('*').alias('Evaluados'),
        F.round(F.avg('PUNT_MATEMATICAS'), 2).alias('Prom_Matematicas'),
        F.round(F.avg('PUNT_INGLES'), 2).alias('Prom_Ingles')
    ) \
    .filter(F.col('Evaluados') >= 50) \
    .sort(F.col('Prom_Matematicas').desc()) \
    .show(10, truncate=False)

print("\n[OK] PROCESAMIENTO EN BATCH Y ANÁLISIS EDA FINALIZADOS CON ÉXITO.\n")
