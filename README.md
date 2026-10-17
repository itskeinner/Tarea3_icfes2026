# Tarea3_icfes2026
====================================================================
ANÁLISIS EDA DE RESULTADOS SABER 11 CON APACHE SPARK (BATCH)
====================================================================

Este documento contiene la documentación del script de PySpark diseñado para realizar un Análisis Exploratorio de Datos (EDA) y diagnóstico estadístico sobre los resultados de las pruebas Saber 11 en Colombia.

El procesamiento se realiza en modalidad Batch (por lotes), leyendo los datos directamente desde el sistema de archivos distribuido HDFS (Hadoop Distributed File System).

REQUISITOS PREVIOS Y ENTORNO

Para ejecutar este script correctamente, el clúster o la máquina virtual deben contar con las siguientes tecnologías configuradas y en ejecución:

Apache Hadoop: Con HDFS operativo (el script asume que el NameNode está en localhost:9000).

Apache Spark: Con soporte para PySpark instalado (versión 3.x recomendada).

Python: Versión 3.8 o superior.

Dataset: El archivo saber11.csv debe estar cargado previamente en la ruta especificada de HDFS.

ESTRUCTURA ESPERADA EN HDFS
El script lee el archivo desde la siguiente ruta:
hdfs://localhost:9000/Tarea3/Saber11/saber11.csv

Nota: Si tu clúster está en otra máquina o IP, deberás cambiar 'localhost' por la IP o hostname correspondiente.

EXPLICACIÓN DETALLADA DEL CÓDIGO

El script está dividido en varias fases lógicas, cada una respondiendo a un interrogante específico del análisis.

2.1. INICIALIZACIÓN Y CONFIGURACIÓN
Código:
spark = SparkSession.builder.appName('Analisis_Saber11_Batch').getOrCreate()
spark.sparkContext.setLogLevel("WARN")

SparkSession: Es el punto de entrada para programar con el Dataset y DataFrame API de Spark.

setLogLevel("WARN"): Se configura para que la consola no se sature con mensajes de INFO de Spark, mostrando únicamente advertencias (WARN) y errores (ERROR). Esto hace que la salida del análisis sea mucho más legible.

2.2. FASE 1: INGESTA Y ESQUEMA ESTRUCTURAL
Código:
df = spark.read.format('csv')
.option('header', 'true')
.option('inferSchema', 'true')
.load(file_path)

Ingesta: Se lee el archivo CSV desde HDFS.

header='true': Indica que la primera fila del CSV contiene los nombres de las columnas.

inferSchema='true': Spark analiza los datos y asigna automáticamente los tipos de datos (enteros, strings, dobles) a cada columna.

Conteo Inicial: Se imprime el número total de registros (df.count()) para verificar que la carga fue exitosa.

2.3. LIMPIEZA Y TIPADO DE VARIABLES
Código:
df_clean = df.filter(F.col("PUNT_MATEMATICAS").isNotNull())
.withColumn("PUNT_MATEMATICAS", F.col("PUNT_MATEMATICAS").cast(IntegerType()))
.withColumn("PUNT_INGLES", F.col("PUNT_INGLES").cast(IntegerType()))

Filtro: Se eliminan los registros donde el puntaje de matemáticas es nulo (isNotNull()), asegurando que los promedios posteriores sean exactos.

Casting: Se fuerza la conversión de las columnas de puntajes a tipo entero (IntegerType()). Esto es vital para realizar operaciones matemáticas y estadísticas correctamente.

2.4. DIAGNÓSTICO ESTADÍSTICO GENERAL
Código:
df_clean.select('PUNT_MATEMATICAS', 'PUNT_INGLES').summary().show()

summary(): Genera un cuadro resumen estadístico (Count, Mean, Stddev, Min, Max, y percentiles 25%, 50%, 75%) para las columnas seleccionadas. Permite entender rápidamente la distribución general de los puntajes.

2.5. AISLAMIENTO DE LA EXCELENCIA ACADÉMICA (PUNTAJE >= 70)
Código:
umbral_excelencia = 70
destacados = df_clean.filter(F.col('PUNT_MATEMATICAS') >= umbral_excelencia)

Filtro: Se crea un DataFrame 'destacados' con los estudiantes que obtuvieron 70 o más puntos en matemáticas.

Cálculo del Porcentaje: Se calcula qué porcentaje del total evaluado representa este grupo de élite.

Distribución Geográfica: Se agrupa por COLE_AREA_UBICACION (Urbano/Rural) para ver dónde se concentran los estudiantes destacados.

2.6. ANÁLISIS DE BRECHAS CONTEXTUALES
El script analiza dos tipos de brechas fundamentales en la educación colombiana:

A. Brecha Territorial (Urbano vs. Rural):
df_clean.groupBy('COLE_AREA_UBICACION').agg(...)
Agrupa los datos por área de ubicación del colegio y calcula el promedio de matemáticas e inglés. Esto permite evidenciar si existe una diferencia significativa entre las zonas urbanas y rurales.

B. Brecha Institucional (Oficial vs. No Oficial):
df_clean.groupBy('COLE_NATURALEZA').agg(...)
Agrupa por la naturaleza del colegio (Público/Oficial vs. Privado/No Oficial), calculando promedios y cantidad de evaluados para comparar el rendimiento entre ambos sectores.

2.7. RANKING TERRITORIAL (TOP 10 MUNICIPIOS)
Código:
df_clean.groupBy('COLE_MCPIO_UBICACION')
.agg(...)
.filter(F.col('Evaluados') >= 50)
.sort(F.col('Prom_Matematicas').desc())
.show(10, truncate=False)

Agrupación: Agrupa los datos por municipio (COLE_MCPIO_UBICACION).

Filtro de Representatividad: Se filtran los municipios que tengan al menos 50 evaluados. Esto evita que municipios con muy pocos estudiantes (ej. 2 o 3) y un puntaje alto distorsionen el ranking.

Ordenamiento: Se ordena de forma descendente (desc()) basándose en el promedio de matemáticas.

Visualización: Se muestran los 10 mejores municipios.

¿CÓMO EJECUTAR EL SCRIPT?

Asegúrate de tener Hadoop y Spark corriendo.

Verifica que el archivo CSV esté en la ruta correcta de HDFS:
hdfs dfs -ls /Tarea3/Saber11/

Ejecuta el script de Python utilizando spark-submit (la forma estándar de enviar aplicaciones a Spark):
spark-submit nombre_del_archivo.py

(Si solo quieres probarlo en tu entorno local sin clúster, puedes ejecutarlo con 'python nombre_del_archivo.py' siempre que tengas PySpark instalado vía pip).

RESULTADOS ESPERADOS

Al finalizar, la consola imprimirá:

El total de filas procesadas.

Un resumen estadístico de los puntajes.

El porcentaje de estudiantes con excelencia académica y su ubicación.

Tablas comparativas de rendimiento por área (Urbano/Rural) y naturaleza (Oficial/No Oficial).

Una tabla con los 10 mejores municipios en matemáticas.

Un mensaje final de éxito: [OK] PROCESAMIENTO EN BATCH Y ANÁLISIS EDA FINALIZADOS CON ÉXITO.

====================================================================
Documentación generada para el análisis de datos masivos con PySpark.
====================================================================
