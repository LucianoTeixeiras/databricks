# Databricks notebook source
# MAGIC %md
# MAGIC # Big Data para Cientista de Dados

# COMMAND ----------

# Lendo o arquivo de dados
arquivo = "/FileStore/tables/bronze/2015_summary.csv"

# COMMAND ----------

# lendo o arquivo de dados
# inferSchema = True
# header = True

flightData2015 = spark\
.read.format("csv")\
.option("inferSchema", "True")\
.option("header", "True")\
.csv(arquivo)

# COMMAND ----------

# imprime os datatypes das colunas do dataframe
flightData2015.printSchema()

# COMMAND ----------

# imprime o tipo da variável flightData2015
type(flightData2015)

# COMMAND ----------

# retorna as primeiras 3 linhas do dataframe em formato de array.
flightData2015.take(5)

# COMMAND ----------

# Usando o comando display
display(flightData2015.show(3))

# COMMAND ----------

# imprime a quantidade de linhas no dataframe.
flightData2015.count()

# COMMAND ----------

# lendo o arquivo previamente com a opção inferSchema desligada
flightData2015 = spark\
.read\
.option("inferSchema", "True")\
.option("header", "True")\
.csv(arquivo)

# COMMAND ----------

# DBTITLE 1,Lendo vários arquivos e criando um dataframe
df = spark\
.read\
.option("inferSchema", "True")\
.option("header", "True")\
.csv("/FileStore/tables/bronze/*.csv")

# COMMAND ----------

df.show(10)

# COMMAND ----------

# imprime a quantidade de linhas do datafrme
df.count()

# COMMAND ----------

# Opções de Plots
display(df.head(10))

# COMMAND ----------

# MAGIC %md
# MAGIC #Trabalhando com SQL

# COMMAND ----------

# DBTITLE 1,Apaga a tabela global "all_files" caso ela exista
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS all_files;

# COMMAND ----------

# DBTITLE 1,Criando uma tabela global no banco de dados do Spark
# MAGIC %sql
# MAGIC CREATE TABLE all_files
# MAGIC USING csv
# MAGIC OPTIONS (path "/FileStore/tables/bronze/*.csv", header "true")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Consutando dados usando a linguagem SQL
# MAGIC SELECT * FROM all_files;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Consutando dados usando a linguagem SQL
# MAGIC SELECT count(*) FROM all_files;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Consutando dados usando a linguagem SQL
# MAGIC SELECT DEST_COUNTRY_NAME
# MAGIC        ,avg(count) AS Quantidade_Paises
# MAGIC FROM all_files
# MAGIC GROUP BY DEST_COUNTRY_NAME
# MAGIC ORDER BY DEST_COUNTRY_NAME;

# COMMAND ----------

# Create a view or table temporária.
df.createOrReplaceTempView("2015_summary_csv")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from 2015_summary_csv

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query na view 2015_summary_csv com multiplicação.
# MAGIC SELECT DEST_COUNTRY_NAME 
# MAGIC       ,ORIGIN_COUNTRY_NAME
# MAGIC       ,count * 10 as count_multiplicado_por_dez
# MAGIC FROM 2015_summary_csv

# COMMAND ----------

# DBTITLE 1,Formas diferentes de consultar Dataframes
from pyspark.sql.functions import max
df.select(max("count")).take(1)

# COMMAND ----------

# Filtrando linhas de um dataframe usando filter
df.filter("count < 2").show(2)

# COMMAND ----------

# Usando where (um alias para o metodo filter)
df.where("count < 2").show(2)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- filtrando linhas com sql
# MAGIC SELECT * 
# MAGIC FROM 2015_summary_csv
# MAGIC WHERE count < 2
# MAGIC LIMIT 2

# COMMAND ----------

# obtendo linhas únicas
df.select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").distinct().count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Manipulando Dataframes

# COMMAND ----------

df.sort("count").show(5)

# COMMAND ----------

from pyspark.sql.functions import desc, asc, expr
# ordenando por ordem crescente
df.orderBy(expr("count desc")).show(10)

# COMMAND ----------

# visualizando estatísticas descritivas
df.describe().show()

# COMMAND ----------

# iterando sobre todas as linhas do dataframe
for i in df.collect():
  #print (i)
  print(i[0], i[1], i[2] * 2)

# COMMAND ----------

# DBTITLE 1,Trabalhando com Strings
from pyspark.sql.functions import lower, upper, col
df.select(col("DEST_COUNTRY_NAME"),lower(col("DEST_COUNTRY_NAME")),upper(lower(col("DEST_COUNTRY_NAME")))).show(10)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Usando SQL..
# MAGIC SELECT DEST_COUNTRY_NAME
# MAGIC       ,lower(DEST_COUNTRY_NAME)
# MAGIC       ,Upper(DEST_COUNTRY_NAME)
# MAGIC FROM 2015_summary_csv

# COMMAND ----------

# remove espaços em branco a esquerda
from pyspark.sql.functions import ltrim
df.select(ltrim(col("DEST_COUNTRY_NAME"))).show(2)

# COMMAND ----------

# remove espaços a direita
from pyspark.sql.functions import rtrim
df.select(rtrim(col("DEST_COUNTRY_NAME"))).show(2)

# COMMAND ----------

# todas as operações juntas..
# a função lit cria uma coluna na cópia do dataframe
from pyspark.sql.functions import lit, ltrim, rtrim, rpad, lpad, trim
df.select(
ltrim(lit(" HELLO ")).alias("ltrim"),
rtrim(lit(" HELLO ")).alias("rtrim"),
trim(lit(" HELLO ")).alias("trim"),
lpad(lit("HELLO"), 3, " ").alias("lp"),
rpad(lit("HELLO"), 10, " ").alias("rp")).show(2)

# COMMAND ----------

df.orderBy("count", "DEST_COUNTRY_NAME").show(5)

# COMMAND ----------

from pyspark.sql.functions import desc, asc
df.orderBy(expr("count desc")).show(2)
df.orderBy(col("count").desc(), col("DEST_COUNTRY_NAME").asc()).show(2)

# COMMAND ----------

# DBTITLE 1,Comparando a performance
# utilizando SQL
sqlWay = spark.sql("""
SELECT DEST_COUNTRY_NAME, count(1)
FROM 2015_summary_csv
GROUP BY DEST_COUNTRY_NAME
""")

# COMMAND ----------

# Utilizando Python
dataFrameWay = df.groupBy("DEST_COUNTRY_NAME").count()

# COMMAND ----------

# imprime o plano de execução do código
sqlWay.explain()

# COMMAND ----------

# imprime o plano de execução do código
dataFrameWay.explain()

# COMMAND ----------

# DBTITLE 1,Lendo o dataset retail-data..
df = spark.read.format("csv")\
.option("header", "true")\
.option("inferSchema", "true")\
.load("/FileStore/tables/bronze/2010_12_01.csv")


# COMMAND ----------

#imprime as  10 primeiras linhas
display(df.head(10))

# COMMAND ----------

# DBTITLE 1,Trabalhando com Data Analysis
# Tipos Boleanos
from pyspark.sql.functions import col
df.where(col("InvoiceNo") != 536365)\
.select("InvoiceNo", "Description")\
.show(5)

# COMMAND ----------

# cria a tabela temporária dftrable
df.createOrReplaceTempView("dfTable")

# COMMAND ----------

# imprime 10 primeiras linhas
display(df.head(10))

# COMMAND ----------

# usando o operador boleando com um predicado em uma expressão.
df.where("InvoiceNo <> 536365").show(5)

# COMMAND ----------

# usando o operador boleando com um predicado em uma expressão.
df.where("InvoiceNo = 536365").show(5)

# COMMAND ----------

# Entendendo a ordem dos operadores boleanos
from pyspark.sql.functions import instr
priceFilter = col("UnitPrice") > 600
descripFilter = instr(df.Description, "POSTAGE") >= 1

# COMMAND ----------

# aplicando os operadores como filtros
df.where(df.StockCode.isin("DOT")).where(priceFilter | descripFilter).show()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Aplicando a mesmo código em SQL
# MAGIC SELECT * 
# MAGIC FROM dfTable 
# MAGIC WHERE StockCode in ("DOT")
# MAGIC AND(UnitPrice > 600 OR instr(Description, "POSTAGE") >= 1)

# COMMAND ----------

# Combinando filtros e operadores boleanos
from pyspark.sql.functions import instr
DOTCodeFilter = col("StockCode") == "DOT"
priceFilter = col("UnitPrice") > 600
descripFilter = instr(col("Description"), "POSTAGE") >= 1


# COMMAND ----------

# Combinando filtros e operadores boleanos
df.withColumn("isExpensive", DOTCodeFilter & (priceFilter | descripFilter))\
.where("isExpensive")\
.select("unitPrice", "isExpensive").show(5)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Aplicando as mesmas ideias usando SQL
# MAGIC SELECT UnitPrice, (StockCode = 'DOT' AND
# MAGIC (UnitPrice > 600 OR instr(Description, "POSTAGE") >= 1)) as isExpensive
# MAGIC FROM dfTable
# MAGIC WHERE (StockCode = 'DOT' AND
# MAGIC (UnitPrice > 600 OR instr(Description, "POSTAGE") >= 1))

# COMMAND ----------

# MAGIC %md
# MAGIC # Trabalhando com tipos diferentes de arquivos

# COMMAND ----------

# MAGIC %md
# MAGIC ### Modos de leitura
# MAGIC - **permissive**: *Define todos os campos para NULL quando encontra registros corrompidos e coloca todos registros corrompidos em uma coluna chamada _corrupt_record.* (default)
# MAGIC 
# MAGIC - **dropMalformed**: *Apaga uma linha corrompida ou que este não consiga ler.*
# MAGIC 
# MAGIC - **failFast**: *Falha imediatamente quando encontra uma linha que não consiga ler.*

# COMMAND ----------

# Lendo arquivos csv
spark.read.format("csv")
.option("mode", "permissive")
.option("inferSchema", "true")
.option("path", "path/to/file(s)")
.schema(someSchema)
.load()

# COMMAND ----------

# leia o arquivo alterando os modos de leitura (failfast, permissive, dropmalformed)
df = spark.read.format("csv")\
.option("mode", "failfast")\
.option("header", "true")\
.option("inferSchema", "true")\
.load("/FileStore/tables/bronze/2010_12_01.csv")

# COMMAND ----------

# imprime as 10 primeiras linhas do dataframe
display(df.head(10))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Criando um schema
# MAGIC - A opção **infer_schema** nem sempre vai definir o melhor datatype.
# MAGIC - Melhora a performance na leitura de grandes bases.
# MAGIC - Permite uma customização dos tipos das colunas.
# MAGIC - É importante saber para reescrita de aplicações. (Códigos pandas)

# COMMAND ----------

# imprime o schema do dataframe (infer_schema=True)
df.printSchema()

# COMMAND ----------

# usa o objeto StructType
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DoubleType, TimestampType
schema_df = StructType([
    StructField("InvoiceNo", IntegerType()),
    StructField("StockCode", IntegerType()),
    StructField("Description", StringType()),
    StructField("Quantity", IntegerType()),
    StructField("InvoiceDate", TimestampType()),
    StructField("UnitPrice", DoubleType()),
    StructField("CustomerID", DoubleType()),
    StructField("Country", StringType())
])

# COMMAND ----------

# verificando o tipo da variável schema_df
type(schema_df)

# COMMAND ----------

# usando o parâmetro schema()
df = spark.read.format("csv")\
.option("header", "True")\
.schema(schema_df)\
.option("timestampFormat",'yyyy-/MM/DD hh:mm:ss')\
.load("/FileStore/tables/bronze/2010_12_01.csv")

# COMMAND ----------

# imprime o schema do dataframe.
df.printSchema()

# COMMAND ----------

# imprime 10 primeiras linhas do dataframe.
display(df.collect())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Arquivos JSON

# COMMAND ----------

df_json = spark.read.format("json")\
.option("mode", "FAILFAST")\
.option("inferSchema", "true")\
.load("/FileStore/tables/bronze/2010_summary.json")

# COMMAND ----------

df_json.printSchema()

# COMMAND ----------

display(df_json.head(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Escrevendo arquivos
# MAGIC - **append** : Adiciona arquivos de saída na lista de arquivos que já existem na localizaçao.
# MAGIC - **overwrite** : Sobreescreve os arquivos no destino.
# MAGIC - **erroIfExists** : Emite um erro e para se já existir arquivos no destino.
# MAGIC - **ignore** : Se existir o dado no destino náo faz nada.

# COMMAND ----------

# escrevendo arquivos csv
df.write.format("csv")\
.mode("overwrite") \
.option("sep", ",") \
.save("/FileStore/tables/bronze/saida_2010_12_01.csv")

# COMMAND ----------

# observe o arquivo gerado.
file = "/FileStore/tables/bronze/saida_2010_12_01.csv/part-00000-tid-513137111285552141-fa5fcb38-55a1-4a12-ac99-df3fa327627c-83-1-c000.csv"
df = spark.read.format("csv")\
.option("header", "True")\
.option("inferSchema", "True")\
.option("timestampFormat",'yyyy-/MM/DD hh:mm:ss')\
.load(file)

# COMMAND ----------

# imprime as 10 primeiras linhas do dataframe 
df.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Escrevendo dados em paralelo

# COMMAND ----------

# reparticionando o dado arquivos csv
# observe o diretório criado
df.repartition(5).write.format("csv")\
.mode("overwrite") \
.option("sep", ",") \
.save("/FileStore/tables/bronze/saida_2010_12_01.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Arquivos Parquet

# COMMAND ----------

# MAGIC %md
# MAGIC #####**Convertendo .csv para .parquet**
# MAGIC - Dataset .csv usado https://www.kaggle.com/nhs/general-practice-prescribing-data

# COMMAND ----------

# Lendo todos os arquivos .csv do diretório bigdata (>4GB)
df = spark.read.format("csv")\
.option("header", "True")\
.option("inferSchema","True")\
.load("/FileStore/tables/bigdata/*.csv")

# COMMAND ----------

display(df.head(10))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# conta a quantidade de linhas
df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC *Atente para NÃO escrever e ler arquivos parquet em versoes diferentes*

# COMMAND ----------

# escrevendo em formato parquet
df.write.format("parquet")\
.mode("overwrite")\
.save("/FileStore/tables/bronze/df-parquet-file.parquet")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/bronze/df-parquet-file.parquet

# COMMAND ----------

# lendo arquivos parquet
# atente para a velocidade de leitura
df_parquet = spark.read.format("parquet")\
.load("/FileStore/tables/bronze/df-parquet-file.parquet")

# COMMAND ----------

# conta a quantidade de linhas do dataframe
df_parquet.count()

# COMMAND ----------

# visualizando o dataframe
display(df_parquet.head(10))

# COMMAND ----------

# visualizando o tamanho dos arquivos
display(dbutils.fs.ls("/FileStore/tables/bronze/df-parquet-file.parquet"))

# COMMAND ----------

# MAGIC %scala
# MAGIC // script para pegar tamanho em Gigabytes
# MAGIC val path="/FileStore/tables/bronze/df-parquet-file.parquet"
# MAGIC val filelist=dbutils.fs.ls(path)
# MAGIC val df_temp = filelist.toDF()
# MAGIC df_temp.createOrReplaceTempView("adlsSize")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- consulta a view criada.
# MAGIC select round(sum(size)/(1024*1024*1024),3) as sizeInGB from adlsSize

# COMMAND ----------

# MAGIC %md
# MAGIC ### Spark + PostgreSQL
# MAGIC - Consultar e escrever em um banco de dados relacional.

# COMMAND ----------

# Isso é equivalente a executar uma query como: select * from pg_catalog.pg_tables
# jdbc:postgresql://pgserver-1.postgres.database.azure.com:5432/{your_database}?user=stack_user@pgserver-1&password={your_password}&sslmode=require
pgDF = spark.read.format("jdbc")\
.option("driver", "org.postgresql.Driver")\
.option("url", "jdbc:postgresql://pgserver-1.postgres.database.azure.com:5432/postgres?user=stack_user@pgserver-1&password=Bigdata2021&sslmode=require")\
.option("dbtable", "pg_catalog.pg_tables")\
.option("user", "stack_user").option("password", "Bigdata2021").load()

# COMMAND ----------

# imprime todas as linhas do dataframe
display(pgDF.collect())

# COMMAND ----------

# consulta dados da coluna schemaname
pgDF.select("schemaname").distinct().show()

# COMMAND ----------

# Especifica uma query diretamente.
# Útil para evitar o "select * from."
pgDF = spark.read.format("jdbc")\
.option("driver", "org.postgresql.Driver")\
.option("url", "jdbc:postgresql://pgserver-1.postgres.database.azure.com:5432/postgres?user=stack_user@pgserver-1&password=Bigdata2021&sslmode=require")\
.option("query", "select schemaname,tablename from pg_catalog.pg_tables")\
.option("user", "stack_user").option("password", "Bigdata2021").load()

# COMMAND ----------

# imprime todas as linhas do dataframe
display(pgDF.collect())

# COMMAND ----------

# imprime as 5 linhas do dataframe df
# não se esqueça de recriar esse dataframe.
df.show(5)

# COMMAND ----------

# cria a tabela "produtos" a apartir dos dados do dataframe df.
pgDF.write.mode("overwrite")\
.format("jdbc")\
.option("url", "jdbc:postgresql://pgserver-1.postgres.database.azure.com:5432/postgres?user=stack_user@pgserver-1&password=Bigdata2021&sslmode=require")\
.option("dbtable", "produtos")\
.option("user", "stack_user")\
.option("password", "Bigdata2021")\
.save()

# COMMAND ----------

# cria o dataframe df_produtos a partir da tabela criada.
df_produtos = spark.read.format("jdbc")\
.option("driver", "org.postgresql.Driver")\
.option("url", "jdbc:postgresql://pgserver-1.postgres.database.azure.com:5432/postgres?user=stack_user@pgserver-1&password=Bigdata2021&sslmode=require")\
.option("dbtable", "produtos")\
.option("user", "stack_user").option("password", "Bigdata2021").load()

# COMMAND ----------

# imprime as linhas do dataframe.
display(df_produtos.collect())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Avançando com Pyspark

# COMMAND ----------

# MAGIC %md
# MAGIC - **mean()** - Retorna o valor médio de cada grupo.
# MAGIC 
# MAGIC - **max()** - Retorna o valor máximo de cada grupo.
# MAGIC 
# MAGIC - **min()** - Retorna o valor mínimo de cada grupo.
# MAGIC 
# MAGIC - **sum()** - Retorna a soma de todos os valores do grupo.
# MAGIC 
# MAGIC - **avg()** - Retorna o valor médio de cada grupo.

# COMMAND ----------

# imprime as 10 primeiras linhas do dataframe
df.show(10)

# COMMAND ----------

# Soma preços unitários por país
df.groupBy("Country").sum("UnitPrice").show()

# COMMAND ----------

# Conta a quantidade de países distintos.
df.groupBy("Country").count().show()

# COMMAND ----------

# retorna o valor mínimo por grupo
df.groupBy("Country").min("UnitPrice").show()

# COMMAND ----------

# retorna o valor mínimo por grupo
df.groupBy("Country").max("UnitPrice").show()

# COMMAND ----------

# retorna o valor médio por grupo
df.groupBy("Country").avg("UnitPrice").show()

# COMMAND ----------

# retorna o valor médio por grupo
df.groupBy("Country").mean("UnitPrice").show()

# COMMAND ----------

# GroupBy várias colunas
df.groupBy("Country","CustomerID") \
    .sum("UnitPrice") \
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Trabalhando com datas
# MAGIC - Existem diversas funçoes em Pyspark para manipular datas e timestamp.
# MAGIC - Evite escrever suas próprias funçoes para isso.
# MAGIC - Algumas funcoes mais usadas:
# MAGIC     - current_day():
# MAGIC     - date_format(dateExpr,format):
# MAGIC     - to_date():
# MAGIC     - to_date(column, fmt):
# MAGIC     - add_months(Column, numMonths):
# MAGIC     - date_add(column, days):
# MAGIC     - date_sub(column, days):
# MAGIC     - datediff(end, start)
# MAGIC     - current_timestamp():
# MAGIC     - hour(column):

# COMMAND ----------

# imprime o dataframe
df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import *
#current_date()
df.select(current_date().alias("current_date")).show(1)

# COMMAND ----------

#date_format()
df.select(col("InvoiceDate"), \
          date_format(col("InvoiceDate"), "dd-MM-yyyy hh:mm:ss")\
          .alias("date_format")).show()

# COMMAND ----------

#datediff
df.select(col("InvoiceDate"),
    datediff(current_date(),col("InvoiceDate")).alias("datediff")  
  ).show()

# COMMAND ----------

#months_between()
df.select(col("InvoiceDate"), 
    months_between(current_date(),col("InvoiceDate")).alias("months_between")  
  ).show()

# COMMAND ----------

# utiliza as funçoes para adicionar, subtrair meses e dias
df.select(col("InvoiceDate"), 
    add_months(col("InvoiceDate"),3).alias("add_months"), 
    add_months(col("InvoiceDate"),-3).alias("sub_months"), 
    date_add(col("InvoiceDate"),4).alias("date_add"), 
    date_sub(col("InvoiceDate"),4).alias("date_sub") 
  ).show()

# COMMAND ----------

# Extrai ano, mës, próximo dia, dia da semana.
df.select(col("InvoiceDate"), 
     year(col("InvoiceDate")).alias("year"), 
     month(col("InvoiceDate")).alias("month"), 
     next_day(col("InvoiceDate"),"Sunday").alias("next_day"), 
     weekofyear(col("InvoiceDate")).alias("weekofyear") 
  ).show()

# COMMAND ----------

# Dia da semana, dia do mës, dias do ano
df.select(col("InvoiceDate"),  
     dayofweek(col("InvoiceDate")).alias("dayofweek"), 
     dayofmonth(col("InvoiceDate")).alias("dayofmonth"), 
     dayofyear(col("InvoiceDate")).alias("dayofyear"), 
  ).show()

# COMMAND ----------

# imprime o timestamp atual
df.select(current_timestamp().alias("current_timestamp")
  ).show(1,truncate=False)

# COMMAND ----------

# retorna hora, minuto e segundo
df.select(col("InvoiceDate"), 
    hour(col("InvoiceDate")).alias("hour"), 
    minute(col("InvoiceDate")).alias("minute"),
    second(col("InvoiceDate")).alias("second") 
  ).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Missing Values com Pyspark

# COMMAND ----------

# visualizando datasets de exemplos da databricks
display(dbutils.fs.ls("/databricks-datasets"))

# COMMAND ----------

# lendo o arquivo de dados
# inferSchema = True
# header = True

arquivo = "dbfs:/databricks-datasets/flights/"

df = spark \
.read.format("csv")\
.option("inferSchema", "True")\
.option("header", "True")\
.csv(arquivo)

# COMMAND ----------

df.show()

# COMMAND ----------

df.filter("delay is NULL").show()

# COMMAND ----------

# filtrando valores missing
df.filter(df.delay.isNull()).show(10)

# COMMAND ----------

# preenche os dados missing com o valor 0
df.na.fill(value=0).show()

# COMMAND ----------

# preenche valores missing com valor 0 apenas da coluna delay
df.na.fill(value=0, subset=['delay']).show()

# COMMAND ----------

# preenche os dados com valores de string vazia
df.na.fill("").show(100)

# COMMAND ----------

df.filter("delay is NULL").show()

# COMMAND ----------

# remove qualquer linha nula de qualquer coluna
df.na.drop().show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Tarefas básicas em dataframes

# COMMAND ----------

# Adicionando uma coluna ao dataframe
df = df.withColumn('Nova Coluna', df['delay']+2)
df.show(10)

# COMMAND ----------

# Reovendo coluna
df = df.drop('Nova Coluna')
df.show(10)

# COMMAND ----------

# Renomenando uma coluna no dataframe
df.withColumnRenamed('Nova Coluna','Delay_2').show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Trabalhando com UDFs
# MAGIC - Integraçáo de código entre as APIs
# MAGIC - É preciso cuidado com performance dos códigos usando UDFs

# COMMAND ----------

from pyspark.sql.types import LongType
# define a função
def quadrado(s):
  return s * s

# COMMAND ----------

# registra no banco de dados do spark e define o tipo de retorno por padrão é stringtype
from pyspark.sql.types import LongType
spark.udf.register("Func_Py_Quadrado", quadrado, LongType())

# COMMAND ----------

# gera valores aleatórios
spark.range(1, 20).show()

# COMMAND ----------

# cria a visão View_temp
spark.range(1, 20).createOrReplaceTempView("View_temp")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Usando a função criada em python juntamente com código SQL
# MAGIC select id, Func_Py_Quadrado(id) as id_ao_quadrado
# MAGIC from View_temp

# COMMAND ----------

# MAGIC %md
# MAGIC ##### UDFs com Dataframes

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import LongType
Func_Py_Quadrado = udf(quadrado, LongType())

# COMMAND ----------

df = spark.table("View_temp")

# COMMAND ----------

df.show()

# COMMAND ----------

display(df.select("id", Func_Py_Quadrado("id").alias("id_quadrado")))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Koalas
# MAGIC - Koalas é um projeto de código aberto que fornece um substituto imediato para os pandas. 
# MAGIC - O pandas é comumente usado por ser um pacote que fornece estruturas de dados e ferramentas de análise de dados fáceis de usar para a linguagem de programação Python.
# MAGIC - O Koalas preenche essa lacuna fornecendo APIs equivalentes ao pandas que funcionam no Apache Spark. 
# MAGIC - Koalas é útil não apenas para usuários de pandas, mas também para usuários de PySpark.
# MAGIC   - Koalas suporta muitas tarefas que são difíceis de fazer com PySpark, por exemplo, plotar dados diretamente de um PySpark DataFrame.
# MAGIC - Koalas suporta SQL diretamente em seus dataframes.

# COMMAND ----------

import numpy as np
import pandas as pd
import databricks.koalas as ks

# COMMAND ----------

# cria um pandas DataFrame
pdf = pd.DataFrame({'A': np.random.rand(5),
                    'B': np.random.rand(5)})

# COMMAND ----------

# imprime um pandas dataframe
type(pdf)

# COMMAND ----------

# Cria um Koalas DataFrame
kdf = ks.DataFrame({'A': np.random.rand(5),
                    'B': np.random.rand(5)})

# COMMAND ----------

# imprime o tipo de dados
type(kdf)

# COMMAND ----------

# Cria um Koalas dataframe a partir de um pandas dataframe
kdf = ks.DataFrame(pdf)
type(kdf)

# COMMAND ----------

# outra forma de converter
kdf = ks.from_pandas(pdf)
type(kdf)

# COMMAND ----------

# métodos já conhecidos
pdf.head()

# COMMAND ----------

# métodos já conhecidos
kdf.head()

# COMMAND ----------

# método describe()
kdf.describe()

# COMMAND ----------

# ordenando um dataframe
kdf.sort_values(by='B')

# COMMAND ----------

# define configurações de layout de células
from databricks.koalas.config import set_option, get_option
ks.get_option('compute.max_rows')
ks.set_option('compute.max_rows', 2000)

# COMMAND ----------

# slice
kdf[['A', 'B']]

# COMMAND ----------

# loc
kdf.loc[1:2]

# COMMAND ----------

# iloc
kdf.iloc[:3, 1:2]

# COMMAND ----------

# MAGIC %md
# MAGIC ** Usando funções python com dataframe koalas**

# COMMAND ----------

# cria função python
def quadrado(x):
    return x ** 2

# COMMAND ----------

# habilita computação de dataframes e séries.
from databricks.koalas.config import set_option, reset_option
set_option("compute.ops_on_diff_frames", True)

# COMMAND ----------

# cria uma nova coluna a partir da função quadrado
kdf['C'] = kdf.A.apply(quadrado)

# COMMAND ----------

# visualizando o dataframe
kdf.head()

# COMMAND ----------

# agrupando dados
kdf.groupby('A').sum()

# COMMAND ----------

# agrupando mais de uma coluna
kdf.groupby(['A', 'B']).sum()

# COMMAND ----------

# This is needed for visualizing plot on notebook
%matplotlib inline

# COMMAND ----------

speed = [0.1, 17.5, 40, 48, 52, 69, 88]
lifespan = [2, 8, 70, 1.5, 25, 12, 28]

index = ['snail', 'pig', 'elephant',
         'rabbit', 'giraffe', 'coyote', 'horse']

kdf = ks.DataFrame({'speed': speed,
                   'lifespan': lifespan}, index=index)
kdf.plot.bar()

# COMMAND ----------

# MAGIC %md
# MAGIC **Usando SQL no Koalas**

# COMMAND ----------

# cria um dataframe Koalas
kdf = ks.DataFrame({'year': [1990, 1997, 2003, 2009, 2014],
                    'pig': [20, 18, 489, 675, 1776],
                    'horse': [4, 25, 281, 600, 1900]})

# COMMAND ----------

# Faz query no dataframe koalas
ks.sql("SELECT * FROM {kdf} WHERE pig > 100")

# COMMAND ----------

# cria um dataframe pandas
pdf = pd.DataFrame({'year': [1990, 1997, 2003, 2009, 2014],
                    'sheep': [22, 50, 121, 445, 791],
                    'chicken': [250, 326, 589, 1241, 2118]})

# COMMAND ----------

# Query com inner join entre dataframe pandas e koalas
ks.sql('''
    SELECT ks.pig, pd.chicken
    FROM {kdf} ks INNER JOIN {pdf} pd
    ON ks.year = pd.year
    ORDER BY ks.pig, pd.chicken''')

# COMMAND ----------

# converte koalas dataframe para Pyspark
kdf = ks.DataFrame({'A': [1, 2, 3, 4, 5], 'B': [10, 20, 30, 40, 50]})
pydf = kdf.to_spark()

# COMMAND ----------

type(pydf)
