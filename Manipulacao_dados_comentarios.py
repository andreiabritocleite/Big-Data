# Databricks notebook source
#A biblioteca PySpark, trabalha com o Apache Spark, que é um framework de processamento distribuído para big data. 

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, FloatType
import pyspark.sql.functions as F


# COMMAND ----------


spark = SparkSession.builder \
    .master('local[*]') \
    .appName("Iniciando com Spark") \
    .getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lendo Csv

# COMMAND ----------

path_orders = '/FileStore/transient/olist/orders'
df_orders = spark.read.format('csv')\
.option("header", True)\
.option("sep", ",")\
.option("quote","\"")\
.option("inferSchema",True)\
.load(path_orders)
#transient\csv\olist

# COMMAND ----------

path_payments = '/FileStore/transient/olist/payments'
df_payments = spark.read.format('csv')\
.option("header", True)\
.option("sep", ",")\
.option("quote","\"")\
.option("inferSchema",True)\
.load(path_payments)

# COMMAND ----------

path_employees = '/FileStore/transient/departments/employees'
df_employees = spark.read.format('csv')\
.option("header", True)\
.option("sep", ",")\
.option("quote","\'")\
.option("inferSchema",True)\
.load(path_employees)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Selecionando e manipulando os dados

# COMMAND ----------

df_orders.select('order_id','customer_id','order_status','order_purchase_timestamp','order_approved_at','order_delivered_carrier_date',\
                'order_delivered_customer_date','order_estimated_delivery_date').display()

# COMMAND ----------

#Vai mostrar o schema da tabela
#schema - esqueleto da tabela, o create table do sql
df_orders.printSchema()

# COMMAND ----------

df_orders.select('order_id','customer_id','order_status','order_purchase_timestamp','order_approved_at','order_delivered_carrier_date',\
                'order_delivered_customer_date','order_estimated_delivery_date').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Colunas

# COMMAND ----------

# MAGIC %md
# MAGIC As colunas são unidades de manipulação de dados do Spark. 
# MAGIC Podemos referencias colunas de algumas formas <br>
# MAGIC * col('nome_coluna') <br>
# MAGIC * dataframe['nome_coluna'] <br>
# MAGIC * dataframe.nome_coluna <br>

# COMMAND ----------

#Usa a função split para dividir a coluna 'order_approved_at' pelos caracteres '-' e, em seguida, obtém o primeiro item do array resultante. Esse valor é renomeado para 'aproved_year_at', 'aproved_month_at', 'aproved_day_at'.

from pyspark.sql.functions import col, round
(
df_orders.select('order_id', 'customer_id', 'order_status', 
F.split(F.col('order_approved_at'), '-').getItem(0).alias('aproved_year_at'),
F.split(df_orders['order_approved_at'], '-').getItem(1).alias('aproved_month_at'),
F.split(df_orders.order_approved_at, '-').getItem(2).alias('aproved_day_at')).display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Criando novas colunas

# COMMAND ----------

#criando uma nova coluna a partir de outras colunas
#tbém tem concat, mas só funciona com string
from pyspark.sql.functions import col, round
#getItem(0) - indice 1, getItem(1) - indice 2, getItem(2) - indice 3
df_orders.withColumn('aproved_year_at', F.split(F.col('order_approved_at'), '-').getItem(0))\
.withColumn('aproved_month_at',F.split(df_orders['order_approved_at'], '-').getItem(1))\
.withColumn('aproved_day_at',F.split(F.split(df_orders.order_approved_at, '-').getItem(2),' ').getItem(0)  )\
.withColumn('country',F.lit('BR') ).display()
#display(): Exibe o DataFrame resultante.


# COMMAND ----------

# MAGIC %md
# MAGIC ### Renomeando colunas

# COMMAND ----------


#Renomear colunas
df_orders_renamed = df_orders.withColumnRenamed("order_id","id_pedido") \
    .withColumnRenamed("customer_id","id_cliente") \
    .withColumnRenamed("order_status","status_pedido") \
    .withColumnRenamed("order_purchase_timestamp","pedido_data_hora") \
    .withColumnRenamed("order_approved_at","aprovado_em") \
    .withColumnRenamed("order_delivered_carrier_date","data_entrega") \
    .withColumnRenamed("order_delivered_customer_date","data_entrega_cliente") \
    .withColumnRenamed("order_estimated_delivery_date","_data_entrega_estimada")


# COMMAND ----------

#chamando a database
df_orders.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Expressões

# COMMAND ----------

from pyspark.sql.functions import expr
# função upper para converter todos os caracteres da coluna 'order_status' para maiúsculas.
(
    df_orders.select('order_id', 'customer_id', 'order_status', 
    F.expr('upper(order_status)'), )                    
    .display()
)

# COMMAND ----------

(
    df_orders.select('order_id', 'customer_id', 'order_status', 
    F.upper('order_status'))                    
    .display()
)

# COMMAND ----------

#substring Cria uma novas colunas
from pyspark.sql.functions import expr
(
    df_orders.select('order_id', 'customer_id', 'order_status', 
    expr('upper(order_status)'),
    expr('substring(order_approved_at, 0,4) as year'),
    expr('substring(order_approved_at, 6,2) as month'),
    expr('substring(order_approved_at, 9,2) as day'))                    
    .display()
)

# COMMAND ----------

cols = ['order_id', 'order_status', 'order_estimated_delivery_date']
df_orders.select(cols).display()

# COMMAND ----------

cols = ['order_id', 'order_status', 'order_estimated_delivery_date']
df_orders.select('customer_id', *cols).display()

# COMMAND ----------

def return_dataframe_selected(df, columns):
    return df.select(colums)

# COMMAND ----------

cols = ['order_ir', 'order_status']
df_teste = return_dataframe_selected(df_orders, cols)


# COMMAND ----------

cols ['order']

# COMMAND ----------

# MAGIC %md
# MAGIC Observações:
# MAGIC * Podemos realizar operações sobre colunas selecionadas. 
# MAGIC * O DataFrame resultante resultante das operações vai obedeçer a order das colunas em que ele foi criado.

# COMMAND ----------

df_orders.display()

# COMMAND ----------

df_orders_selected =  (
    df_orders.select('order_id', 'customer_id', 'order_status', 
    expr('upper(order_status)'),
    expr('substring(order_approved_at, 0,4) as year'),
    expr('substring(order_approved_at, 6,2) as month'),
    expr('substring(order_approved_at, 9,2) as day'))   
)

# COMMAND ----------

df_orders_selected.selectExpr('order_id', 'customer_id','upper(order_status) as order_status','concat(year,"-",month,"-",day) as date').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Selecionando valores únicos

# COMMAND ----------

#.distinct(): Retorna apenas os valores distintos na coluna 'year
df_orders_selected.select('year').distinct().display()

# COMMAND ----------

#comando para agrupar e somar regsitros
df_ano  = df_orders_selected.select('year')
df_ano.groupBy('year').count().display()

# COMMAND ----------

df_orders_selected.dropDuplicates(subset=['year']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filtrando registros e condições

# COMMAND ----------

# MAGIC %md
# MAGIC Operadores lógicos disponíveis:
# MAGIC * e: &
# MAGIC * ou: |
# MAGIC * não: ~
# MAGIC
# MAGIC As funções `filter()` e `where()` podem ser utilizadas no processo de filtragem.

# COMMAND ----------

#O operador ~ representa a negação lógica. Portanto, essa expressão verifica se a coluna 'year' não é igual a 'null'
df_orders_selected.filter(~(col('year') == 'null')).display()

# COMMAND ----------

df_orders_selected.filter((col('year').isNull())).display(5)

# COMMAND ----------

df_orders_selected.select('order_status').distinct().display()

# COMMAND ----------

(
    df_orders_selected.filter((col('year') == '2016') & (col('order_status') == 'invoiced'))
    .display()
)

# COMMAND ----------

#condições para select
(
    df_orders_selected.filter(((col('order_status') == 'unavailable') | (col('order_status') == 'canceled')) & (col('year') == '2017')).display()
  
)

# COMMAND ----------

#condições para select com duas condições
(
    df_orders_selected.filter(((col('order_status') == 'canceled')) & (col('year') == '2018')).display()
  
)

# COMMAND ----------

#Filtra as linhas do DataFrame onde a condição é satisfeita. A condição é composta por duas partes conectadas pelo operador lógico & (AND)
#Verifica se o valor na coluna 'order_status' está presente em uma lista de valores, neste caso, 'unavailable' ou 'canceled'
#
#A função isin é uma função do PySpark que permite verificar se os valores de uma coluna estão contidos em uma lista de valores especificada. Ela é frequentemente usada em conjunto com o método filter para realizar operações de filtragem em DataFrames.

(
    df_orders_selected.filter((col('order_status').isin('unavailable', 'canceled')) & (col('year') == '2017')).display()
)

# COMMAND ----------


(
    df_orders_selected
    .filter((col('order_status').isin('unavailable', 'canceled')))
    .filter((col('year') == '2017'))
    .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Utilizando expressões no filtro

# COMMAND ----------

(
    df_orders_selected
    .filter('order_status in ("unavailable", "canceled") and year == "2017"')
    .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Observações
# MAGIC Quando nos referimos às colunas por meio da função `col()`, temos acesso à diversos métodos das colunas que podem ser utilizados para auxliar na filtragem do DataFrame. Alguns deles são:
# MAGIC * `isin()`: checa se a coluna contém os valores listados na função.
# MAGIC * `contains()`: utilizado para verificar se uma coluna de texto contém algum padrão especificado (não aceita regex). Aceita uma outra coluna de texto.
# MAGIC * `like()`: utilizado para verificar se uma coluna de texto contém algum padrão especificado (não aceita regex). Funciona de forma similar ao "LIKE" do SQL.
# MAGIC * `rlike()`: utilizado para verificar se uma coluna de texto contém algum padrão especificado (**aceita regex**). Funciona de forma similar ao "RLIKE" do SQL.
# MAGIC * `startswith()`: utilizado para verificar se uma coluna de texto começa com algum padrão especificado (**aceita regex**).
# MAGIC * `endswith()`: utilizado para verificar se uma coluna de texto termina com algum padrão especificado (**aceita regex**).
# MAGIC * `between()`: checa se os valores da coluna estão dentro do intervalo especificado. Os dois lados do intervalo são inclusivos.
# MAGIC * `isNull()`: retorna True se o valor da coluna é nulo
# MAGIC * `isNotNull()`: retorna True se o valor da coluna não é nulo
# MAGIC
# MAGIC Outros métodos úteis:
# MAGIC * `alias()/name()`: usado para renomear as colunas em operações como select() e agg()
# MAGIC * `astype()/cast()`: usado para mudar o tipo das colunas. Aceita tanto um string como um tipo especificado pelo módulo pyspark.sql.types
# MAGIC * `substr()`: utilizado para cortar um string com base em índices dos caracteres 

# COMMAND ----------

#TERMINAr....
df_orders_selected('year').alias('ANO')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Funções numéricas
# MAGIC * `round()`: arredonda o valor numérico
# MAGIC * `ceil()`: arredonda o valor numérico para o maior inteiro mais próximo
# MAGIC * `floor()`: arredonda o valor numérico para o menor inteiro mais próximo
# MAGIC * `sqrt()`: retorna a raiz quadrada do valor
# MAGIC * `exp()`: retorna a exponencial do valor
# MAGIC * `log()`: retorna a logaritmo natural do valor
# MAGIC * `log10()`: retorna a logaritmo na base 10 do valor
# MAGIC * `greatest()`: retorna o maior valor dentre os valores das colunas. Análogo ao `max()`, mas entre colunas
# MAGIC * `least()`: retorna o menor valor dentre os valores das colunas. Análogo ao `min()`, mas entre colunas

# COMMAND ----------

df_payments.display()

# COMMAND ----------

df_payments.select(F.round('payment_value',1)).display()

# COMMAND ----------

df_payments.select('*',F.floor('payment_value').alias('payment_value_floor')).display()

# COMMAND ----------

df_payments.select('*',F.ceil('payment_value').alias('payment_value_ceil')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Funções para Texto
# MAGIC * `upper()`: retorna o string em letras maiúsculas
# MAGIC * `lower()`: retorna o string em letras minúsculas
# MAGIC * `initcap()`: retorna a primeira letra de cada palavra no string em letras maiúsculas
# MAGIC * `trim()`: retira os espaços em branco do início e do final do string
# MAGIC * `ltrim() / rtrim()`: retira os espaços em branco do início e do final do string, respectivamente
# MAGIC * `lpad() / rpad()`: acrescenta um caractere no início e no final do string, respectivamente, até que o string tenha um determinado comprimento
# MAGIC * `length()`: retorna o comprimento do string, em quantidade de caracteres
# MAGIC * `split()`: quebra o string a partir de um padrão e retorna um array com os string resultantes
# MAGIC * `concat()`: concatena uma ou mais colunas de string
# MAGIC * `concat_ws()`: concatena uma ou mais colunas de string, com um separador entre elas
# MAGIC * `regexp_extract()`: retorna um match no string a partir de um padrão regex
# MAGIC * `regexp_replace()`: substitui um mtach no strinf a partir de um padrão regex com outros caracteres
# MAGIC * `substring()`: retorna os caracteres do string que estão entre dos indices especificados. Análogo a `f.col().substring()`

# COMMAND ----------

df_employees.display()

# COMMAND ----------

df_employees.select(F.split('email','@')).display()

# COMMAND ----------

#F.concat - Concatena as colunas 'first_name' e 'last_name', separadas por um espaço em branco.

(df_employees.select('*',
                    F.concat('first_name',F.lit(' '),'last_name').alias('full_name'),
                    F.split('email','@').getItem(1).alias('email_company'),
                    F.upper('email').alias('email_upper'),
                    F.length('email').alias('len_email'),
                    F.length(F.trim('email')).alias('len_email_trim')
                    ).display()
)
                    

# COMMAND ----------

# MAGIC %md
# MAGIC #### Funções para data
# MAGIC * `add_months()`: retorna a data depois de adicionar "x" meses
# MAGIC * `months_between()`: retorna a diferença entre duas datas em meses
# MAGIC * `date_add()`: retorna a data depois de adicionar "x" dias
# MAGIC * `date_sub()`: retorna a data depois de subtrair "x" dias
# MAGIC * `next_day()`: retorna o dia seguinte de alguma data
# MAGIC * `datediff()`: retorna a diferença entre duas datas em dias
# MAGIC * `current_date()`: retorna a data atual
# MAGIC * `dayofweek() / dayofmonth() / dayofyear()`: retorna o dia relativo à semana, ao mês e ao ano, respectivamente
# MAGIC * `weekofyear()`: retorna a semana relativa ao ano
# MAGIC * `second() / minute() / hour()`: retorna os segundos, os minutos e as horas de uma coluna de date-time, respectivamente
# MAGIC * `month() / year()`: retorna o mês e o ano de uma coluna de data, respectivamente
# MAGIC * `last_day()`: retorna o último dia do mês do qual a data considerada pertence
# MAGIC * `to_date()`: transforma a coluna no tipo data (t.DateType())
# MAGIC * `trunc()`: formata a data para a unidade especificada
# MAGIC     * `year`: "{ano}-01-01"
# MAGIC     * `month`: "{ano}-{mes}-01"

# COMMAND ----------

df_orders.display()

# COMMAND ----------

#Seleciona três colunas do DataFrame df_orders: 'order_purchase_timestamp', 'order_delivered_customer_date', e adiciona mais três colunas derivadas nas próximas linhas.
#order_purchase_timestamp'. Essa função é útil para calcular uma data futura, neste caso, 3 meses após a data original da compra.
(df_orders.select('order_purchase_timestamp',
                  'order_delivered_customer_date',
                  F.year('order_purchase_timestamp'),
                  F.add_months('order_purchase_timestamp',3),
                  F.datediff('order_delivered_customer_date','order_purchase_timestamp'))
 .display()
 )


# COMMAND ----------

# MAGIC %md
# MAGIC #### Funções para Arrays
# MAGIC * `array()`: constrói um array com as colunas selecionadas
# MAGIC * `flatten()`: transforma um array de arrays em um unico array
# MAGIC * `explode()`: retorna uma nova linha para cada elemento no array 
# MAGIC * `size()`: retorna o número de elementos no array
# MAGIC * `sort_array()`: ordena os elementos do array, de forma crescente ou decrescente
# MAGIC * `reverse()`: reverte a ordem dos elementos de um array
# MAGIC * `array_distinct()`: remove elementos duplicados do array
# MAGIC * `array_contains()`: verifica se o array contém o elemento especificado
# MAGIC * `arrays_overlap()`: partir de 2 colunas de arrays, verifica se elas tem algum elemento em comum, retornando True ou False
# MAGIC * `array_union()`: a partir de 2 colunas de arrays, retorna um array com os elementos unidos das duas colunas, sem duplicatas
# MAGIC * `array_except()`: a partir de 2 colunas de arrays, retorna um array com os elementos que estão em uma coluna mas não estão na outra, sem duplicatas
# MAGIC * `array_intersect()`: a partir de 2 colunas de arrays, retorna um array com os elementos que nas duas colunas, sem duplicatas
# MAGIC * `array_join()`: retorna um string após concatenar os elementos do array usando o delimitador especificado
# MAGIC * `array_max() / array_min()`: retorna o máximo e o mínimo valor do array, respectivamente
# MAGIC * `array_remove()`: remove todos os elementos do array que são iguais ao valor especificado

# COMMAND ----------

data = [
 ("João,,Silva",["Excel","VBA","Contatabilidade"],["Contador","Financeiro"],"MG","SP"),
 ("Rosália,lima,",["Costura","Cozinha","Educação"],["Dona de Casa","Avó"],"PI","SP"),
 ("Severino,,Matos",["CSharp","VB"],["Desenvolvedor","Arquiteto"],"CE","SP")
]

from pyspark.sql.types import StringType, ArrayType,StructType,StructField
schema = StructType([ 
    StructField("Nome",StringType(),True), 
    StructField("Conhecimentos",ArrayType(StringType()),True), 
    StructField("Ocupacao",ArrayType(StringType()),True), 
    StructField("EstadoOrigem", StringType(), True), 
    StructField("EstadoAtual", StringType(), True)
  ])

df = spark.createDataFrame(data=data,schema=schema)

# COMMAND ----------

from pyspark.sql.functions import explode
df.select(df.Nome,explode(df.Conhecimentos)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC * `drop()`: retira do DataFrame as linhas com nulos, com base no que foi passado para o argumento how
# MAGIC     * any (default): retira todas as linhas com pelo menos um valor nulo nas colunas
# MAGIC     * all: somente retira as linhas com todos os valores nulos nas colunas
# MAGIC * `fill()`: preenche os valores nulos no DataFrame com uma constante, passada pelo usuário
# MAGIC * `replace()`: substitui o valor (não somente os valores nulos) por algum outro passado pelo usuário
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import split
df.select(split(col('Nome'),",").alias("ArrayNome")).display()

# COMMAND ----------

from pyspark.sql.functions import array
df.select(df.Nome,array(df.EstadoOrigem,df.EstadoAtual).alias("Estados")).display()

# COMMAND ----------

from pyspark.sql.functions import array_contains
df.select(df.Nome,array_contains(df.Conhecimentos,"Cozinha")
    .alias("array_contains")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Funções para valores nulos
# MAGIC * `drop()`: retira do DataFrame as linhas com nulos, com base no que foi passado para o argumento how
# MAGIC     * any (default): retira todas as linhas com pelo menos um valor nulo nas colunas
# MAGIC     * all: somente retira as linhas com todos os valores nulos nas colunas
# MAGIC * `fillna()`: preenche os valores nulos no DataFrame com uma constante, passada pelo usuário
# MAGIC * `replace()`: substitui o valor (não somente os valores nulos) por algum outro passado pelo usuário

# COMMAND ----------

data = [
    ("João",None,"M"),
    ("Maria","NY","F"),
    ("Valéria",None,None)
  ]

columns = ["nome","estado","sexo"]
df = spark.createDataFrame(data,columns)

# COMMAND ----------

#Validar o valor nulo, ela verifica se o valor é nulo
df.filter("estado is NULL").display()


# COMMAND ----------

df.filter(df.estado.isNull()).display()


# COMMAND ----------

df.filter(col("estado").isNull()).display()

# COMMAND ----------

df_limpo = df.fillna('indefinido')
df_limpo.display()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


