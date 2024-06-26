import os
import subprocess
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, date_format, month, row_number, count, split, explode
from pyspark.sql.window import Window

def upload_to_hdfs(local_dir, hdfs_dir):
    """
    Função para fazer upload dos arquivos Parquet para o HDFS.
    """
    # Comando para copiar arquivos para o HDFS
    command = f"hdfs dfs -put {local_dir}/parquets/*.parquet {hdfs_dir}"
    try:
        subprocess.run(command, shell=True, check=True)
        print(f"Arquivos carregados com sucesso para {hdfs_dir}")
    except subprocess.CalledProcessError as e:
        print(f"Erro ao carregar arquivos para o HDFS: {e}")
    command =  f"hdfs dfs -put {local_dir}/taxi_zone_lookup.csv {hdfs_dir}"
    try:
        subprocess.run(command, shell=True, check=True)
        print(f"Arquivo carregado com sucesso para {hdfs_dir}")
    except subprocess.CalledProcessError as e:
        print(f"Erro ao carregar arquivo para o HDFS: {e}")

def init_spark():
    SparkContext("local", "Trabalho Final")
    return SparkSession.builder.getOrCreate()

def load_parquets(spark):

    dataFramesPorMes = []
    
    for month in range(1, 13):

        file_path = f"{hdfs_dir}/yellow_tripdata_2022-{month:02d}.parquet"
        
        dataframe = spark.read.parquet(file_path)
        
        dataframe = dataframe.withColumn("date_id", lit(month))
        
        dataFramesPorMes.append(dataframe)

    dataframe = dataFramesPorMes[0]
    for df in dataFramesPorMes[1:]:
        dataframe = dataframe.union(df)

    return dataframe

def remove_unused_col(dataframe):
    return dataframe.drop( 'RatecodeID', 'Store_and_fwd_flag', 'Payment_type', 'Fare_amount',\
                           'Extra', 'MTA_tax', 'Improvement_surcharge', 'Tip_amount', \
                            'Total_amount', 'Congestion_Surcharge', 'airport_fee', \
                            'Passenger_count', 'Trip_distance', 'tolls_amount')

def load_zones(spark, dataframe):
    zones = spark.read.csv("/user/paulo/taxi_zone_lookup.csv", header=True, inferSchema=True)
    zones = zones.withColumnRenamed('LocationID', 'DOLocationID')
    dataframe_with_zones =  dataframe.join(zones, on='DOLocationID', how='inner')
    return dataframe_with_zones

def load_zones_adjacencies(spark):
    grafo_path = '/user/paulo/grafo.txt'
    

    dataframe = spark.read.text(grafo_path) \
                    .withColumn("value", split("value", " ")) \
                    .selectExpr("value[0] as DOLocationID", "value[1] as AdjacencieID")
    
    return dataframe

def filtrar_periodo(dataframe):
    inicio_periodo = "2022-01-01"
    fim_periodo = "2022-12-31"
    
    dataframe_filtrado = dataframe.filter(
        col('tpep_dropoff_datetime').cast('date').between(inicio_periodo, fim_periodo)
    )
    
    return dataframe_filtrado

def etl_date(dataframe):

    dataframe = (dataframe
    .withColumn('date', date_format('tpep_pickup_datetime', 'MMMM'))
    .withColumn('month', month('tpep_pickup_datetime')))
    
    return dataframe

def top_locations(dataframe):
    result = dataframe.groupBy('month', 'VendorID', 'Zone', 'date').agg(count('*').alias('qtd'))

    window_spec = Window.partitionBy('month', 'VendorID').orderBy(col('qtd').desc())
    
    result = result.withColumn('rank', row_number().over(window_spec))
    
    result = result.drop('qtd')
    
    result = result.filter(col('rank') <= 5)
    
    result = result.orderBy(col('VendorID').asc(), col('month').asc(), col('rank').asc())

    result = result.drop('month')

    result = result.select('VendorID', 'date', 'Zone', 'rank')

    print(result.count())
    result.show()


def main(local_dir, hdfs_dir):
    
    #upload_to_hdfs(local_dir, hdfs_dir)
    
    spark = init_spark()

    dataframe = load_parquets(spark)

    dataframe = remove_unused_col(dataframe)
    
    dataframe = load_zones(spark, dataframe)

    dataframe_adjacencies = load_zones_adjacencies(spark)

    dataframe.createOrReplaceTempView("CorridaTaxi")
    
    #Filtrar para o ano de 2022
    dataframe = filtrar_periodo(dataframe)
    
    dataframe = etl_date(dataframe)
    
    top_locations(dataframe)

    dataframeSelect = dataframe.select("tpep_pickup_datetime","tpep_dropoff_datetime", "VendorID", "date", "month", "DOLocationID")
    


if __name__ == "__main__":
    
    local_dir = "/home/paulo/Documentos/BancoDeDados/trabalho-final"
    
    hdfs_dir = "/user/paulo"
    
    main(local_dir, hdfs_dir)
