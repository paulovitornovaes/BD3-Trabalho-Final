import os
import subprocess
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

def upload_to_hdfs(local_dir, hdfs_dir):
    """
    Função para fazer upload dos arquivos Parquet para o HDFS.
    """
    # Comando para copiar arquivos para o HDFS
    command = f"hdfs dfs -put {local_dir}/*.parquet {hdfs_dir}"
    try:
        subprocess.run(command, shell=True, check=True)
        print(f"Arquivos carregados com sucesso para {hdfs_dir}")
    except subprocess.CalledProcessError as e:
        print(f"Erro ao carregar arquivos para o HDFS: {e}")

def init_spark():
    SparkContext("local", "Trabalho Final")
    return SparkSession.builder.getOrCreate()

def load_parquets(spark):

    dataFramesPorMes = []
    
    for month in range(1, 13):

        file_path = f"{hdfs_dir}/yellow_tripdata_2022-{month:02d}.parquet"
        
        dataFrame = spark.read.parquet(file_path)
        
        dataFrame = dataFrame.withColumn("date_id", lit(month))
        
        dataFramesPorMes.append(dataFrame)

    dataFrame = dataFramesPorMes[0]
    for df in dataFramesPorMes[1:]:
        dataFrame = dataFrame.union(df)

    return dataFrame


def main(local_dir, hdfs_dir):
    
    #upload_to_hdfs(local_dir, hdfs_dir)
    
    spark = init_spark()

    dataFrame = load_parquets(spark)

    dataFrame.createOrReplaceTempView("CorridaTaxi")

   
    temp = spark.sql('SELECT * FROM CorridaTaxi')

    temp.show()


if __name__ == "__main__":
    
    local_dir = "/home/paulo/Documentos/BancoDeDados/trabalho-final/parquets"
    
    hdfs_dir = "/user/paulo"
    
    main(local_dir, hdfs_dir)