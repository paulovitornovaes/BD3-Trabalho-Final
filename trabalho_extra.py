from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, lower, array, udf, length, concat_ws, size
from pyspark.sql.types import ArrayType, StringType
from pyspark.ml.feature import StopWordsRemover
import nltk
from nltk.corpus import wordnet
from nltk.stem import WordNetLemmatizer
from pyspark import SparkContext, SparkConf


def init_spark():
    SparkContext("local", "PySpark Word Count")
    return SparkSession.builder.getOrCreate()

def ler_texto(spark, file):
    linhas = spark.read.text(file)
    palavras = linhas.select(explode(split(lower(col("value")), r"\W+")).alias("palavra"))
    palavras = palavras.filter(col("palavra") != "")
    palavras = palavras.filter(length(col("palavra")) > 1)
    return palavras

def filtrar_stopwords(palavras):
    palavras_array = palavras.select(array("palavra").alias("palavras"))
    remover = StopWordsRemover(inputCol="palavras", outputCol="palavras_filtradas")
    palavras_filtradas = remover.transform(palavras_array)
    palavras_filtradas = palavras_filtradas.select("palavras_filtradas")
    palavras_filtradas = palavras_filtradas.withColumnRenamed("palavras_filtradas", "palavras")
    palavras_filtradas = palavras_filtradas.filter(size(col("palavras")) > 0)
    return palavras_filtradas

def lematizar_palavra(palavra, lematizador):
        if nltk.pos_tag([palavra])[0][1].startswith('V'):
            return lematizador.lemmatize(palavra, pos='v')
        else:
            return palavra

def filtrar_verbos_infinitivo(palavras):

    lematizador = WordNetLemmatizer()

    lematizador_udf = udf(lambda palavras: [lematizar_palavra(palavra, lematizador) for palavra in palavras], ArrayType(StringType()))

    palavras_infinitivo = palavras.withColumn("palavra", lematizador_udf(col("palavras")))
    
    palavras_infinitivo_str = palavras_infinitivo.withColumn("palavras_filtradas", concat_ws(" ", col("palavra")))

    palavras_infinitivo_str = palavras_infinitivo_str.select("palavras_filtradas").withColumnRenamed("palavras_filtradas", "palavras")
    
    return palavras_infinitivo_str

def ordenar_por_frequencia(palavras):
    return palavras.groupBy("palavras").count().orderBy(col("count").desc())

def main():

    spark = init_spark()

    palavras = ler_texto(spark, "Shakespeare_alllines.txt")

    palavras = filtrar_stopwords(palavras)
    
    palavras = filtrar_verbos_infinitivo(palavras)

    palavras = ordenar_por_frequencia(palavras)

    palavras.show()


if __name__ == "__main__":
    main()
