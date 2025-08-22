import pandas as pd
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, lit, when, col

spark = SparkSession.builder.appName("BoardGamesApp").getOrCreate()

jdbc_options = {
    "url": spark.conf.get("spark.mysql.penguins.url"),
    "user": spark.conf.get("spark.mysql.penguins.user"),
    "password": spark.conf.get("spark.mysql.penguins.password"),
    "driver": "com.mysql.cj.jdbc.Driver",
}

bgg_df = spark.read.csv("../input/bgg_ranks.csv", header=True, inferSchema=True)
mmbg_df = spark.read_csv("../input/melissa-monfared-board-games.csv", header=True, inferSchema=True, encoding="utf-8") # change encoding if needed


bgg_df.show()
mmbg_df.show()

spark.stop()