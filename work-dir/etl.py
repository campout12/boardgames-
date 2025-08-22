from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, lit, when, col, split , explode, trim, regexp_replace, length
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, DoubleType, IntegerType, FloatType, LongType, DecimalType



spark = SparkSession.builder.appName("BoardGamesApp").getOrCreate()

jdbc_options = {
    "url": spark.conf.get("spark.mysql.board_games.url"),
    "user": spark.conf.get("spark.mysql.board_games.user"),
    "password": spark.conf.get("spark.mysql.board_games.password"),
    "driver": "com.mysql.cj.jdbc.Driver",
}

bgg_df = spark.read.csv(
    "/opt/spark/files/in/bgg_ranks.csv", 
    header=True, 
    inferSchema=True,
    # quote='"',
    # escape='"', 
    # multiLine=False,
    # encoding="utf-8"
)
# bgg_df = bgg_df.withColumn("yearpublished", regexp_replace(col("yearpublished"), '"', ''))
bgg_df = bgg_df.filter(length(col("yearpublished").cast("string")) <= 4)

mmbg_df = spark.read.csv(
    "/opt/spark/files/in/melissa-monfared-board-games.csv", 
    header=True, 
    inferSchema=True, 
    encoding="cp1252"
) # change encoding if needed


# drop name column from mmbg
mmbg_df =mmbg_df.drop("name")

# merge dataframes
joined_df = bgg_df.join(mmbg_df, on="id")

joined_df =joined_df.drop("id")

# rename columns
joined_df= joined_df.withColumnsRenamed({
    "name": "game_name",
    "Year Published": "year_published",
    "Min Players": "min_players",
    "Max Players": "max_players",
    "Rating Average": "avg_rating",
    "rank": "ranking",
    "Domains": "domain_name",
    "Mechanics": "mechanics_name"
})

window = Window.orderBy(lit(1))

joined_df = joined_df.withColumn("board_game_id", row_number().over(window))

#master dataframe with all columns, not normalized
full_df = joined_df.select(
"board_game_id",
"game_name", 
"year_published", 
"min_players", 
"max_players", 
"avg_rating", 
"ranking", 
"domain_name",
"mechanics_name")

#data cleaning
full_df = full_df.dropDuplicates().dropna(subset=["game_name", "min_players", "year_published", "max_players", "avg_rating"])

#print("Full data frame not normalized")
# full_df.show()


def make_table_and_bridge(df, column_name):
    table = df.withColumn(
        column_name, explode(split(col(column_name), ","))
    ).withColumn( column_name, trim(col(column_name)))

    unique_table = table.select(col(column_name)).distinct()
    window = Window.orderBy(lit(1)) # generate unique IDs for each unique name
    unique_table = unique_table.withColumn(f"{column_name}_id", row_number().over(window))

    bridge_table = table.join(
        unique_table, on=column_name
    ).select(
        "board_game_id", f"{column_name}_id"
    ).dropDuplicates()
    
    return unique_table, bridge_table

domain_df, board_game_domains_df = make_table_and_bridge(full_df, "domain_name")
mechanics_df, board_game_mechanics_df = make_table_and_bridge(full_df, "mechanics_name")
# domain_df.show()
# mechanics_df.show()
# board_game_domains_df.show()
# board_game_mechanics_df.show()


#BOARD GAMES TABLE
board_games_df = full_df.drop("domain_name", "mechanics_name")
#print("final board games table")
#board_games_df.show()

numeric_types = (DoubleType, IntegerType, FloatType, LongType, DecimalType)

for c, t in board_games_df.dtypes:
    spark_type = board_games_df.schema[c].dataType
    if isinstance(spark_type, numeric_types) or spark_type == StringType():
        # Cast string numeric columns to double if necessary
        board_games_df = board_games_df.withColumn(
            c,
            when(col(c) == "NA", None).otherwise(col(c).cast(spark_type))
        )


print("attempting sql inserts. . .")

board_games_df.write.format("jdbc").options(**jdbc_options,dbtable="board_game").mode("append").save()
print("board game insert successful")

#DOMAIN TABLE
domain_df.write.format("jdbc").options(**jdbc_options,dbtable="domain").mode("append").save()
print("domain insert successful")

#MECHANICS TABLE
mechanics_df.write.format("jdbc").options(**jdbc_options,dbtable="mechanics").mode("append").save()
print("mechanics insert successful")

#BOARD GAME DOMAINS BRIDGE TABLE
board_game_domains_df.write.format("jdbc").options(**jdbc_options,dbtable="board_game_domains").mode("append").save()
print("board game domains insert successful")

#BOARD GAME MECHANICS BRIDGE TABLE
board_game_mechanics_df.write.format("jdbc").options(**jdbc_options,dbtable="board_games_mechanics").mode("append").save()
print("board game mechanics insert successful")

spark.stop()