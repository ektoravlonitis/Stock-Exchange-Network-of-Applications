from pyspark.sql import SparkSession
from pyspark.sql.functions import max as SparkMax
from pyspark.sql.functions import year as SparkYear
from pyspark.sql.functions import avg as SparkAVG
from pyspark.sql.functions import stddev as SparkSTD
from pyspark.sql.functions import month as SparkMonth
from pyspark.sql.functions import min as SparkMin

spark = SparkSession.builder.master("local[1]") \
    .config("spark.jars", "/usr/share/java/mysql-connector-java-8.0.28.jar") \
    .appName("6107app").getOrCreate()


def checkChoices(inv, port):
    if inv == 1:
        inv = 'Inv1'
        port = 1

    elif inv == 2:
        inv = 'Inv2'
        port = 2
    elif inv == 3:
        inv = 'Inv3'
        port = 3
    return inv, port


def startApp(inv, port=None):
    inv, port = checkChoices(inv, port)

    df = spark.read.format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/InvestorsDB") \
        .option("driver", "com.mysql.jdbc.Driver") \
        .option("dbtable", f"{inv}_P{port}1") \
        .option("user", "itc6107") \
        .option("password", "itc6107") \
        .load()
    df2 = spark.read.format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/InvestorsDB") \
        .option("driver", "com.mysql.jdbc.Driver") \
        .option("dbtable", f"{inv}_P{port}2") \
        .option("user", "itc6107") \
        .option("password", "itc6107") \
        .load()

    print("Process Started")

    # print('Max and Min of Daily Change and Percentage Change')

    # print('Portfolio 1')
    maxAndMinChange1 = df.agg(SparkMax('Difference'), SparkMax('Percentage'),
                              SparkMin('Difference'), SparkMin('Percentage'))

    maxAndMinChange1 = maxAndMinChange1.toPandas().to_string(index=False)

    # print('Portfolio 2')
    maxAndMinChange2 = df2.agg(SparkMax('Difference'), SparkMax('Percentage'),
                               SparkMin('Difference'), SparkMin('Percentage'))

    maxAndMinChange2 = maxAndMinChange2.toPandas().to_string(index=False)

    # print('Max and Min of Daily Change and Percentage Change per Year')

    df = df.withColumn('YEAR', SparkYear(df['Timestamp']))
    df2 = df2.withColumn('YEAR', SparkYear(df2['Timestamp']))

    # print('Portfolio 1')
    groupedYears1 = df.groupBy('YEAR') \
        .agg({'Difference': 'max', 'Percentage': 'max'})

    groupedYears1 = groupedYears1.toPandas().to_string(index=False)

    # print('Portfolio 2')
    groupedYears2 = df2.groupBy('YEAR') \
        .agg({'Difference': 'max', 'Percentage': 'max'})

    groupedYears2 = groupedYears2.toPandas().to_string(index=False)

    # print('Avg and Std of Evaluation')

    # print('Portfolio 1')
    maxEval1 = df.agg(SparkAVG('Evaluation'), SparkSTD('Evaluation'))

    maxEval1 = maxEval1.toPandas().to_string(index=False)

    # print('Portfolio 2')
    maxEval2 = df2.agg(SparkAVG('Evaluation'), SparkSTD('Evaluation'))

    maxEval2 = maxEval2.toPandas().to_string(index=False)

    start_year = 2000
    end_year = 2001
    df = df.withColumn('YEAR', SparkYear(df['Timestamp']))
    df2 = df2.withColumn('YEAR', SparkYear(df2['Timestamp']))

    # print(f'Avg and Std of Evaluation in period {start_year}-{end_year}')

    # print('Portfolio 1')
    stdAvgPeriod1 = df.filter((str(start_year) <= df['YEAR']) & (df['YEAR'] <= str(end_year))) \
        .agg(SparkAVG('Evaluation'), SparkSTD('Evaluation'))

    stdAvgPeriod1 = stdAvgPeriod1.toPandas().to_string(index=False)

    # print('Portfolio 2')
    stdAvgPeriod2 = df2.filter((str(start_year) <= df2['YEAR']) & (df2['YEAR'] <= str(end_year))) \
        .agg(SparkAVG('Evaluation'), SparkSTD('Evaluation'))

    stdAvgPeriod2 = stdAvgPeriod2.toPandas().to_string(index=False)

    # print('Avg Evaluation per month')
    df = df.withColumn('MONTH', SparkMonth(df['Timestamp']))
    df2 = df2.withColumn('MONTH', SparkMonth(df2['Timestamp']))

    # print('Portfolio 1')
    avgEvalMonth1 = df.groupBy(df['YEAR'], df['MONTH']).agg(SparkAVG('Evaluation')) \
        .orderBy('YEAR', 'MONTH', ascending=True)

    avgEvalMonth1 = avgEvalMonth1.toPandas().to_string(index=False)

    # print('Portfolio 2')
    avgEvalMonth2 = df2.groupBy(df2['YEAR'], df2['MONTH']).agg(SparkAVG('Evaluation')) \
        .orderBy('YEAR', 'MONTH', ascending=True)

    avgEvalMonth2 = avgEvalMonth2.toPandas().to_string(index=False)

    with open(f'{inv}_1_stats', 'w') as file:
        file.write("Maximum and Minimum Changes")
        file.write(f"\n{maxAndMinChange1}\n")
        file.write(f"\nPer Year")
        file.write(f"\n{groupedYears1}\n")
        file.write(f"\nAverage evaluation and the standard deviation of evaluation")
        file.write(f"\n{maxEval1}\n")
        file.write(f"\nAverage evaluation and the standard deviation of evaluation in period {start_year}-{end_year}")
        file.write(f"\n{stdAvgPeriod1}\n")
        file.write(f"\nAverage evaluation per month")
        file.write(f"\n{avgEvalMonth1}\n")
        file.close()

    with open(f'{inv}_2_stats', 'w') as file:
        file.write("Maximum and Minimum Changes")
        file.write(f"\n{maxAndMinChange2}\n")
        file.write(f"\nPer Year")
        file.write(f"\n{groupedYears2}\n")
        file.write(f"\nAverage evaluation and the standard deviation of evaluation")
        file.write(f"\n{maxEval2}\n")
        file.write(f"\nAverage evaluation and the standard deviation of evaluation in period {start_year}-{end_year}")
        file.write(f"\n{stdAvgPeriod2}\n")
        file.write(f"\nAverage evaluation per month")
        file.write(f"\n{avgEvalMonth2}\n")
        file.close()

    return print("Process Finished")


startApp(1)
