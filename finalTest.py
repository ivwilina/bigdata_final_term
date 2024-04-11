import pyspark
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import  *
spark = SparkSession.builder.appName("test").getOrCreate()
data_raw = spark.read.format('csv').option('header','true').load('D:/Coding Workspace/bigdata_final_term/Spark_Premium_2020_2021.csv')

data = data_raw.select('Div','Date','Time','HomeTeam','AwayTeam','FTHG','FTAG','FTR','HTHG','HTAG','HTR','Referee')

print('### Câu 3.1 ###')
print(f'Có {data.select("HomeTeam").distinct().count()} đội bóng tham gia giải đấu')
print('### Câu 3.2 ###')
print(f'Có {data.select("Div").count()} trận đấu trong mùa giải')

print('### Câu 3.3 ###')
print(f'Có {data.filter(col("FTR")=="D").count()} trận đấu có kết quả hoà')
print(f'Có {data.filter(col("FTR")=="A").count()+data.filter(col("FTR")=="H").count()} trận đấu có kết quả chênh lệch')

print('### Câu 3.4 ###')
latest = data.orderBy(desc('Time')).collect()[0][2] #20:15
matchesLate = data.filter(col('Time')==latest)
print(f'Thời gian diễn ra trận đấu muộn nhất trong ngày là {latest} gồm {matchesLate.count()}')
matchesLate.show(matchesLate.count())

print('### Câu 3.5 ###')
ars_win = data.filter(col('HomeTeam')=='Arsenal').filter(col('FTR')=='H').count() + data.filter(col('AwayTeam')=='Arsenal').filter(col('FTR')=='A').count() 
print(f'Số trận thắng Arsenal dành được trong mùa giải là {ars_win}')

print('### Câu 3.6 ###')
ars_point = ars_win*3 + data.filter(col('HomeTeam')=='Arsenal').filter(col('FTR')=='D').count()*1 \
    + data.filter(col('AwayTeam')=='Arsenal').filter(col('FTR')=='D').count()*1
print(f'Số điểm mà Arsenal dành được là {ars_point}')

print('### Câu 3.7 ###')
def pointCount(x):
    point = data.filter(col('HomeTeam')==x).filter(col('FTR')=='D').count()*1 \
        + data.filter(col('AwayTeam')==x).filter(col('FTR')=='D').count()*1 \
            + data.filter(col('HomeTeam')==x).filter(col('FTR')=='H').count()*3 \
                + data.filter(col('AwayTeam')==x).filter(col('FTR')=='A').count()*3
    return point
team_list = data.select('HomeTeam').distinct()
listFull = []
columns = ['Teams','Point']
for x in range(team_list.count()):
    name = team_list.collect()[x][0]
    point = pointCount(name)
    tempList = [name,point]
    listFull.append(tempList)
df = spark.createDataFrame(listFull,columns)
print(f'Đội bóng vô địch mùa giải là {df.orderBy(desc("Point")).collect()[0][0]} với {df.orderBy(desc("Point")).collect()[0][1]} điểm')

# # testing
# temp_raw = data.filter(col('HomeTeam')=='Arsenal')
# temp_raw.show(temp_raw.count())
# temp = temp_raw.groupBy('FTR').agg(count('*').alias('Count'))
# temp2 = temp.withColumn('Point',when(col('FTR')=='D',col('Count')*1).when(col('FTR')=='A',col('Count')*0).when(col('FTR')=='H',col('Count')*3))
# # temp2 = temp.withColumn('Point',temp['Count']*2)
# temp2.show(temp2.count())

print('### Câu 3.8 ###')
filter1 = data.groupBy('HomeTeam').agg(sum('FTHG').alias('GAHT'))
filter2 = data.groupBy('AwayTeam').agg(sum('FTAG').alias('GAAT'))
filter3 = filter1.join(filter2, filter1['HomeTeam']==filter2['AwayTeam'])
filter4 = filter3.withColumn('Goals',col('GAHT')+col('GAAT')).drop('AwayTeam','GAHT','GAAT').withColumnRenamed('HomeTeam','Team')
final = df.join(filter4, df['Teams']==filter4['Team']).drop('Team')
final.orderBy(desc('Point'),desc('Goals')).show(final.count())

print('### Câu 3.9 ###')
data.withColumn('3.9',col('HomeTeam')[0:3]).show()

print('### Câu 3.10 ###')
data.withColumn('3.10',when(col('Time').between("00:00","12:00"),'Time 1') \
                .when(col('Time').between("12:00","18:00"),'Time 2') \
                    .when(col('Time').between("18:00","24:00"),'Time 3')).show()