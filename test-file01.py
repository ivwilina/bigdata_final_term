import pyspark
import pandas
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, year, sum, lit, when, min, avg, max, col, split, explode, count, lower
spark = SparkSession.builder.appName("test").getOrCreate()
data_raw = spark.read.format('csv').option('header','true').load('D:\BTL-HK2\Big-Data\Đề tài 2\Spark_Premium_2020_2021.csv')

data = data_raw.select('Date','Time','HomeTeam','AwayTeam','FTHG','FTAG','FTR','HTHG','HTAG','HTR','Referee')


def pointsCal(temp):
    temp_as_hometeam = data.filter(data['HomeTeam']==temp)
    temp_as_awayteam = data.filter(data['AwayTeam']==temp)
    temp_hometeam_win_count = temp_as_hometeam.filter(temp_as_hometeam['FTHG']>temp_as_hometeam['FTAG']).count()
    temp_awayteam_win_count = temp_as_awayteam.filter(temp_as_awayteam['FTHG']<temp_as_awayteam['FTAG']).count()
    temp_win = temp_hometeam_win_count + temp_awayteam_win_count
    temp_hometeam_tie_count = temp_as_hometeam.filter(temp_as_hometeam['FTHG']==temp_as_hometeam['FTAG']).count()
    temp_awayteam_tie_count = temp_as_awayteam.filter(temp_as_awayteam['FTHG']==temp_as_awayteam['FTAG']).count()
    temp_tie = temp_hometeam_tie_count + temp_awayteam_tie_count
    temp_point = temp_win*3 + temp_tie
    return temp_point

def winsCount(temp):
    temp_as_hometeam = data.filter(data['HomeTeam']==temp)
    temp_as_awayteam = data.filter(data['AwayTeam']==temp)
    temp_hometeam_win_count = temp_as_hometeam.filter(temp_as_hometeam['FTHG']>temp_as_hometeam['FTAG']).count()
    temp_awayteam_win_count = temp_as_awayteam.filter(temp_as_awayteam['FTHG']<temp_as_awayteam['FTAG']).count()
    temp_win = temp_hometeam_win_count + temp_awayteam_win_count
    return temp_win

def goalsCount(temp):
    temp_as_hometeam = data.filter(data['HomeTeam']==temp)
    temp_as_awayteam = data.filter(data['AwayTeam']==temp)
    temp_hometeam_goals = temp_as_hometeam.agg({"FTHG":"sum"}).collect()[0][0]
    temp_awayteam_goals = temp_as_awayteam.agg({"FTAG":"sum"}).collect()[0][0]
    temp_goals = temp_hometeam_goals + temp_awayteam_goals
    return temp_goals

print('Câu 3.1')
team_count = data.select('HomeTeam').distinct().count()
print(f'Số đội bóng tham gia: {team_count}')

print('Câu 3.2')
match_count = data.count()
print(f'Tổng số trận đấu: {match_count}')

print('Câu 3.3')
tie_match_count = data.filter(data['FTHG']==data['FTAG']).count()
non_tie_match_count = data.filter(data['FTHG']!=data['FTAG']).count()
print(f'Số trận có tỉ số hòa nhau: {tie_match_count}')
print(f'Số trận có tỉ số lệch nhau: {non_tie_match_count}')

print('Câu 3.4') #cần rút gọn 
match_get = data.select('Date','Time','HomeTeam','AwayTeam')
match_time_filter_lv1 = match_get.sort(desc('Time'))
time_match = match_time_filter_lv1.collect()[0][1]
match_time_filter_lv2 = match_time_filter_lv1.filter(match_time_filter_lv1['Time']==time_match)
print(f'Thời gian diễn ra trận đấu muộn nhất trong ngày là {time_match} bao gồm các trận đấu: ')
match_time_filter_lv2.show(match_time_filter_lv2.count())

print('Câu 3.5')
Arsenal_as_hometeam = data.filter(data['HomeTeam']=='Arsenal')
Arsenal_as_awayteam = data.filter(data['AwayTeam']=='Arsenal')
Ars_hometeam_win_count = Arsenal_as_hometeam.filter(Arsenal_as_hometeam['FTHG']>Arsenal_as_hometeam['FTAG']).count()
Ars_awayteam_win_count = Arsenal_as_awayteam.filter(Arsenal_as_awayteam['FTHG']<Arsenal_as_awayteam['FTAG']).count()
Ars_win = Ars_hometeam_win_count + Ars_awayteam_win_count
print(f'Số trận thắng của Arsenal: {Ars_win}')

print('Câu 3.6')
Ars_hometeam_tie_count = Arsenal_as_hometeam.filter(Arsenal_as_hometeam['FTHG']==Arsenal_as_hometeam['FTAG']).count()
Ars_awayteam_tie_count = Arsenal_as_awayteam.filter(Arsenal_as_awayteam['FTHG']==Arsenal_as_awayteam['FTAG']).count()
Ars_tie = Ars_hometeam_tie_count + Ars_awayteam_tie_count
Ars_point = Ars_win*3 + Ars_tie
print(f'Số điểm mà Arsenal dành được: {Ars_point}')

print('Câu 3.7')
all_team1 = data.select('HomeTeam').distinct()
all_team_pandas = all_team1.withColumn('Points',lit(None)).toPandas()
for x in range(all_team1.count()):
    name = all_team1.collect()[x][0]
    getPoint = pointsCal(name)
    all_team_pandas.iloc[x,1]=getPoint
all_team_pyspark = spark.createDataFrame(all_team_pandas)
all_team_pyspark_ordered = all_team_pyspark.orderBy(desc('Points'))
print(f"Đội bóng vô địch mùa giải là {all_team_pyspark_ordered.collect()[0][0]} với {all_team_pyspark_ordered.collect()[0][1]} điểm")

print('Câu 3.8')
all_team_wins_pandas = all_team1.withColumn('Wins',lit(None)).toPandas()
for x in range(all_team1.count()):
    name = all_team1.collect()[x][0]
    getWins = winsCount(name)
    all_team_wins_pandas.iloc[x,1]=getWins
all_team_wins_pyspark = spark.createDataFrame(all_team_wins_pandas)
all_team_wins_pyspark_ordered = all_team_wins_pyspark.orderBy(desc('Wins'))
all_team_wins_pyspark_ordered.show()

print('Câu 3.9')
all_team_goals_pandas = all_team1.withColumn('Goals',lit(None)).toPandas()
for x in range(all_team1.count()):
    name = all_team1.collect()[x][0]
    getGoals = goalsCount(name)
    all_team_goals_pandas.iloc[x,1]=getGoals
all_team_goals_pyspark = spark.createDataFrame(all_team_goals_pandas)
all_team_goals_pyspark_ordered = all_team_goals_pyspark.orderBy(desc('Goals'))
all_team_goals_pyspark_ordered.show()

print('Câu 3.10')

