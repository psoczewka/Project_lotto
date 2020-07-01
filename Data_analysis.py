import numpy as np
import pandas as pd
import pymysql
from sqlalchemy import create_engine
import matplotlib.pyplot as plt

engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                               .format(user="final_project",
                                       pw="qwerty",
                                       db="project_lotto"))
lottery_data = pd.read_sql_table("lottery_data", engine, index_col='prize_id')
polish_cities = pd.read_sql_table("polish_cities", engine, index_col='index')
polish_provinces = pd.read_sql_table("polish_provinces", engine, index_col='index')

#Quick look if there are any missing values
lottery_data.info()
polish_cities.info()
polish_provinces.info()

#adjusting index of lottery_data DF, so it starts from 0
lottery_data.set_index(lottery_data.index - 1, inplace=True)

#prepparing series with yearly statistics
#lottery_year_count show number of wins per year
#lottery_year_sum showing sum of prize money per year
lottery_data['year']=lottery_data['lottery_date'].dt.year
lottery_data.sort_values('year', inplace=True)
lottery_year_count = lottery_data.groupby('year')['lottery_prize'].count()
lottery_year_sum = lottery_data.groupby('year')['lottery_prize'].sum()

#preparing a plot showing changes in number of wins and sum of prize money per year from 1996 to 2020
fig, ax1 = plt.subplots(figsize=(12,5))
plt.title('Lottery yearly trends from 1996 to 2020')
plt.xticks([1996, 2000, 2005, 2010, 2015, 2020])
plt.xlabel('Year')

ax1.plot(lottery_year_count.index, lottery_year_count,color="b")
ax1.tick_params(axis='y', labelcolor="b")
ax1.legend(['Number of wins'], loc=2, )
plt.ylabel('Number of wins', color='b')

ax2 = ax1.twinx()

ax2.plot(lottery_year_sum.index, lottery_year_sum/1000000, color="r")
ax2.tick_params(axis='y', labelcolor="r")
ax2.legend(["Total money won"], loc=1)
ax2.ticklabel_format(axis='y', style='plain')
plt.ylabel('Total money won [mln]', color='r')

plt.show()

#boxplot showing prize money distributions in years from 1996 to 2020
lottery_data.boxplot(column = 'lottery_prize', by = 'year', figsize=(15,8), showmeans=True, rot=45)

#prepparing monthly data analogously to yearly data
#this time using pivot table - multiple indexing on month and month_number allows to sort months in yearly order
lottery_data['month']=lottery_data['lottery_date'].dt.month_name()
lottery_data['month_number'] = lottery_data['lottery_date'].dt.month
lottery_data.sort_values('month_number', inplace=True)
lottery_data_sum = lottery_data.pivot_table(index=['month', 'month_number'], values='lottery_prize', aggfunc=sum).sort_index(level=1)
lottery_data_count = lottery_data.pivot_table(index=['month', 'month_number'], values='lottery_prize', aggfunc='count').sort_index(level=1)
lottery_data

#preparing a plot showing changes in number of wins and sum of prize money per month
fig, ax1 = plt.subplots(figsize=(12,5))
plt.title('Lottery monthly numbers')

ax1.plot(lottery_data_count.index.get_level_values(0), lottery_data_count['lottery_prize'],color="b")
ax1.tick_params(axis='y', labelcolor="b")
ax1.tick_params(axis='x', labelrotation =45)
ax1.legend(['Number of wins'], loc=2, )
plt.ylabel('Number of wins', color='b')
plt.yticks([60,80,100,120,140,160])

ax2 = ax1.twinx()

ax2.plot(lottery_data_sum.index.get_level_values(0), lottery_data_sum['lottery_prize']/1000000, color="r")
ax2.tick_params(axis='y', labelcolor="r")
ax2.legend(["Total money won"], loc=1)
plt.ylabel('Total money won [mln]', color='r')
plt.yticks([200,400,600,800])

plt.show()

#boxplot showing prize money distributions in montly manner
lottery_data.boxplot(column = 'lottery_prize', by = 'month_number', figsize=(12,8), showmeans=True, rot=45)

#merging lottery_dara and polish_cities data to link lottery location with its province
lottery_city = pd.merge(lottery_data, polish_cities, left_on='lottery_location', right_on='City', how='inner')
#checking for NaNs
lottery_city.info()
lottery_data.info()

#counting % of removed rows and % of lottery prize loss
removed_rows_count = (lottery_data['lottery_prize'].count() - lottery_city['lottery_prize'].count())/lottery_data['lottery_prize'].count() * 100
removed_rows_sum = (lottery_data['lottery_prize'].sum() - lottery_city['lottery_prize'].sum())/lottery_data['lottery_prize'].sum() * 100
removed_rows_count = str(round(removed_rows_count, 2))
removed_rows_sum = str(round(removed_rows_sum, 2))
print('Percent of removed rows from lottery_data: '+removed_rows_count+'%')
print('Percent of prize money sum from lottery_data in removed rows: '+removed_rows_sum+'%')

polish_provinces.set_index('Province', inplace=True)
#merging lottery_city and polish_province
lottery_prov = pd.merge(lottery_city, polish_provinces, on=['Province'], how='inner', suffixes=['_city', '_province'],)
prov_sum = lottery_prov.groupby('Province')['lottery_prize'].sum().sort_index()
prov_count = lottery_prov.groupby('Province')['lottery_prize'].count().sort_index()

#barplot showing average money prize per win in provinces
prov_sum.divide(prov_count).sort_values(ascending=False).plot(kind='bar', figsize=(15,5), rot=45, title='Average money prize per win in provinces')

prov_pop = polish_provinces['Population'].sort_index()

#barplot showing amount of money won per province citizen
prov_sum.divide(prov_pop).sort_values(ascending=False).plot(kind='bar', figsize=(15,5), rot=45, title='Amount of money won per province citizen')

#barplot showing number of wins per 1 mln citizenss of a province
(prov_count.divide(prov_pop).sort_values(ascending=False) * 1000000).plot(kind='bar', figsize=(15,5), rot=45, title='Number of wins per 1 mln citizens of a province')

#boxplot showing prize money distributions in polish provinces
lottery_prov.boxplot(column = 'lottery_prize', by = 'Province', figsize=(22,8), showmeans=True, rot=45)
