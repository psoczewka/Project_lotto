import requests
from bs4 import BeautifulSoup
import pandas as pd
import lxml
import datetime
import pymysql
from sqlalchemy import create_engine

class Data_gathering:
    def __init__(self):
        #defining url-s with data sources
        self.url_lotto = 'http://megalotto.pl/najwyzsze-wygrane/lotto'
        self.url_polish_cities = 'https://www.polskawliczbach.pl/Miasta'
        self.url_polish_provinces = 'https://www.polskawliczbach.pl/Wojewodztwa'

        #defining sql user and connection with the "project_lotto" database
        user = "final_project"
        passwd = "qwerty"
        self.conn = pymysql.connect("localhost", user, passwd, "project_lotto")
        self.c = self.conn.cursor()

    def scrape_lottery_data(self):
        #definig lists that will serve as a base for creating the table with prize, location and dates of lottery wins
        self.lottery_prizes_list = []
        self.lottery_locations_list = []
        self.lottery_dates_list = []

        while True:
            self.page = requests.get(self.url_lotto)
            html_content = BeautifulSoup(self.page.content, 'html.parser')
            prizes = html_content.find_all(class_ = 'numbers_in_list numbers_in_list_najwyzsze_wygrane')
            cities = html_content.find_all(class_ = 'date_in_list date_in_list_najwyzsze_wygrane_miasto')
            dates = html_content.find_all(class_='date_in_list date_in_list_najwyzsze_wygrane_date')
            next_page = html_content.find_all(class_ = 'prev_next')

            for index, prize in enumerate(prizes):
                #preparing prizes as int type. replace function was used to remove spaces prior convertion into int.
                if index > 0:
                    self.lottery_prizes_list.append(int(str(prizes[index]).split('>')[1].split(',')[0].replace(" ", "")))
                    self.lottery_locations_list.append(str(cities[index]).split('>')[1].split(' <')[0])
                    self.lottery_dates_list.append(str(dates[index]).split('>')[1].split(' <')[0])


            if "Następny" in str(next_page[1]): #getting url of the next page with prizes
                self.url_lotto = "http://megalotto.pl" + str(next_page[1]).split('href="')[1].split('"')[0]
            else:
                break #ending the 'while True' loop when there is no next page with prizes list


    def add_lottery_data_to_database(self):

        self.c.execute("create table lottery_data ("
                       "prize_id int primary key auto_increment,"
                       "lottery_prize int,"
                       "lottery_location varchar(255),"
                       "lottery_date date)")
        self.conn.commit()
        print("lottery_data table was added to the project_lotto database")
        
        for index, prize in enumerate(self.lottery_prizes_list):
            self.c.execute("insert into lottery_data values (default, %s, %s, %s)",
                           (prize, self.lottery_locations_list[index], self.lottery_dates_list[index]))
        self.conn.commit()
        print("lottery data were added to lottery_data table")
        self.conn.close()


    def scrape_polish_cities_data(self):
        self.polish_cities_table = pd.read_html(self.url_polish_cities)[0].drop(['Unnamed: 0', 'Powiat', 'Obszar'], axis=1)
        # changing columns' names to english
        self.polish_cities_table.columns = ['City', 'Province', 'Population'] #changing columns' names to english

        #changing format from object into str and int. no inplace argument for series.str.replace function and copy=False
        #for df.astype is not assigning changes to existing variables, so reassign is applied.
        self.polish_cities_table['Population'] = self.polish_cities_table['Population'].str.replace(" ", "")
        self.polish_cities_table = \
            self.polish_cities_table.astype({'City' : 'string', 'Province' : 'string', 'Population' : 'int'})
        self.polish_cities_table.info()

    def add_polish_cities_to_database(self):
        engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                               .format(user="final_project",
                                       pw="qwerty",
                                       db="project_lotto"))
        self.polish_cities_table.to_sql('polish_cities', con=engine)

    def scrape_polish_provinces(self):
        self.polish_provinces_table = pd.read_html(self.url_polish_provinces)[0].drop(['Unnamed: 0', 'Obszar'], axis=1)
        self.polish_provinces_table.columns = ['Province', 'Population', 'Urbanisation [%]']
        self.polish_provinces_table['Population'] = self.polish_provinces_table['Population'].str.replace(" ", "")
        self.polish_provinces_table['Urbanisation [%]'] = self.polish_provinces_table['Urbanisation [%]'].str.replace(",", ".")
        self.polish_provinces_table['Urbanisation [%]'] = self.polish_provinces_table['Urbanisation [%]'].str.replace("%", "")
        # changing columns' names to english
        self.polish_provinces_table = \
            self.polish_provinces_table.astype({'Province' : 'string', 'Population' : 'int', 'Urbanisation [%]' : 'float'})
        print(self.polish_provinces_table.info())

    def add_polish_provinces_to_database(self):
        engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                               .format(user="final_project",
                                       pw="qwerty",
                                       db="project_lotto"))
        self.polish_provinces_table.to_sql('polish_provinces', con=engine)