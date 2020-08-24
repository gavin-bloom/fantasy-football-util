import pandas as pd
import sqlite3
from bs4 import BeautifulSoup
from matplotlib import pyplot as plt
import requests
import re


#global constants
url = 'https://www.pro-football-reference.com'
year = 2019
maxp = 300
fantasyConfig = {
    'ptsPerPassYd': 0.04,
    'ptsPerPassTd': 4,
    'ptsPerPassInt': -2,
    'ptsPerRushYd': 0.1,
    'ptsPerRushTd': 6,
    'ptsPerRec': 0.5,
    'ptsPerRecYd': 0.1,
    'ptsPerRecTd': 6,
    'ptsPerConv': 2,
    'ptsPerFumbLost': -2,
    'ptsPerFumbTd': 6
}
table_name = 'game_stats'
db_name = 'fantasyDB.db'

#gets around sqlite's restriction of not adding a primary key to a table after it
#is made by maiking new table and copying data over
def get_create_table_string(tablename, connection):
    sql = """
    select * from sqlite_master where name = "{}" and type = "table"
    """.format(tablename)
    result = connection.execute(sql)

    create_table_string = result.fetchmany()[0][4]
    return create_table_string

def add_pk_to_create_table_string(create_table_string, colname):
    regex = "(\n.+{}[^,]+)(,)".format(colname)
    return re.sub(regex, "\\1 PRIMARY KEY,",  create_table_string)

def add_pk_to_sqlite_table(tablename, index_column, connection):
    cts = get_create_table_string(tablename, connection)
    cts = add_pk_to_create_table_string(cts, index_column)
    template = """
    BEGIN TRANSACTION;
        ALTER TABLE {tablename} RENAME TO {tablename}_old_;

        {cts};

        INSERT INTO {tablename} SELECT * FROM {tablename}_old_;

        DROP TABLE {tablename}_old_;

    COMMIT TRANSACTION;
    """

    create_and_drop_sql = template.format(tablename = tablename, cts = cts)
    connection.executescript(create_and_drop_sql)

def calc_fantasy_pts(tdf, config):
    pts = (
        (tdf['Scoring_2PM'] * config['ptsPerConv'])
        + (tdf['Fumbles_FL'] * config['ptsPerFumbLost'])
        + (tdf['Fumbles_TD'] * config['ptsPerFumbTd'])
        + (tdf['Passing_Yds'] * config['ptsPerPassYd'])
        + (tdf['Passing_TD'] * config['ptsPerPassTd'])
        + (tdf['Passing_Int'] * config['ptsPerPassInt'])
        + (tdf['Rushing_Yds'] * config['ptsPerRushYd'])
        + (tdf['Rushing_TD'] * config['ptsPerRushTd'])
        + (tdf['Receiving_Yds'] * config['ptsPerRecYd'])
        + (tdf['Receiving_TD'] * config['ptsPerRecTd'])
        + (tdf['Receiving_Rec'] * config['ptsPerRec'])
        )
    tdf['Fantasy_Points'] = pts
    return tdf

def calc_fantasy_pts_df(tdf, config):
    tdf = tdf.apply(lambda x: calc_fantasy_pts(x, config), axis = 1)
    return tdf

# grab fantasy players list
def scrape_and_load(table_name, db_name, url, year, maxp, fantasyConfig):
    pd.set_option('mode.chained_assignment', None)
    try:
        r = requests.get(url + '/years/' + str(year) + '/fantasy.htm')
        soup = BeautifulSoup(r.content, 'html.parser')
        parsed_table = soup.find_all('table')[0]

        df = []

        # first 2 rows are col headers
        for i,row in enumerate(parsed_table.find_all('tr')[2:]):
            if i % 10 == 0: print(i, end=' ')
            if i >= maxp:
                print('\nComplete.')
                break

            try:
                dat = row.find('td', attrs={'data-stat': 'player'})
                name = dat.a.get_text()
                stub = dat.a.get('href')
                stub = stub[:-4] + '/gamelog/' + str(year)
                print(name)

                pos = row.find('td', attrs={'data-stat': 'fantasy_pos'}).get_text()


                # grab this players game stats
                tdf = pd.read_html(url + stub)[0]

                # flatten multi index
                tdf.columns = ['_'.join(col).rstrip('_') for col in tdf.columns.values]

                # fix the away/home column
                tdf = tdf.rename(columns={'Unnamed: 6_level_0_Unnamed: 6_level_1': 'Away'})
                tdf['Away'] = [1 if r=='@' else 0 for r in tdf['Away']]

                #fixing generic columnds
                tdf = tdf.rename(columns={'Unnamed: 0_level_0_Rk': 'Rk'})
                tdf = tdf.rename(columns={'Unnamed: 1_level_0_Date': 'Date'})
                tdf = tdf.rename(columns={'Unnamed: 2_level_0_G#': 'G#'})
                tdf = tdf.rename(columns={'Unnamed: 3_level_0_Week': 'Week'})
                tdf = tdf.rename(columns={'Unnamed: 4_level_0_Age': 'Age'})
                tdf = tdf.rename(columns={'Unnamed: 5_level_0_Tm': 'Team'})
                tdf = tdf.rename(columns={'Unnamed: 7_level_0_Opp': 'Opp'})
                tdf = tdf.rename(columns={'Unnamed: 8_level_0_Result': 'Result'})

                tdf = tdf.rename(columns={'Unnamed: 9_level_0_GS': 'GS'})
                tdf['GS'] = [1 if r=='*' else 0 for r in tdf['GS']]

                if 'Passing_Yds.1' in tdf:
                    tdf = tdf.rename(columns={'Passing_Yds.1': 'Passing_Sack_Yds'})



                # drop "Total" row
                tdf = tdf.query('Rk == Rk')

                # add other info
                tdf['Name'] = name
                tdf['Position'] = pos
                tdf['Season'] = year

                #fill preliminary df with 0's
                tdf = tdf.fillna(0)

                #fill fantasy points
                tdf['Fantasy_Points'] = 0


                df.append(tdf)

            except:
                pass

        #create master df
        df = pd.concat(df, ignore_index = True)
        df = df.fillna(0)
        df = calc_fantasy_pts_df(df, fantasyConfig)
        df.head()

        #create connection to sqlite3 database and loads df into a new table
        #then adds primary key functionality to the table
        conn = sqlite3.connect(db_name)
        df.to_sql(table_name, conn, if_exists="replace")
        add_pk_to_sqlite_table(table_name, "index", conn)
        conn.close()
        return True
    except:
        return False

def main():
    pass

if __name__ == "__main__":
   print(scrape_and_load(table_name, db_name, url, year, maxp, fantasyConfig))
   main()
