from bs4 import BeautifulSoup as soup
import requests
import pandas as pd
import time
import re
from functools import reduce
import sys
from urllib.error import HTTPError


def get_fixture_data(url, league, season):
    print('Getting fixture data...')
    fixturedata = pd.DataFrame([])
    tables = pd.read_html(url)

    fixtures = tables[0][['Wk', 'Day', 'Date', 'Time', 'Home', 'Away', 'xG', 'xG.1', 'Score']].dropna()
    fixtures['season'] = url.split('/')[6]
    fixturedata = pd.concat([fixturedata, fixtures])

    # assign id for each game
    fixturedata["game_id"] = fixturedata.index

    # export to csv file
    fixturedata.reset_index(drop=True).to_csv(f'{league.lower()}_{season.lower()}_fixture_data.csv',
                                              header=True, index=False, mode='w')
    print('Fixture data collected...')


def get_match_links(url, league):
    print('Getting player data...')
    match_links = []
    html = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
    links = soup(html.content, "html.parser").find_all('a')
    key_words_good = ['/en/matches/', f'{league}']
    for l in links:
        href = l.get('href', '')
        if all(x in href for x in key_words_good):
            if 'https://fbref.com' + href not in match_links:
                match_links.append('https://fbref.com' + href)
    return match_links


def player_data(match_links, league, season):
    player_data = pd.DataFrame([])
    for count, link in enumerate(match_links):
        try:
            tables = pd.read_html(link)
            for table in tables:
                try:
                    table.columns = table.columns.droplevel()
                except Exception:
                    continue
            def get_team_1_player_data():
                data_frames = [tables[3], tables[9]]
                df = reduce(lambda left, right: pd.merge(left, right,
                                                         on=['Player', 'Nation', 'Age', 'Min'], how='outer'),
                            data_frames).iloc[:-1]

                return df.assign(home=1, game_id=count)

            def get_team_2_player_data():
                data_frames = [tables[10], tables[16]]
                df = reduce(lambda left, right: pd.merge(left, right,
                                                         on=['Player', 'Nation', 'Age', 'Min'], how='outer'),
                            data_frames).iloc[:-1]
                return df.assign(home=0, game_id=count)
            t1 = get_team_1_player_data()
            t2 = get_team_2_player_data()
            player_data = pd.concat([player_data, pd.concat([t1, t2]).reset_index()])

            print(f'{count + 1}/{len(match_links)} matches collected')
            player_data.to_csv(f'{league.lower()}_{season.lower()}_player_data.csv',
                               header=True, index=False, mode='w')
        except:
            print(f'{link}: error')
        time.sleep(3)

def main():
    league = 'Premier-League'
    season = '2022-2023'
    url = 'https://fbref.com/en/comps/9/2022-2023/schedule/2022-2023-Premier-League-Scores-and-Fixtures'
    get_fixture_data(url, league, season)
    match_links = get_match_links(url, league)
    player_data(match_links, league, season)

    print('Data collected!')

if __name__ == '__main__':
    try:
        main()
    except HTTPError:
        print('The website refused access, try again later')
        time.sleep(5)