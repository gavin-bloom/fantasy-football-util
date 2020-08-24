# fantasy-football-util
Scrapes individual player statlines for each game of a specified season from https://www.pro-football-reference.com/. Data is transformed for consistent formatting of statlines across QB's, RB's, WR's, and TE's. Finally data is loaded into a table in a local sqlite database. In the future will expand with plotting functionality as well as possible machine learning based prediction of future performance.

#Dependencies:
Python 3.x
pandas
sqlite3
beautifulsoup4
requests
re
matplotlib (eventually)

#Getting Started:
https://www.tutorialspoint.com/sqlite/sqlite_installation.htm
pip install the other packages
useful step: download https://sqlitebrowser.org/
run it with python
