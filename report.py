#!/usr/bin/python -tt
from __future__ import print_function
import sys
import os
import csv
import sqlite3
import pandas as pd
import numpy as np

file_db = 'db/invest.db'

# number of years of data to extract
# some companies' latest annual reports are of diffrent years.
# get 6 years to maximize overlap
years = 6  

num_tickers = 0
data_frame = pd.DataFrame([])

#
# Convert none to empty string
#
def xstr(s):
    if s is None:
        return ''
    return str(s)

#
# Generate report in csv format from DB
#
def gen_csv():
    global data_frame, years
    file_csv = 'report.csv'
    data_frame = data_frame.reset_index(drop=True)
    os.chdir('./analysis')

    with pd.option_context('display.max_rows', None, 'display.max_columns', None):
        print(data_frame)

    with open(file_csv, 'wb') as fd:
        wr = csv.writer(fd)

        # Revenue
        wr.writerow(['Revenue'])
        df = pd.pivot_table(data_frame, index=['Ticker'], columns=['Year'], values='Revenue')
        df.to_csv(fd, mode = 'a')

        # Net_Profit_Margin
        wr.writerow(['Net Profit Margin'])
        df = pd.pivot_table(data_frame, index=['Ticker'], columns=['Year'], values='NPM')
        df.to_csv(fd, mode = 'a', header=False)

        # ROE
        wr.writerow(['ROE'])
        df = pd.pivot_table(data_frame, index=['Ticker'], columns=['Year'], values='ROE')
        df.to_csv(fd, mode = 'a', header=False)

        # Current_Ratio,
        wr.writerow(['Current Ratio'])
        df = pd.pivot_table(data_frame, index=['Ticker'], columns=['Year'], values='Current_Ratio')
        df.to_csv(fd, mode = 'a', header=False)

        # Cash_Ratio
        wr.writerow(['Cash Ratio'])
        df = pd.pivot_table(data_frame, index=['Ticker'], columns=['Year'], values='Cash_Ratio')
        df.to_csv(fd, mode = 'a', header=False)

        # Debt_Equity_Ratio
        wr.writerow(['Debt Eqty Ratio'])
        df = pd.pivot_table(data_frame, index=['Ticker'], columns=['Year'], values='Debt_Equity_Ratio')
        df.to_csv(fd, mode = 'a', header=False)

        # PE
        wr.writerow(['PE'])
        df = pd.pivot_table(data_frame, index=['Ticker'], columns=['Year'], values='PE')
        df.to_csv(fd, mode = 'a', header=False)
       
        # BVPS
        wr.writerow(['BVPS'])
        df = pd.pivot_table(data_frame, index=['Ticker'], columns=['Year'], values='BVPS')
        df.to_csv(fd, mode = 'a', header=False)
        
        # EPS
        wr.writerow(['EPS'])
        df = pd.pivot_table(data_frame, index=['Ticker'], columns=['Year'], values='EPS')
        df.to_csv(fd, mode = 'a', header=False)

        # DPS
        wr.writerow(['DPS'])
        df = pd.pivot_table(data_frame, index=['Ticker'], columns=['Year'], values='DPS')
        df.to_csv(fd, mode = 'a', header=False)

        # Interest expense
        wr.writerow(['Op Earnings per Share'])
        df = pd.pivot_table(data_frame, index=['Ticker'], columns=['Year'], values='EBITPS')
        df.to_csv(fd, mode = 'a', header=False)

        # Interest expense
        wr.writerow(['Interest Expense per Share'])
        df = pd.pivot_table(data_frame, index=['Ticker'], columns=['Year'], values='IPS')
        df.to_csv(fd, mode = 'a', header=False)

    fd.close()
    return

#
# Fetch financial data from DB
# arguments:
#        tickers - list of tickers in tuple format [EXCHANGE, TICKER]
#        conn    - database connection
#
def gen_report(ticker, conn):
    global data_frame, years

    print('Generating report: ' + ticker[0])

    # Get annual report data for past # years
    sql1 = ("select year as Year, "
            "month as Month, "
            "i001 as Date, "
            "ticker as Ticker, "
            "is001 as Revenue, "
            "r007 as NPM, "
            "r016 as ROE, "
            "r021 as Current_Ratio, "
            "a001/el009 as Cash_Ratio, "
            "r023 as Debt_Equity_Ratio, "
            "r001 as PE, "
            "r055 as BVPS, "
            "is061 as EPS, "
            "is062 as DPS, "
            "is025/bs008 as EBITPS, "
            "(is042 + is026)/bs008 as IPS "
            "from fin_annual where ticker ='%s' " 
            "order by year desc "
            "limit %s") % (ticker[0], years)
    
    try:
        df1 = pd.read_sql(sql1, conn)
        df = df1
    except Exception, e:
        print(str(e))
        return

    # Check if latest record in annualized table is newer
    sql2 = ("select year as Year, "
            "month as Month, "
            "i001 as Date, "
            "ticker as Ticker, "
            "is001 as Revenue, "
            "r007 as NPM, "
            "r016 as ROE, "
            "r021 as Current_Ratio, "
            "a001/el009 as Cash_Ratio, "
            "r023 as Debt_Equity_Ratio, "
            "r001 as PE, "
            "r055 as BVPS, "
            "is061 as EPS, "
            "is062 as DPS, "
            "is025/bs008 as EBITPS, "
            "(is042 + is026)/bs008 as IPS "
            "from fin_annualized where ticker ='%s' " 
            "order by year desc, month desc "
            "limit 1") % (ticker[0])

    try:
        df2 = pd.read_sql(sql2, conn)
    except Exception, e:
        print(str(e)).iloc[[0]]
        return
    
    if df2.empty:
        return
    if df2.iloc[[0]]['Year'][0] < df1.iloc[[0]]['Year'][0]:
        return
    elif df2.iloc[[0]]['Month'][0] > df1.iloc[[0]]['Month'][0]: 
        df2.set_value(0, 'Year', df1.iloc[[0]]['Year'][0] + 1)
        df = pd.concat([df2, df1])
    
    data_frame = data_frame.append(df)

    return

#
# print usage info
#
def print_usage(name):
    print('Synopsis:')
    print('          Generate report from database')
    print('Usage:')
    print('        ', name, '<filename>')
    return

#
# Args:
#       filename: file contains list of exchange and tickers
#
def main():
    if len(sys.argv) < 2:
        print_usage(sys.argv[0])
        return

    file_in = sys.argv[1]

    try:
        f = open(file_in, "rU")
    except Exception, e:
        print(str(e))
        sys.exit('ERROR: Unable to open ' + file_in)

    tickers = f.readlines()
    tickers = [x.strip().split(',') for x in tickers]
    f.close()

    try:
        conn = sqlite3.connect(file_db)
    except:
        print('ERROR: unable to open database')
    
    for t in tickers:
        gen_report(t, conn)

    gen_csv()

    conn.close()
    return

if __name__ == '__main__':
    main()
