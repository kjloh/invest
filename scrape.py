#!/usr/bin/python -tt
import sys
import os
import csv
import requests
from bs4 import BeautifulSoup

#
# Convert none to empty string
def xstr(s):
    if s is None:
        return ''
    return str(s)

def gen_csv(wr, data):
    begin = False

    for d in data:  
        if begin and len(d) == 6:
            d = [xstr(e).replace(",", "") for e in d]
            # convert to csv
            wr.writerow(d)

            # Find the end of data 
            if str(d[0]).strip() == '% of leverage-to-industry':
                print "End   <<<"
                return
        elif (len(d) == 1) and (d[0] == u'INDICATORS'):
            begin = True
            print "Begin >>>"  
    return

def fetch_findata1(ticker, start_date):
    return

#
# Parse HTML and extract data we want
#
def parse_html(csv_file, soup):
    data = []

    trs = soup.find_all('tr')
    for tr in trs:
        tds = tr.findAll('td')
        data.append([td.find(text=True) for td in tds])

    try:
        myfile = open(csv_file, 'wb')
    except:
        print 'ERROR: Unable to open file: ' + csv_file 
    
    wr = csv.writer(myfile)

    gen_csv(wr, data)
        
    myfile.close()

    return

def fetch_html(u):
    try:
        page = requests.get(u)
    except:
        print 'EXCEPTION: unable to fetch HTML page.'
        return None

    if page.status_code != 200:
        print 'ERROR: unable to fetch HTML page.'
        return None

    contents = page.content
    soup = BeautifulSoup(contents, 'html.parser')

    return soup
#
# Fetch financial data from websites
# input:
#        tickers - list of tickers in tuple format [EXCHANGE, TICKER]
# http://www.advfn.com/stock-market/NASDAQ/AAPL/financials?btn=start_date&start_date=14&mode=annual_reports
#
def fetch_findata(ticker, type):
    if type == 1:
        url  = 'http://www.advfn.com/stock-market/%s/%s/financials?btn=start_date&mode=annual_reports'
        url1 = 'http://www.advfn.com/stock-market/%s/%s/financials?btn=start_date&start_date=%d&mode=annual_reports'
    elif type == 2:
        url  = 'http://www.advfn.com/stock-market/%s/%s/financials?btn=quarterly_reports&mode=company_data'
        url1 = 'http://www.advfn.com/stock-market/%s/%s/financials?btn=istart_date&istart_date=%d&mode=quarterly_reports'
    else:
        return

    num = 1
    step = 5

    print 'Ticker: ' + ticker[0]
    u = url % (ticker[1], ticker[0])
    print 'URL: ' + u
    file_str = ticker[0] + '_' + ticker[1]
    csv_file = file_str + '.csv'
    print 'File: ' + csv_file
    soup = fetch_html(u)
    if soup is None:
        return
    parse_html(csv_file, soup)

    file_str = ticker[0] + '_' + ticker[1] + '_'
    # get the years that financial data are available
    if type == 1:
        select = soup.find('select', {'id' : 'start_dateid'})
    elif type == 2:
        select = soup.find('select', {'id' : 'istart_dateid'})
    else:
        return

    if select is None:
        start_max = 5
    else:
        options = select.findAll('option')
        op_values = [op['value'] for op in options]
        start_max = int(op_values[len(op_values) - 1])

    for start_date in range(1, start_max, step):
        u = url1 % (ticker[1], ticker[0], start_date)
        print 'URL: ' + u
        csv_file = file_str + str(num) + '.csv'
        print 'File: ' + csv_file
        soup = fetch_html(u)
        if soup is None:
            return
        parse_html(csv_file, soup)
        num += 1
    
    return

def fetch_pricedata(tickers):
    url="http://www.advfn.com/stock-market/%s/%s/financials?btn=annual_reports&mode=company_data"

    return

def print_usage(name):
    print 'Synopsis:'
    print 'Download financial data and save as CSV.'
    print 'Usage:'
    print name, '<filename>'
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
    except:
        sys.exit('ERROR: Unable to open ' + file_in)

    tickers = f.readlines()
    tickers = [x.strip().split(',') for x in tickers]
    f.close()

    # fetch annual financial data
    os.chdir("data")
    for t in tickers:
        fetch_findata(t, 1)

    # fetch quarterly financial data
    os.chdir("../data_quarter")
    for t in tickers:
        fetch_findata(t, 2)
        
    return

if __name__ == '__main__':
    main()
