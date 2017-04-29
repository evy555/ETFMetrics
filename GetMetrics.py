import pandas as pd
import datetime as dt
import time
import os

class ETFMetrics():
    def __init__(self):
        self.todays_date = dt.date.today().strftime("%m/%d/%Y").replace('/','')
        self.df = pd.read_csv('ETF.csv',names=['Fund'])
        self.ticker_list = self.df['Fund'].tolist()
        self.newFolderHoldings = 'Holdings{}'.format(self.todays_date)
        self.newFolderMetrics = 'Metrics{}'.format(self.todays_date)

        try:
            os.makedirs('ETFMetrics\{}'.format(self.newFolderHoldings))
            os.makedirs('ETFMetrics\{}'.format(self.newFolderMetrics))
        except:
            print("Folder already exists")


    ### Function to grab all holdings for imported ETF list.
    def get_holdings(self):
        start_time = time.time()
        self.etfHoldings = []
        for i in self.ticker_list:
            print(i)
            total = 0
            results = pd.DataFrame()
            while True:
                try:
                    url = 'http://research2.fidelity.com/fidelity/screeners/etf/public/etfholdings.asp?symbol={}&view=Holdings&page={}'.format(i,total)
                    hd = pd.read_html(url)[0]
                    hd['Weight'] = hd['Weight'].apply(lambda x: float(x.split('%')[0])/100)
                    results = results.append(hd)
                    total += 1
                except:
                    print('Nothing grabbed at page {}'.format(total))
                    break
            try:
                print('before drop dup {}'.format(results.count()))
                results.drop_duplicates(subset=['Symbol','Company'],keep='first',inplace=True)
                print('after drop dup {}'.format(results.count()))
                if results['Weight'].sum() > 0:
                    results.to_csv('ETFMetrics\{}\{}{}.csv'.format(self.newFolderHoldings,i,self.todays_date),index=False)
                    self.etfHoldings.append(results)
                else:
                    print('This fund had no information to add {}'.format(i))
            except Exception as e:
                print(e)
                print('This fund had no information to add {}'.format(i))
        print("{} Minutes".format((time.time()-start_time)/60))

    ### Combine's all tickers into one list and then grabs the metric information from Fidelity. Doing it this way reduces duplication of web scraping.
    def get_metrics(self):
        start_time = time.time()

        self.problem_tickers = []
        self.problem_companies = []
        column_names = ['Market Capitalization','Total Return (1 Year Annualized)','Beta (1 Year Annualized)',
                        'EPS (TTM)','Current Consensus EPS Estimate','EPS Growth (TTM vs. Prior TTM)',
                        'P/E (TTM)','Dividend Yield (Annualized)','Total Revenue (TTM)',
                        'Revenue Growth(TTM vs Prior TTM)','Shares Outstanding','Shares Short','Institutional Ownership']

        cash_metrics = ['$0.00B','0.00','0.00','$0.00','$0.00','0.00%','0.00','0.00%','$0B','0.00%','0','0.00M','0%']

        self.allTickers = pd.concat(self.etfHoldings)[["Symbol","Company"]]
        self.allTickers.drop_duplicates(subset = "Symbol", inplace=True)
        self.allTickers.reset_index(inplace = True, drop = True)
        self.allTickers.to_csv("AllHoldings.csv", index=False)

        def add_metrics(i, metric_list,column_names):
            metric_counter = 0
            for name in column_names:
                self.allTickers.loc[i,name] = metric_list[metric_counter]
                metric_counter += 1

        l = len(self.allTickers.index.values)
        print("There are {} unique symbols".format(l))
        for i in range(0,l):
            symbol, company = self.allTickers.loc[i,'Symbol'], self.allTickers.loc[i,'Company']
            print(symbol)
            if (len(company) == 5) and (company[0:4] == 'Cash'):
                print(symbol, company)
                self.allTickers.loc[i,'Symbol'] = 'CASH'
                self.allTickers.loc[i,'Company'] = 'Cash'
                add_metrics(i, cash_metrics,column_names)

            else:
                url = 'https://eresearch.fidelity.com/eresearch/evaluate/snapshot.jhtml?symbols={}&type=o-NavBar'.format(symbol)
                try:
                    metrics = pd.read_html(url)[9].iloc[7:,1].tolist()
                    add_metrics(i, metrics,column_names)

                except:
                    print('There was an issue with ticker {} company {}'.format(symbol, company))
                    self.problem_tickers.append(symbol)
                    self.problem_companies.append(company)
        self.allTickers.to_csv('ETFMetrics\{}\AllTickermetrics{}.csv'.format(self.newFolderMetrics,self.todays_date),index=False)
        self.allTickers.set_index(["Symbol"], inplace=True)
        print("All Tickers took {} Minutes".format((time.time()-start_time)/60))


    ### Takes the metrics from each holding and applies them to the holdings for each individual ETF.
    def individual_metrics(self):
        try:
            column_names = list(self.allTickers.columns.values)
            allTickers = self.allTickers
            allTickers.drop_duplicates(subset = "Symbol", inplace=True)
        except:
            allTickers = pd.read_csv("ETFMetrics\Metrics{0}\AllTickermetrics{0}.csv".format(self.todays_date),encoding="iso-8859-1")
            allTickers.drop_duplicates(subset = "Symbol", inplace=True)
            allTickers.set_index(["Symbol"], inplace=True)
            column_names = list(allTickers.columns.values)
        column_names = column_names[2:]
        for ticker in self.ticker_list:
            etfDF = pd.read_csv("ETFMetrics\Holdings{1}\{0}{1}.csv".format(ticker,self.todays_date),encoding="iso-8859-1")
            etfLength = len(etfDF.index.values)
            for i in range(0,etfLength):
                for column in column_names:
                    etfDF.loc[i,column] = allTickers.loc[etfDF.loc[i,"Symbol"],column]
            etfDF.to_csv('ETFMetrics\{}\{}{}.csv'.format(self.newFolderMetrics,ticker, self.todays_date),index=False)







Metrics = ETFMetrics()
Metrics.get_holdings()
Metrics.get_metrics()
Metrics.individual_metrics()
