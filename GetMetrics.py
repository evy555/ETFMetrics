import pandas as pd
import datetime as dt
import time
import os

class ETFMetrics():
    def __init__(self):
        self.todays_date = dt.date.today().strftime("%m/%d/%Y").replace('/','')
        self.df = pd.read_csv('ETF.csv',names=['Fund'])
        self.ticker_list = df['Fund'].tolist()


    def get_holdings(self):
        start_time = time.time()
        newFolder = 'Holdings{}'.format(self.todays_date)
        os.makedirs('ETFMetrics\{}'.format(newFolder))
        for i in self.ticker_list:
            print(i)
            total = 0
            results = pd.DataFrame()
            while True:
                try:
                    url = 'http://research2.fidelity.com/fidelity/screeners/etf/public/etfholdings.asp?symbol={}&view=Holdings&page={}'.format(i,total)
                    print(url)
                    hd = pd.read_html(url)[0]
                    hd['Weight'] = hd['Weight'].apply(lambda x: float(x.split('%')[0])/100)
                    print(hd['Weight'].sum())
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
                    results.to_csv('ETFMetrics\{}\{}{}.csv'.format(newFolder,i,self.todays_date),index=False)
                else:
                    print('This fund had no information to add {}'.format(i))
            except Exception as e:
                print(e)
                print('This fund had no information to add {}'.format(i))
        print("{} Minutes".format((time.time()-start_time)/60))


    def get_metrics(self):
        start_time = time.time()
        newFolder = 'Metrics{}'.format(self.todays_date)
        os.makedirs('ETFMetrics\{}'.format(newFolder))
        problem_tickers = []
        problem_companies = []
        column_names = ['Market Capitalization','Total Return (1 Year Annualized)','Beta (1 Year Annualized)',
                        'EPS (TTM)','Current Consensus EPS Estimate','EPS Growth (TTM vs. Prior TTM)',
                        'P/E (TTM)','Dividend Yield (Annualized)','Total Revenue (TTM)',
                        'Revenue Growth(TTM vs Prior TTM)','Shares Outstanding','Shares Short','Institutional Ownership']

        cash_metrics = ['$0.00B','0.00','0.00','$0.00','$0.00','0.00%','0.00','0.00%','$0B','0.00%','0','0.00M','0%']

        def add_metrics(df, i, metric_list,column_names):
            metric_counter = 0
            for name in column_names:
                self.df.loc[i,name] = metric_list[metric_counter]
                metric_counter += 1
            return self.df

        for ticker in self.ticker_list:
            print(ticker)
            self.df = pd.read_csv('ETFMetrics\Holdings{1}\{0}{1}.csv'.format(ticker,self.todays_date),encoding = 'iso-8859-1')
            l = len(self.df.index.values)
            for i in range(0,l):
                metric_counter = 0
                symbol, company = self.df.loc[i,'Symbol'], self.df.loc[i,'Company']
                #print('{} {} Length {} first 4 {}'.format(symbol, company, len(company),company[0:4]))
                if (len(company) == 5) and (company[0:4] == 'Cash'):
                    print(symbol, company)
                    self.df.loc[i,'Symbol'] = 'Cash'
                    self.df.loc[i,'Company'] = 'Cash'
                    add_metrics(self.df, i, cash_metrics,column_names)

                else:
                    url = 'https://eresearch.fidelity.com/eresearch/evaluate/snapshot.jhtml?symbols={}&type=o-NavBar'.format(symbol)
                    try:
                        metrics = pd.read_html(url)[9].iloc[7:,1].tolist()
                        add_metrics(self.df, i, metrics,column_names)

                        #for name in column_names:
                        #df.loc[i,name] = metrics[metric_counter]
                        #metric_counter += 1
                    except:
                        print('There was an issue with ticker {} company {}'.format(symbol, company))
                        problem_tickers.append(symbol)
                        problem_companies.append(company)
            self.df.to_csv('ETFMetrics\{}\{}{}metrics.csv'.format(newFolder,ticker,self.todays_date),index=False)
            print("This fund took {} Minutes".format((time.time()-start_time)/60))
            print('-'*50)
        print("{} Minutes".format((time.time()-start_time)/60))
        return self.df, problem_tickers, problem_companies


Metrics = ETFMetrics()
Metrics.get_holdings()
Metrics.get_metrics()
