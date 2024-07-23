import pandas as pd
import re
import os
import json
from dyplot.bar import Bar
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
from contextlib import closing
import sqlite3
import logging
logger = logging.getLogger(__name__)

class company():
    def __init__(self, ticker, conn, debug = True):
        self.ticker = ticker
        self.url = ""
        self.debug = debug
        with closing(conn.cursor()) as cursor:
            row = cursor.execute("SELECT * FROM COMPANY WHERE ticker = '{}'".format(self.ticker)).fetchone()
        self.ranking = row[0]
        self.cik = row[1]
        self.name = row[3]
        self.sic = row[4]
        self.sicDescription = row[5]
        self.latest_filing_date = row[6]
        self.latest_report_date = row[7]
        self.latest_primaryDocument = row[8]
        self.latest_accessionNumber = row[9]
        self.latest_form = row[10] 
        self.latest_filing_url = "https://www.sec.gov/Archives/edgar/data/{}/{}/{}.htm".format(self.cik, self.latest_accessionNumber.replace('-', ''), self.latest_primaryDocument)
        self.latest_inline_xbrl = "https://www.sec.gov/ix?doc=/Archives/edgar/data/{}/{}/{}".format(self.cik, self.latest_accessionNumber.replace('-', ''), self.latest_primaryDocument)
        self.fact_json_file = os.path.join(os.path.join(os.environ['FINPYDATA'], "edgar", "api", "xbrl", "companyfacts",'{}.json'.format(self.ticker)))
        try:
            with open(self.fact_json_file, 'r') as file:
                self.fact_json = json.load(file)
        except:
            print("Error loading {} json file".format(ticker))

    def get_cik(self):
        return self.cik

    def get_ticker(self):
        return self.ticker

    def get_concept(self, concept, accounting='us-gaap'):
        try:
            concept_json = self.fact_json['facts'][accounting][concept]
        except:
            raise ValueError("concept {} does not exists for {}".format(concept, self.ticker))
        return concept_json

    def get_concept_quaterly_df(self, concept, accounting='us-gaap', units='USD'):
        concept_json = self.get_concept(concept, accounting)
        df = pd.DataFrame.from_records(concept_json['units'][units])
        df = df[df['frame'].notna()]
        df['start'] = pd.to_datetime(df.loc[:]['start'])
        df['end'] = pd.to_datetime(df.loc[:]['end'])
        qf = df[df['frame'].str.contains('Q')]
        qf.loc[:]['frame'] = qf['frame'].str[2:]
        qf = qf.set_index(pd.PeriodIndex(qf['frame'], freq='Q'))
        qf = qf.reindex(pd.PeriodIndex(pd.date_range( qf.iloc[0]['start'], qf.iloc[-1]['end'],freq = 'Q')))
        nan_indexes = qf[qf['start'].isna()].index.tolist()
        row_above_nan = [x-1 for x in nan_indexes]
        row_below_nan = [x+1 for x in nan_indexes]
        qf.loc[qf['start'].isna(),'start'] = list(qf.loc[row_above_nan, 'end'] + pd.Timedelta(1, 'd'))
        qf.loc[qf['end'].isna(),'end'] = list(qf.loc[row_below_nan, 'start'] - pd.Timedelta(1, 'd'))
        val_nan_row = qf.loc[qf['val'].isna()].index.tolist()
        row3_above_val_nan = [x - 3 for x in val_nan_row]
        start = list(qf.loc[row3_above_val_nan,'start'])
        end = list(qf.loc[val_nan_row,'end'])
        year_period = pd.DataFrame({'start': start, 'end': end})
        merged = df.merge(year_period, how='outer', indicator=True)
        merged[merged['_merge'] =='both'].val
        qf.loc[val_nan_row, 'val'] = 0
        qf.loc[val_nan_row, 'val'] = list(merged[merged['_merge'] =='both'].val) - qf['val'].rolling(4).sum()[val_nan_row]
        qf.loc[val_nan_row, 'accn'] = list(merged[merged['_merge'] =='both'].accn)
        qf.loc[val_nan_row, 'filed'] = list(merged[merged['_merge'] =='both'].filed)
        qf = qf.rename(columns={'val': concept})
        return qf

    def get_concept_yearly_df(self, concept, accounting='us-gaap', units='USD'):
        concept_json = self.get_concept(concept, accounting)
        try:
            df = pd.DataFrame.from_records(concept_json['units'][units])
        except:    
            raise ValueError("unit does not exists in concept {} for {}".format(concept, self.ticker))
        try:
            df = df[df['frame'].notna()]
        except:    
            raise ValueError("frame does not exists in concept {} for {}".format(concept, self.ticker))
        df['start'] = pd.to_datetime(df.loc[:]['start'])
        df['end'] = pd.to_datetime(df.loc[:]['end'])
        df = df[~df['frame'].str.contains('Q')]
        df.loc[:]['frame'] = df['frame'].str[2:]
        df = df.set_index(pd.PeriodIndex(df['frame'], freq='Y'))
        df = df.rename(columns={'val': concept})
        return df

    def plot_concept_quaterly(self, concept, accounting='us-gaap', type = "Bar"):
        qf = self.get_concept_quaterly_df(concept, accounting)
        g = Bar(height=qf[concept], label=concept)
        g.set_xticklabels(list(qf[concept].index.strftime("%YQ%q")), "categories")
        g.option["axis"]["x"]["tick"]["rotate"] = 90 
        return(g.savefig(html_file="c3_bar.html", width="800px", height="800px"))

    def plot_concept_yearly(self, concept, accounting='us-gaap', type = "Bar"):
        qf = self.get_concept_yearly_df(concept, accounting)
        if self.debug:
            print(qf)
        g = Bar(height=qf[concept], label=concept)
        g.set_xticklabels(list(qf[concept].index.strftime("%Y")), "categories")
        g.option["axis"]["x"]["tick"]["rotate"] = 90 
        return(g.savefig(html_file="c3_bar.html", width="800px", height="800px"))

    def get_concepts(self, concept, duplicated_list=[], accounting='us-gaap'):
        """
        The argument of concept should be in the following example format.
        concept = [{"name" : "NetIncomeLoss", "units" : 'USD'},
                    {"name" : "ProfitLoss", "units" : 'USD'},
                    {"name" : "RevenueFromContractWithCustomerExcludingAssessedTax", "units" : 'USD'},
                    {"name" : "Revenues", "units" : 'USD'}
                  ]
        duplicated_list = [
                     "Net Income": ["NetIncomeLoss", "ProfitLoss"],
                     "Revenues" : ["RevenueFromContractWithCustomerExcludingAssessedTax", "Revenues"]
                   ] 
        """
        concepts = {}
        for i in concept:
            try:
                df = self.get_concept_yearly_df(i['name'], accounting, i['units'])
            except ValueError as e:    
                logger.error(e.args)
                continue
            df = df.iloc[:, 0:3]
            concepts[i['name']] = df
        if duplicated_list:
            concepts = self.remove_duplicated_concepts(concepts, duplicated_list)
        return concepts 

    def remove_duplicated_concepts(self, concepts, duplicated_list=[]):
        """
            concepts should be from the function of get_concepts.
            It is a dictionary of dataframes. The keys of the dictionary are the concepts of edgar. The dataframes has the facts 
            of these concepts.
            The following is an example of duplicated lists.
            duplicated_list = [ 
              ["NetIncomeLoss", "ProfitLoss"],
              ["RevenueFromContractWithCustomerExcludingAssessedTax", "Revenues"]
            ]
            For example, it checks the latest date of NetIncomeLoss and ProfitIncomeLoss from the duplicated list.
            It removes the keys with an earlier date and renames the key. 
        """
        for l in duplicated_list:
            dl = []
            for i in l:
                if (i in concepts) and (not concepts[i].empty):
                    dl.append(i)
            if not dl:        
                logger.error("All items in {} do not exist in {}.".format(l, self.ticker))
            if len(dl) > 1:
                p = dl[0]
                for i in range(1, len(dl)):
                    if ((concepts[dl[i]]['end'][-1] > concepts[p]['end'][-1]) or ((concepts[dl[i]]['end'][-1] == concepts[p]['end'][-1]) and (concepts[dl[i]]['end'][0] < concepts[p]['end'][0]))):
                        del concepts[p]
                        p = i
                    else:
                        del concepts[dl[i]]
        return concepts

    def get_latest_filing(self, cik_json, forms = ["10-Q", "10-K"]):
        filings_recent = zip(cik_json['filings']['recent']['form'],
                             cik_json['filings']['recent']['accessionNumber'],
                             cik_json['filings']['recent']['filingDate'],
                             cik_json['filings']['recent']['reportDate'],
                             cik_json['filings']['recent']['primaryDocument']
                            )
        for i in filings_recent:
            if i[0] in forms:
                return(i)


    def find_form_accessionNumbers(self, form):
        """
        All the sec forms:
        13F-HR
        8-K
        10-K
        10-Q
        """
        form_accessionNumbers = []
        accessionNumber = zip(self.cik_json['filings']['recent']['form'],
                              self.cik_json['filings']['recent']['accessionNumber'],
                              self.cik_json['filings']['recent']['filingDate'])
        for i in accessionNumber:
            if i[0] == form:
                form_accessionNumbers.insert(0, (i[1], i[2]))
        return form_accessionNumbers

    def get_all_forms(self, form):
        form_accessionNumbers = self.find_form_accessionNumbers(form)
        for a in form_accessionNumbers:
            aN = a[0].replace('-', '')
            if not os.path.isdir(os.path.join(self.concept_dir, aN)):
                print("form accession number " + a[0] + "of " + self.cik + " is never processed. fetching it...") 
                os.makedirs(os.path.join(self.concept_dir, aN))
                if form == "13F-HR":
                    f13_html = self.edgar_root + self.edgar_data + self.cik + "/" + aN + "/" + a[0] + "-index.html"
                    if self.debug:
                        print(f13_html)
                    f13_req = requests.get(f13_html, headers = self.hdr)
                    soup = BeautifulSoup(f13_req.text, 'html.parser')
                    match_re = "/xslForm13F_X01/" + "([\-\d]*|form13fInfoTable).xml"
                    pattern = re.compile(match_re)
                    xml_path = ""
                    for link in soup.find_all('a'):
                        href = link.get('href')
                        if pattern.search(href):
                            xml_path = href
                            html_file_name = link.contents[0]
                            break
                    if xml_path == "":
                        continue
                    f13_xml = self.edgar_root + xml_path
                    if self.debug:
                        print(f13_xml)
                    f13_xml_req = requests.get(f13_xml, headers = self.hdr)
                    f13_final_html = os.path.join(self.concept_dir, aN, html_file_name)
                    with open(f13_final_html , 'w') as file: 
                        file.write(f13_xml_req.text) 
                    with open(os.path.join(self.concept_dir, aN, "13F-HR"), 'w') as file: 
                        pass
            else:    
                print("form accession number %s of %s has been processed.", a[0], self.cik) 
