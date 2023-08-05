import requests, json
import os
import time
from bs4 import BeautifulSoup
from datetime import date
import pandas as pd
import re
class company():
    def __init__(self, ticker, name, email, debug = False):
        self.hdr = {'User-Agent' : name + email}
        self.debug = debug
        self.ticker = ticker
        url = 'http://www.sec.gov/cgi-bin/browse-edgar?CIK={}&Find=Search&owner=exclude&action=getcompany&count=10'
        if self.debug:
            print(url.format(ticker))
        results = requests.get(url.format(ticker), headers = self.hdr)
        tables = pd.read_html(results.text)
        table = tables[2]
        table = table[table['Filings'].isin(['10-K', '10-Q'])]
        self.latest_filing_date = date.fromisoformat(table['Filing Date'][0])
        if self.debug:
            print(self.latest_filing_date) 
        match = re.search(r'CIK=\d{10}', results.text)
        if match:
            self.cik = match.group().split('=')[1]
        else:
            exit("No match found.")
        self.edgar_root = "https://www.sec.gov/"
        self.edgar_data = "Archives/edgar/data/"
        url_str = 'https://data.sec.gov/submissions/CIK{}.json'.format(self.cik)
        if self.debug:
            print(url_str)
        if not os.path.isdir(os.path.join(os.environ['FINPYDATA'], "edgar", "submissions")):
            os.makedirs(os.path.join(os.environ['FINPYDATA'], "edgar", "submissions"))
        if not os.path.isdir(os.path.join(os.environ['FINPYDATA'], "edgar", "submissions", self.cik)):
            os.makedirs(os.path.join(os.environ['FINPYDATA'], "edgar", "submissions", self.cik))
        if not os.path.isdir(os.path.join(os.environ['FINPYDATA'], "edgar", "api", "xbrl", "companyconcept", 'CIK{}'.format(self.cik), "us-gaap")):
            os.makedirs(os.path.join(os.environ['FINPYDATA'], "edgar", "api", "xbrl", "companyconcept", 'CIK{}'.format(self.cik), "us-gaap"))
        self.concept_dir = os.path.join(os.environ['FINPYDATA'], "edgar", "api", "xbrl", "companyconcept", 'CIK{}'.format(self.cik), "us-gaap")
        self.cik_json_file = os.path.join(os.environ['FINPYDATA'], "edgar", "submissions", self.cik + '.json')
        if self.debug:
            print(self.cik_json_file) 
        if not os.path.isfile(self.cik_json_file) or self.latest_filing_date < date.fromtimestamp(os.path.getmtime(self.cik_json_file)):
            url = requests.get(url_str, headers = self.hdr)
            with open(self.cik_json_file, 'w') as file:
                file.write(url.text)
        with open(self.cik_json_file, 'r') as file:
            self.cik_json = json.load(file)
    def get_cik(self):
        return self.cik
    def get_ticker(self):
        return self.ticker
    def get_concept(self, concept):
        xbrl_api_url = "https://data.sec.gov/api/xbrl/companyconcept/CIK{}/us-gaap/{}.json".format(self.cik, concept)
        concept_file = os.path.join(self.concept_dir, concept + ".json")
        if self.debug:
            print(xbrl_api_url)
        if not os.path.isfile(concept_file) or self.latest_filing_date < date.fromtimestamp(os.path.getmtime(concept_file)):
            url = requests.get(xbrl_api_url, headers = self.hdr)
            with open(concept_file, 'w') as file:
                file.write(url.text)
        with open(concept_file, 'r') as file:
            concept_json = json.load(file)
        return concept_json
    def get_concept_quaterly_df(self, concept, get_concept = True):
        if get_concept:
            concept_json = self.get_concept(concept)
        else:
            concept_json = concept
        df = pd.DataFrame.from_records(concept_json['units']['USD'])
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
    def get_concept_yearly_df(self, concept, get_concept = True):
        if get_concept:
            concept_json = self.get_concept(concept)
        else:
            concept_json = concept
        df = pd.DataFrame.from_records(concept_json['units']['USD'])
        df = df[df['frame'].notna()]
        df['start'] = pd.to_datetime(df.loc[:]['start'])
        df['end'] = pd.to_datetime(df.loc[:]['end'])
        df = df[~df['frame'].str.contains('Q')]
        df.loc[:]['frame'] = df['frame'].str[2:]
        df = df.set_index(pd.PeriodIndex(df['frame'], freq='Y'))
        return df

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
                              self.cik_json['filings']['recent']['reportDate'])
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
