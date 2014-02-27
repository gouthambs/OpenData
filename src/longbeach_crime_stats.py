# -*- coding: utf-8 -*-
"""
Created on Sat Feb 22 12:07:53 2014

@author: Gouthaman Balaraman
"""
import requests
import pandas as pd
from bs4 import BeautifulSoup
import re
import numpy as np
import os

#####################################################
# A bunch of constants used throught the script.    #
#####################################################
_curdir= os.path.abspath(os.path.curdir)
_posdat = re.compile('(\w+):(\d+)px')
_topdat = re.compile('top:(\d+)px')
_leftdat = re.compile('top:(\d+)px')

# this is the full format with all columns; The numbers here bracket the columns
maptbl_long = [(0,75),(75,145),(145,212),(212,283),(283,350),(350,418),(418,486),
         (486,554),(554,621),(621,688),(688,756),(756,823),(823,890),(890,958),
        (958,1026),(1026,1094),(1094,1199)]
# This provides a mapping to the column with the text
mptbltxt = ['RD','MURDER','MANSLTR','FORCED_RAPE','ROBBERY','AGGRAV_ASSAULT',
'BURGLARY_RES','BURGLARY_COM','AUTO_BURG','GRAND_THEFT','PETTY_THEFT',
'BIKE_THEFT','AUTO_THEFT','ARSON','TOTAL_PART1','TOTAL_PART2','GRAND_TOTAL']

#this a truncate version I found for some months; The numbers here bracket the columns
maptbl_short=[(0,133),(133,194.5),(194.5,264),(264,329),(329,396),(396,466),(466,531),
        (531,597),(597,667.5),(667.5,736),(736,803),(803,871),(871,938),(938,1004),(1004,1300)
]    


def load_html(filename):
    soup = BeautifulSoup(file(filename).read())
    return soup

def grab_pages(soup):
    return soup.body.find_all('div')


def cleanup_data(data):
    # remove &nbsp
    data = data.replace(u'\xa0','')
    return data
    
def create_buckets(arr):
    '''
    Here we bin the rows based on 'top' value
    '''
    sarr = np.sort(arr)
    # coarseness ; this is used to separate different rows
    crsns = 10# np.mean(sdiff)
    s = 0
    prev = sarr[0]
    buckets = []
    for sa in sarr[1:]:
        if sa-prev>crsns:
            e = (sa+prev)*0.5
            buckets.append((s,e))
            s = e
            prev = sa
        #else
    buckets.append((s,s+40))
    return [buckets,[i for i,y in enumerate(buckets)]]

def create_frame(pnodes,mptbl,mptbltxt,lftmrkr):
    '''
    For a given page, here I use the position to tag it with a column number.
    Then a data frame is created and the pivot_table option is construct back
    a proper table to resemble the actual data set.
    
    '''
    df = pd.DataFrame(pnodes)
    [tmptbl,tmptblval] = create_buckets(df.top.unique()) # buckets for top
    dval = []
    for t in tmptbl:
        dvlst = df[(df["top"]>=t[0])&(df["top"]<=t[1])&(df['left']<lftmrkr)]['content'].values
        #dval.append(dvlst[0] if len(dvlst)>0 else u'RD') 
        cval = dvlst[0] if len(dvlst)>0 else u'RD'
        dval.append(cval)
        #df[(df["top"]>=t[0])&(df["top"]<=t[1])]['rowval'] = cval
    
    df['row'] = df['top'].map(lambda g: 
                [
                    dval[i] for i,x in enumerate(tmptbl)
                    if ((x[0]<=g)and(g<=x[1])) or None
                ][0]  
            )
    dfs = df[df['row']!='RD']
    dlst = dcnt = []
    for i,v in dfs.iterrows():
        if v.left<lftmrkr:
            dcnt.append(v.content)
            dlst.append(v.top)
    
   
    
    dfs['column'] = dfs['left'].map(lambda g: [mptbltxt[i] for i,x in enumerate(mptbl)
                    if ((x[0]<=g)and(g<=x[1]))][0])
                        
    pvt = dfs.pivot(index='row',columns='column',values='content')
    pvt.fillna(0,inplace=True)
    for c in pvt.columns:
        try:
            pvt[c] = pvt[c].astype(int)
        except:
            pass
    return pvt
    
'''
# this didn't work; need to check later    
def grab_monthlypdfs():
    domain='http://www.longbeach.gov'
    url = 'http://www.longbeach.gov/police/statistics.asp'
    res = requests.get(url)
    sp  = BeautifulSoup(res.text)
    tbody = sp.find_all('tbody')
    links = tbody[3].find_all('a')
    pdfdir = os.path.join(_curdir,'files','PDF')
    if not os.path.exists(pdfdir):
        os.makedirs(pdfdir)
    for l in links:
        title = '_'.join( l['title'].split(" ") )
        print title
        try:
            res = requests.get(domain+l['href'],stream=True)
            pdffile = os.path.join(pdfdir,title+'.pdf')
            with open(pdffile,'wb') as f:
                for chunk in res.iter_content(chunk_size=1024): 
                    if chunk: # filter out keep-alive new chunks
                        f.write(chunk)
                        f.flush()
        except Exception as e:
            print 'FAILED: '+str(e)+l['title']+" "+l['href']
'''        
    
def extract_nodes(p,lftmrkr):
    '''
    This is the code that extracts the beautiful soup html document into
    a bunch of nodes for easy processing
    '''
    nodes = p.find_all('p' )
    dlist = []
    nextdat = {}
    for node in nodes:
        ddict = {}
        attrs = node.attrs
        attrssty = attrs.get('style','')
        attrscls = attrs.get('class','')
        if attrscls[0] == 'ft01' or attrscls[0] == 'ft03':
            posns = _posdat.findall(attrssty)
            if len(posns) == 2:
                k,v = zip(*posns)
                if ('top' in k ) and ('left' in k):
                    if nextdat != {}:
                        nextdat['top'] = int(v[0]) if k[0] == 'top' else int(v[1])
                        ddict   = nextdat
                        nextdat = {}
                    
                    ddict[k[0]] = int(v[0])
                    ddict[k[1]] = int(v[1])
                    cont        = node.contents
                    if len(cont) == 1 :
                        ddict['content'] =  cont[0].replace('\xa0','0')
                    elif len(cont)==3:
                        ddict['content'] =  cont[0].replace('\xa0','0')
                        nextdat['content'] = cont[2].replace('\xa0','0')
                        nextdat['left'] = int(v[1])if k[1] == 'left' else int(v[0])
                    #if (ddict['left']<lftmrkr) and (ddict['content']!= 'RD'):
                    #    currrd = ddict['content']
                    #ddict['rd'] = currrd
                    dlist.append(ddict)
    return dlist
    
            
def create_html(pdffile):
    '''
    Given a pdf file, this calls pdftohtml.exe to convert to html
    '''
    try:
        pdftohtml = "pdftohtml.exe "
        htmldir = os.path.join(_curdir,'files','HTML')
        if not os.path.exists(htmldir):
            os.makedirs(htmldir)
        pdffile = os.path.abspath(pdffile)
        fileprefix = os.path.split(pdffile)[1].split('.pdf')[0]
        cmd = pdftohtml+pdffile+" -c -noframes "+os.path.join(htmldir,fileprefix+".html")
        print cmd        
        os.system(cmd)
    except Exception as e:
        print str(e)
        
def convert_all_pdfs(pdfdir):
    '''
    Convenient method to loop over all the pdf files. Calls create_html
    file in a loop.
    '''
    for f in os.listdir(pdfdir):
        if f.endswith('.pdf'):
            create_html(os.path.join(pdfdir,f))

def _finalize_dataframe(ddf):
    '''
    Does some clean-up, check sums to validate the data. This is a basic 
    check. Nothing is guaranteed!
    '''
    # do a checksum test
    if 'TOTAL_PART1' in ddf.columns:
        checksum = np.sum(\
            np.power(
                ddf[mptbltxt[1:14]].astype(int).sum(axis=1) - 
                ddf['TOTAL_PART1'].astype(int)
            ,2)
        )
        if checksum:
            print "Failed check sum test "+str(checksum)
        else:
            print "Passed checksum test"
    # reorder the columns
    if len(ddf.columns) == 17:
        ddf = ddf[mptbltxt]
    else:
        ddf = ddf[mptbltxt[:15]]
    del ddf['RD']
    ddf.index.name = 'RD'
    return ddf


def create_csv(htmlfile):
    ''' 
    This creates the csv file given a html file
    '''
    try:
        print "Converting "+htmlfile
        soup = load_html(htmlfile)
        pages = grab_pages(soup)
        num_nodes = len(pages[0])
        leftmrkr = 75 if num_nodes > 440 else 133 # to handle two pdf formats
        mptbl    = maptbl_long if num_nodes > 440 else maptbl_short 
        #filetype = 1 if num_nodes > 480 else 0 # 1 if long type else 0
        pvts = []
        for i,p in enumerate(pages):
            print 'Page-'+str(i)
            dlist = extract_nodes(p,leftmrkr)
            #df = create_frame(dlist,mptbl0,mptbltxt,leftmrkr)
            df = create_frame(dlist,mptbl,mptbltxt,leftmrkr)
            pvts.append(df)
        ddf = pd.concat(pvts)
        exclrows = set(['0'+str(i)for i in range(2000,2020,1)]) | set(['%CHG'])
        exclrows = exclrows & set(ddf.index)
        ddf.drop(exclrows,inplace=True)
        ddf.fillna(0,inplace=True)
        #cleanup 
        
        ddf = _finalize_dataframe(ddf)
        
        csvdir = os.path.join(_curdir,'files','CSV')
        if not os.path.exists(csvdir):
            os.makedirs(csvdir)
        htmlfile = os.path.abspath(htmlfile)
        fileprefix = os.path.split(htmlfile)[1].split('.html')[0]
        csvfile = os.path.join(csvdir,fileprefix+".csv")
        ddf.to_csv(csvfile)
    except Exception as e:
        print str(e)

def convert_all_htmls(htmldir):
    '''
    This is a top leve driver which calls create_csv in a loop
    '''
    for f in os.listdir(htmldir):
        if f.endswith('.html'):
            create_csv(os.path.join(htmldir,f))
            #break
if __name__=='__main__':
    '''
    Here is a complete example to loop over all pdfs and create all csvs.
    
    >>>pdfdir = "D:\\Development\\Python\\CrimeData\\files\\PDF"    
    >>>convert_all_pdfs(pdfdir)
    >>>htmldir = "D:\\Development\\Python\\CrimeData\\files\\HTML"    
    >>>convert_all_htmls(htmldir)
    
    Or you can do individual file conversions:
    
    >>>pdffile = os.path.join(pdfdir,'January_2013.pdf')
    >>>create_html(pdffile)
    '''    
    
    
    
    # Convert pdfs to html
    pdfdir = "D:\\Development\\Python\\CrimeData\\files\\PDF"    
    pdffile = os.path.join(pdfdir,'January_2013.pdf')
    create_html(pdffile)
    #convert_all_pdfs(pdfdir)
    
    # Then convert html to csv
    htmldir = "D:\\Development\\Python\\CrimeData\\files\\HTML"    
    html = os.path.join(htmldir,'January_2013.html')
    create_csv(html)
    #convert_all_htmls(htmldir)