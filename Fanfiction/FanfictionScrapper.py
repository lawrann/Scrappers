# -*- coding: utf-8 -*-
"""
Created on Thu Apr  4 22:25:07 2019

@author: Lawrann
"""

import requests, re, os, threading, sys, time
from bs4 import BeautifulSoup
from threading import Thread
from nltk.stem import WordNetLemmatizer



wordnet_lemmatizer = WordNetLemmatizer()
lock = threading.Lock()
uniqueID = 10000000
textfilecount = 0
threads = list()
punctuation="?:!.,;"

wd = os.getcwd() + "\\scrappedata"

def getList(url):
    response3 = requests.get(url)
    soup3 = BeautifulSoup(response3.text, 'html.parser')
    movielist = soup3.find_all('a', href=True)
    movielinklist = list()
    moviename = list()
    count = 0
    for i in movielist:
        # first movie is 103 starwars, last movie in the list is 2190, change value to select which movie fanfic you want to scrape
        if (count >= 103 and count <=109): ##103 2190 1062<10
            movielink = 'https://www.fanfiction.net'+i['href']
            print(movielink)
            movielinklist.append(movielink)
            moviename.append(i.text)
        count = count+1
    for i in range(len(movielist)):
        try:
            textfilenumber = int(i/4)
            getMovie(movielinklist[i],moviename[i],textfilenumber)
        except:
            continue
    
    for i in threads:
        i.join()
        
def getMovie(movieUrl,movieName,textfilenumber):
    mostfavelink = "?&srt=4&lan=1&r=10"
    rflag = True
    while(rflag == True):
        try:
            response = requests.get(movieUrl+mostfavelink)
            rflag = False
        except:
            time.sleep(2)

    authorList = getAuthors(movieUrl+mostfavelink)
    linkPattern = re.compile(r'(/s/\d*/)') #href="/s/13194877/1/
    exit_flag = False
    while(exit_flag!=True):
        soup = BeautifulSoup(response.text, 'html.parser')
        linklist = soup.findAll(style='min-height:77px;border-bottom:1px #cdcdcd solid;')               
        titlelist = soup.findAll(class_="stitle")
        for i in range(len(linklist)):
            author = authorList[i]
            addlink = re.search(linkPattern, str(titlelist[i])).group(0)                      
            storylink = 'https://www.fanfiction.net'+addlink
            title = titlelist[i].text
            process = Thread(target=getPassage, args=[movieName,title,author,storylink,textfilenumber]) ## Threading
            process.start()
            threads.append(process)
        exit_flag= True
        
def getPassage(movieName,title,author, postUrl,textfilenumber):
    txtfile = (wd + "\\textfile%i.txt" %(textfilenumber))
    with lock:
        if not os.path.exists(txtfile):
            txt_file = open(txtfile, 'w') 
            txt_file.write('[')
            txt_file.close
        
    with open (txtfile,'a', encoding="utf-8") as txt_file: 
        r2flag = True
        while(r2flag == True):
            try:
                response2 = requests.get(postUrl)
                r2flag = False
            except:
                time.sleep(2)
        passagelist = list()        
        chapter_exit_flag = False
        chapterCount = 1
        metadataCount = 0
        ## For iterating through chapters
        soup = BeautifulSoup(response2.text, 'html.parser')
        metadata = soup.find_all(class_="xgray xcontrast_txt") ##metadata[count].get_text()

        while (chapter_exit_flag != True): 
            soup = BeautifulSoup(response2.text, 'html.parser')
            
            # Scraping passage
            passage = [s.get_text(separator=" ", strip=True) for s in soup.find_all( class_="storytext xcontrast_txt nocopy")]
            passage = str(passage)
            passage = passage[2:-2]
            while(True):
                if (passage.endswith("\\")):
                    passage = passage[:-1]
                else:
                    break
            try:
                passagelist.append(passage)
                ## Check for next chapter
                chapterNext = soup.findAll(style="float:right; ")
                nextChapter = re.search('Next', str(chapterNext[0]))
                
                if(str(nextChapter) == 'None'): 
                    chapter_exit_flag = True
                else:
                    ## Next Chapter
                    chapterCount = chapterCount + 1
                    chapterUrl = postUrl + str(chapterCount)
                    #print(chapterUrl)
                    response2 = requests.get(chapterUrl)
            except:
                chapter_exit_flag = True
                print('End of Chapter')
                
        with lock:
            metadata = metadata[metadataCount].get_text()
            metadataCount = metadataCount + 1
            global uniqueID
            print(uniqueID)
            spassage = ""
            for i in range(len(passagelist)):
                p = str(passagelist[i])
                p = str(p).replace('\\','')
                p = str(p).replace('"','\\"')
                spassage = spassage + '[Chapter '+ str(i+1) + '] '+ str(p)
            summary = spassage[11:111]
            while(True):
                if (summary.endswith("\\")):
                    summary = summary[:-1]
                else:
                    break
            lastchapter = passagelist[-1]
            lastchapter = lastchapter.replace('\\','')
            lastchapter = lastchapter.replace('"','\\"')
            metadata = metadata.replace('\\','')
            metadata = metadata.replace('"','\\"')
            metadata = metadata.replace('/',' ')
            postUrl = postUrl[:-1]
            title = str(title).replace('"','\\"')
            author = str(author).replace('"','\\"')
            movieName = str(movieName).replace('/',' ')
            txt_file.write('{"Movie":"' + movieName 
                                + '","Title":"' + title 
                                + '","Author":"' + author 
                                + '","Hyperlink":"' + postUrl
                                + '","Passage":"' + spassage 
                                + '","LastChapter":"' + lastchapter 
                                + '","Summary":"' + summary 
                                + '","MetaData":"' + metadata
                                +'"},')
            uniqueID = uniqueID + 1
            
        sys.exit()
        
def getAuthors(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    tlist = list()
    authorlist = soup.findAll(class_="z-list zhover zpointer ")
    namelist = list()
    for i in range(len(authorlist)):
        soup2 = BeautifulSoup(str(authorlist[i]),'html.parser')
        title = soup2.find_all('a',href=True)
        for k in range(len(title)):
            if (title[k].text != "" and title[k].text!='reviews'):
                tlist.append(title[k].text)
        
    for j in range(len(tlist)):
        if (1+j*2 >= len(tlist)):
            break
        else:
            namelist.append(tlist[1+j*2])
    return namelist

getList('https://www.fanfiction.net/movie')
           
count = 0
while (True):
    txtfile = (wd+"\\textfile%i.txt" %(count))
    if os.path.exists( txtfile):
        with open(txtfile, 'rb+') as filehandle:
            filehandle.seek(-1, os.SEEK_END)
            filehandle.truncate()
        with open (txtfile,'a', encoding="utf-8") as txt_file: 
            txt_file.write(']')
        count = int(count) + 1
    else:
        break
        
        