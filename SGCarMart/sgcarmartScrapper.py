# -*- coding: utf-8 -*-
"""
Created on Sun Aug 12 16:10:28 2018

@author: Lawrann
"""

import requests, re, os, threading
from bs4 import BeautifulSoup
from csv import writer
from threading import Thread
lock = threading.Lock()

#################################################################
def crawl3(url,index):
    response = requests.get(url)
    soup = BeautifulSoup(response.text,'html.parser')
    tablelist = soup.find_all('td')
    with lock:
        rtchk = 0
        coechk = 0
        omvchk = 0
        arfchk = 0
    
        for j in range(len(tablelist)):
            if (tablelist[j].get_text() == 'Road Tax' and rtchk == 0):
                rt = tablelist[j+1].get_text()
                if (rt!='-'):
                    rt = rt.replace('$','')
                    rt = rt.replace(',','')
                    rt = rt.replace(' /yr','')
                roadtax.append(rt)
                rtchk = 1;
            if (tablelist[j].get_text() == 'COE' and coechk == 0):
                c = tablelist[j+1].get_text()
                if (c!='-'):
                    c = c.replace('$','')
                    c = c.replace(',','')
                coe.append(c)
                coechk = 1
            if (tablelist[j].get_text() == 'OMV' and omvchk == 0):
                o = tablelist[j+1].get_text()
                if(o!='-'):
                    o = o.replace('$','')
                    o = o.replace(',','')
                omv.append(o)
                omvchk = 1
            if (tablelist[j].get_text() == 'ARF' and arfchk == 0):
                a = tablelist[j+1].get_text()
                if (a!='-'):
                    a = a.replace('$','')
                    a = a.replace(',','')
                    a = int(a)/2
                arf.append(a)
                arfchk = 1
            
        if rtchk == 0 :
            roadtax.append('-')
        if coechk == 0 :
            coe.append('-')
        if omvchk == 0 :
            omv.append('-')
        if arfchk == 0 :
            arf.append('-')
        
def crawl(changingurl,dealershipname,i):     # 1URL, 100cars
#    print('Scraping ' + dealershipname.get_text() + ' at ' + changingurl) ##
    response2 = requests.get(changingurl)
    soup2 = BeautifulSoup(response2.text, 'html.parser')
    carnamelist = soup2.findAll(style='width:186px;padding-left:4px;')
    carpricelist = soup2.findAll(style='width:67px; font-weight:bold;')
    carreglist = soup2.findAll(style= 'width:89px;')
    carmileagelist = soup2.findAll('div',style='width:83px;')
    deprelist = soup2.findAll('div', style = 'width:101px;')
    postlist = soup2.findAll('td', class_='font_gray_light font_10')
    
    for i in range(len(carnamelist)):
        link = (re.search(linkPattern,str(carnamelist[i]))).group(0)
        #hyperlink
        hyperlink = addLink + link
        carlink.append(hyperlink)
        #carname
        carname.append(carnamelist[i].get_text())
        dealership.append(dealershipname.get_text())
            #mileage
        try:
            cm = re.search(mileagePattern,carmileagelist[i].get_text()).group(0)
        except AttributeError:
            cm = '-'
            carmileage.append(cm)
        if (cm!='-'):
            cm = cm.replace(',','')
            cm = cm.replace(' km','')
            carmileage.append(cm)
            #price
        try:
            cp = re.search(pricePattern,carpricelist[i].get_text()).group(0)
        except AttributeError:
            cp = None
        if (cp!= None):
            cp = cp.replace('$','')
            cp = cp.replace(',','')
            carprice.append(cp)    
        else:
            carprice.append('SOLD')
            #depreciation
        try:
            dp = re.search(deprePattern, deprelist[i].get_text()).group(0)
        except AttributeError:
            dp = None
        if (dp!=None):   
            dp = dp.replace('$','')
            dp = dp.replace(',','')
            cardepre.append(dp)
        else:
            cardepre.append('-')
            #registered date    
        try:
            cr = re.search(regisPattern,carreglist[i*2].get_text()).group(0)
        except AttributeError:
            cr = None
        if (cr!=None):
            carreg.append(cr)  
        #post date    
        try:
            pp = re.search(postPattern,postlist[i*2].get_text()).group(0)
        except AttributeError:
            pp = None
        if (pp!=None):
            postdate.append(pp)     ############################################################################################################
        ##### soup3 opens hyperlinks to get rt,coe,omv,arf #####    

        print('Added ---' 
        +' Cars: ' + str(len(carname)) 
        +' Price: ' + str(len(carprice)) 
        +' Reg: ' + str(len(carreg))
        +' Depre: ' + str(len(cardepre)) 
        +' Link: ' + str(len(carlink)) 
        +' Mileage: ' + str(len(carmileage))
        +' Post date:' + str(len(postdate))
        +' COE: ' + str(len(coe))
        +' Road Tax: ' + str(len(roadtax))
        +' OMV: ' + str(len(omv))
        +' ARF: ' + str(len(arf)))
        
#################################################################
        
    
        
dealershipPattern = re.compile(r'listing.php\?DL=\d\d\d\d')
dealershipnumPattern = re.compile(r'\d\d\d\d')
regisPattern = re.compile(r'(\d\d-\w\w\w-\d\d\d\d|N\.A\.)')
pricePattern = re.compile(r'\$\d\d\d?,\d\d\d')
linkPattern = re.compile(r'info\.php\?ID=\d\d\d\d\d\d\d?')
mileagePattern = re.compile(r'\d\d\d?,\d\d\d\skm')
deprePattern = re.compile(r'(\$\d\d?,\d\d\d\s|N\.A\.)')
postPattern = re.compile(r'\d\d-\w\w\w-\d\d\d\d')

addLink = 'http://www.sgcarmart.com/used_cars/'
baseurl = ('http://www.sgcarmart.com/used_cars/' )
lock = threading.Lock()

#### soup1 opens based url to access list of dealerships ####
response = requests.get(baseurl)
soup1 = BeautifulSoup(response.text, 'html.parser')
dealership = soup1.find('select', style='width:180px; border:1px solid #7F9DB9')
dslink = re.findall(dealershipPattern,str(dealership)) ## containts listing.php\?DL=\d\d\d\d
dsNum = re.findall(dealershipnumPattern,str(dslink)) ## containts digit for link


csvfile = 'carData.csv'
pathinput = os.getcwd() + '\\'

with open (pathinput+csvfile,'w',newline='') as csv_file:
    csv_writer = writer(csv_file)
    headers = ['Car Dealership','Car Name','$Car Price','Registered Date','$Depreciation/Yr','Car Mileage/km','Post Date','Road Tax', 'COE','OMV','ARF','Hyperlink']
    csv_writer.writerow(headers)
    
    carname = list()
    carprice = list()
    carreg = list()
    carlink = list()
    carmileage = list()
    cardepre = list()
    postdate = list()
    dealership = list()
    roadtax = list()
    coe = list()
    omv = list() 
    arf = list() 
    changingurl = list()
    dealershipname = list()
    thread = list()
    threads = list()
    
    for i in range(len(dsNum)):
        dealershipname.append(soup1.find('option', value=dslink[i]))
        changingurl.append('http://www.sgcarmart.com/used_cars/listing.php?RPG=ALL&DL=%d&ORD=' % int(dsNum[i]))
        
        ############################################################################################################

    for i in range(len(changingurl)): #873
        process2 = Thread(target=crawl, args=[changingurl[i],dealershipname[i],i])
        process2.start()
        thread.append(process2)
    for process2 in thread:
        process2.join()
    for i in range(len(carlink)):
        process = Thread(target=crawl3, args=[carlink[i],i])
        process.start()
        threads.append(process)
#    for j in range(len(carname)):
#        csv_writer.writerow([dealership[j],carname[j],carprice[j],carreg[j],cardepre[j],carmileage[j],postdate[j],carlink[j]])
    for process in threads:
        process.join()
    for j in range(len(carname)):
        csv_writer.writerow([dealership[j],carname[j],carprice[j],carreg[j],cardepre[j],carmileage[j],postdate[j],roadtax[j],coe[j],omv[j],arf[j],carlink[j]])
print('Done')
