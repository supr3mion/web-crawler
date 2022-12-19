import gc
# import io
import json
import os
import os.path
import random
import re
import shutil
import ssl
import sys
import time
import traceback
import urllib
import urllib.error
import urllib.parse
import urllib.request
import urllib.robotparser
from datetime import datetime
from tabnanny import check
from typing import Container
# from zipfile import ZipFile
from urllib.parse import urlparse
from uuid import uuid4

# import elasticsearch
import pyodbc
from bs4 import BeautifulSoup
from lxml import etree

import redis
#folowing information if for elasticsearch and kibana
# from ElasticConnect import client, index, index_download, index_error
from redis.client import parse_slowlog_get

print("\nStarting...")

official_doc = {
    "@i": "",
    "@l": "",
    "@m": "",
    "@t": "",
    "@x": "",
    "Application": "",
    "SourceContext": "",
    "EnvironmentUserName": "",
    "MachineName": "",
    "AdditionalInfo": {}
}
additional_doc = {
    "StatusCode": "",
    "WebAddress": "",
    "FileLocation": "",
    "WebDomain": "",
    "Origin": ""
}

#the folowing information is vor the random user-agend genrator
from random_user_agent.params import OperatingSystem, SoftwareName
from random_user_agent.user_agent import UserAgent

software_names = [SoftwareName.EDGE.value, SoftwareName.OPERA.value]
operating_systems = [OperatingSystem.WINDOWS.value, OperatingSystem.LINUX.value]   

user_agent_rotator = UserAgent(software_names=software_names, operating_systems=operating_systems, limit=None)

# Add any url you don't want to crawl.
blacklist = ["/shoppingcart", "/shopping-cart", "/cart", "/winkelwagen", "mailto:", "tel:+"]

rp = urllib.robotparser.RobotFileParser()

# opening database connection
server = ''
database = 'WebCrawler'
username = 'webcrawler'
password = ''
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()

print("Connected to database")

rootPath = ""

# reddis connection setup
# r = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)
Elastic = redis.StrictRedis(host="", port=, db=0, decode_responses=True)
red = redis.StrictRedis(host="", port=, db=, decode_responses=True, password="")
red.client_setname("")
prot = redis.StrictRedis(host="", port=, db=, decode_responses=True, password="")
prot.client_setname("")
print("Connected to redis, client name set to: ")
QueKey = ""
ela_key = "Logging"
BufQueKey = ""
CentralIndex = 0
HeshKey = ""

WebDomain = ""

crawler_proxy_http = ""
crawler_proxy_https = ""
proxies = {}

ExpireKey = ""
xpathselector = ""
Expire = {}
QueryFilter = {}
ErrorFormat={
    "ErrorCode": "",
    "Error": "",
    "Where": "",
    "When": ""
}

StartTime = time.time()

TimeSleep = 0.0001  # tijdens het toevoegen aan de queue kan de crawler gaan zeuren als het te snel gaat


class crawler():
    def start(data):
        siteurl = data
        global soup
        global WebDomain
        global crawling
        global proxies
        global QueKey
        global StatusCode
        global BufQueKey
        global xpathselector
        global ExpireKey
        global QueryFilter
        # Create FolderPath if it does not exist.
        folderpath = str(siteurl["FolderPath"])
        folderpath = folderpath.replace("D:\\", "")
        folderpath = folderpath.replace("\\", "/")
        folderpath = folderpath.lower()
        folderpath = "/mnt/d/" + folderpath
        # Block any request to crawl if it exists inside /robots.txt
        try:
            if rp.can_fetch("*", siteurl["BaseUrl"] + siteurl["Path"]) == False:
                print("blocked by robots.txt")
                red.delete(ExpireKey)
                del folderpath, ExpireKey
                return
        except:
            red.delete(ExpireKey)
            del folderpath, ExpireKey
            crawler.GETError("crawler.start:robot_parser.can_fetch", siteurl)
            pass
        crawling = siteurl["BaseUrl"] + siteurl["Path"]

        user_agent = user_agent_rotator.get_random_user_agent()
        opener.addheaders = [('Referer', str(crawling)), ('Accept', '*/*'), ('User-agent', user_agent)]
        urllib.request.install_opener(opener)

        url = str(siteurl["Path"])

        #ADD EXCEPTIONS FOR THE LINK AFTER IT HAS FETCHED THE 'href' TAG BELOW IN THE RIGHT ORDER#
        # If url is None. don't crawl.
        if url == None:
            red.delete(ExpireKey)
            del ExpireKey, user_agent, url, folderpath, siteurl
            return
        # If the url is absolute, it will replace BaseUrl with "".
        elif url.startswith("%s" % siteurl["BaseUrl"]):
            url = url.replace("%s" % siteurl["BaseUrl"], "")
            pass
        # If url startswith blacklist (which is specified above start() function), pass.
        elif url.startswith(tuple(blacklist)) == True:
            red.delete(ExpireKey)
            del ExpireKey, user_agent, url, folderpath, siteurl
            return
        # If a url has a "/" in it; create a folder of everything before that slash.

        # If url startswith "//", don't crawl.
        elif url.startswith("//"):
            red.delete(ExpireKey)
            del ExpireKey, user_agent, url, folderpath, siteurl
            return
        # END EXCEPTIONS

        else:
                
            if url != "":
                file_name = "".join(os.path.join(folderpath) + '%s' % re.sub('[^-A-Za-z0-9]+', '', url))
            else:
                file_name = "".join(os.path.join(folderpath) + '%s' % re.sub('[^-A-Za-z0-9]+', '', siteurl["BaseUrl"]))

            file_name = file_name[:250]

            if os.path.exists(file_name) == False:

                RandomExpire = random.randint(int(Expire[0]), int(Expire[1]))
                red.expire(ExpireKey, RandomExpire)
                del RandomExpire

                if url.endswith("/"):
                    url = url[:-1]
                
                crawl = str(url)

                crawl = crawl.replace(" ", "")

                try:
                    red.lrem(BufQueKey, -1, url)
                except:
                    crawler.GETError("crawler.start:download_function", siteurl)

                if crawl.__contains__(siteurl["BaseUrl"]):
                    del ExpireKey, user_agent, url, folderpath, siteurl
                    return

                if siteurl["BaseUrl"].endswith("/"):
                    siteurl["BaseUrl"] = siteurl["BaseUrl"][:-1]
                
                try:
                    ssl._create_default_https_context = ssl._create_unverified_context
                    urllib.request.urlretrieve(url = siteurl["BaseUrl"] + crawl, filename = file_name)
                    soup = BeautifulSoup(open(str(file_name), errors = "ignore").read(), 'html.parser')

                    #-------------------------------------------------------------------------------------------------------------------

                    try:
                        dom = etree.HTML(str(soup))
                        if dom.xpath(xpathselector)[0] != None:
                            # name = str(red.hget(HeshKey, "WebDomain")).replace("www.", "") + ":" + str(crawl).replace("/", "")
                            name = "jipsnel.nl:{}".format(crawl.replace("/", ""))
                            prot.set(name, str(soup))
                        del dom, name
                    except:
                        pass
                    try:
                        del dom
                    except:
                        pass

                    #-------------------------------------------------------------------------------------------------------------------
                        
                    StatusCode = str(200)
                    print ("\nStatusCode: " + str(StatusCode))
                    print ("crawled " + crawl)
                except urllib.error.HTTPError as e:
                    StatusCode = str(e.code)
                    crawler.status_code_error(StatusCode, siteurl, crawl, folderpath)
                    crawler.GETError("crawler.start:download_function", siteurl)
                    
                    WebDomain = str(red.hget(HeshKey, "WebDomain"))
                    crawler.DBUpload(siteurl, StatusCode, file_name, WebDomain)
                    return
                # When there is a OSError raised, save the url to a .txt and skip the url.
                except OSError as e:
                    crawler.os_error(siteurl, crawl, folderpath)
                    crawler.GETError("crawler.start:download_function", siteurl)
                    
                    WebDomain = str(red.hget(HeshKey, "WebDomain"))
                    crawler.DBUpload(siteurl, str(e.errno), file_name, WebDomain)
                    return
                # When there is a UnicodeEncodeError raised, save the url to a .txt and skip the url.
                except UnicodeEncodeError as e:
                    crawler.unicode_encode_error(siteurl, crawl)
                    StatusCode = 404
                    crawler.GETError("crawler.start:download_function", siteurl)
                    
                    WebDomain = str(red.hget(HeshKey, "WebDomain"))
                    crawler.DBUpload(siteurl, str(StatusCode), file_name, WebDomain)
                    return
                except:
                    crawler.GETError("crawler.start:download_function", siteurl)
                    return

                # Send the crawled url information to database updater
                WebDomain = str(red.hget(HeshKey, "WebDomain"))
                crawler.DBUpload(siteurl, StatusCode, file_name, WebDomain)

                crawler.elastic_handler("Information", "downloaded: {}".format(siteurl["BaseUrl"] + siteurl["Path"]), 
                "None", "crawler.start:DownloadFunction", StatusCode, "{}".format(siteurl["BaseUrl"] + siteurl["Path"]), file_name, WebDomain, siteurl["Origin"])

            else:
                red.delete(ExpireKey)
                print ("\nTried to crawl " + crawling + ", but it got DENIED")
                del file_name, url, siteurl, user_agent, crawling, folderpath, ExpireKey
                return

        for link in soup.find_all('meta', attrs = {'name':"robots"}):
            if link.get("content") and "NOFOLLOW" in str(link.get("content")).upper():
                continue

        #----------------------------------------------------------------------------------------------------------------------

        def href_search(link):
            
            if link.get("rel") and "NOFOLLOW" in str(link.get("rel")).upper():
                return

            # crawled hrefs from url.
            url = link.get('href')
            del link

            # escaping the url
            try:
                url = str(url).replace("&lt;", "<")
                url = str(url).replace("&gt;", ">")
                url = str(url).replace("&amp;", "&")
            except:
                pass

            #ADD EXCEPTIONS FOR THE LINK AFTER IT HAS FETCHED THE 'href' TAG BELOW IN THE RIGHT ORDER#
            # If url is None. don't crawl.
            if url == None:
                return
            # If the url is absolute, it will replace BaseUrl with "".
            elif url.startswith("%s" % siteurl["BaseUrl"]):
                url = url.replace("%s" % siteurl["BaseUrl"], "")
                pass
            # If url startswith blacklist (which is specified above start() function), pass.
            elif url.startswith(tuple(blacklist)) == True:
                return
            # If url startswith "//", don't crawl.
            elif url.startswith("//"):
                return

            elif siteurl["BaseUrl"].endswith("/"):
                # Remove the trailing "/"; otherwise you'll get https://www.example.com//examplepath.
                url = url[1:]

                found = False
                Query = urlparse(url).query
                for q in QueryFilter:
                    if Query.__contains__(q):
                        found = True
                if found == True:
                    del Query, found, q
                    return
                del Query, found, q

                # Create a file name from the url. If url has special characters, it will remove those characters.
                file_name = "".join(os.path.join(folderpath) + '%s' % re.sub('[^-A-Za-z0-9]+', '', url))
                file_name = file_name[:250]
                # Check if the url already is crawled by checking if the file already exists.
                if os.path.exists(file_name) == False:
                    try:
                        item = red.lrem(BufQueKey, 1, url)
                        red.lpush(BufQueKey, url)
                    except:
                        crawler.GETError("crawler.start:check_buffer_queue", None)
                    if item == 0:
                        siteurl_copy = json.loads(json.dumps(siteurl))
                        siteurl_copy["Path"] = url
                        siteurl_copy["Origin"] = siteurl["Path"]
                        try:
                            red.rpush(QueKey, json.dumps(siteurl_copy))
                        except:
                            crawler.GETError("crawler.start:add_to_queue", None)
                        time.sleep(TimeSleep)
                        del file_name, url, item, siteurl_copy
                    else:
                        del file_name, url, item
                        return
                else:
                    del file_name, url

            elif url.startswith("/"):

                found = False
                Query = urlparse(url).query
                for q in QueryFilter:
                    if Query.__contains__(q):
                        found = True
                if found == True:
                    del Query, found, q
                    return
                del Query, found, q

                # Create a file name from the url. If url has special characters, it will remove those characters.
                file_name = "".join(os.path.join(folderpath) + '%s' % re.sub('[^-A-Za-z0-9]+', '', url))
                file_name = file_name[:250]
                # Check if the url already is crawled by checking if the file already exists.
                if os.path.exists(file_name) == False:
                    try:
                        item = red.lrem(BufQueKey, 1, url)
                        red.lpush(BufQueKey, url) # verander url voor de file naam voor de volgende stress test van het programmma
                    except:
                        crawler.GETError("crawler.start:check_buffer_queue", None)
                    if item == 0:
                        siteurl_copy = json.loads(json.dumps(siteurl))
                        siteurl_copy["Path"] = url
                        siteurl_copy["Origin"] = siteurl["Path"]
                        try:
                            red.rpush(QueKey, json.dumps(siteurl_copy))
                        except:
                            crawler.GETError("crawler.start:add_to_queue", None)
                        time.sleep(TimeSleep)
                        del file_name, url, item, siteurl_copy
                    else:
                        del file_name, url, item
                        return
                else:
                    del file_name, url
            
            else:
                del url
                pass

        #----------------------------------------------------------------------------------------------------------------------

        # Find all 'a' tags from link.
        for link in soup.find_all('a'):
            href_search(link)
        
        for link in soup.find_all('link'):
            if link.get("rel") and (str(link.get("rel")).upper().__contains__("NEXT") or str(link.get("rel")).upper().__contains__("PREV")):   # kan verschillen van pagina naar pagina
                href_search(link)

        #----------------------------------------------------------------------------------------------------------------------

        del soup, folderpath, crawling, siteurl, StatusCode, user_agent, crawl, WebDomain, xpathselector
        try:
            del link
        except:
            pass
        try:
            del StatusCode
        except:
            pass

                
    # This function gets called when function start() crawls an invalid url that raises a HTTPError.
    def status_code_error(status_code, siteurl, url, folderpath):
        # If the folder and file do not yet exist; create them. The structure is like this: D:\<website>\<date>\<status_code>\<status_code>.txt
        try:
            if not os.path.exists('%s' % folderpath + '%s' % status_code):
                os.makedirs('%s' % folderpath + '%s' % status_code)
                open(file = "%s\\" % folderpath + "%s\\" % str(status_code) + "%s.csv" % str(status_code), mode = "x")
            # Write to file and return to function start().
            try:
                with open(file = "%s\\" % folderpath + "%s\\" % str(status_code) + "%s.csv" % str(status_code), mode = "a") as f:
                    f.write("%s" % siteurl["BaseUrl"] + ";" + "%s" % url + ";\n")
                    print ("\nexcel file " + str(status_code) + ".csv added BaseUrl: " + siteurl["BaseUrl"] + ", url: " + url)
            except:
                crawler.GETError("crawler.status_code_error", siteurl)
                del status_code, siteurl, url, folderpath
                return
            del status_code, siteurl, url, folderpath
            return
        except:
            crawler.GETError("crawler.status_code_error", siteurl)
            del status_code, siteurl, url, folderpath
            return

    # This function gets called when function start() crawls an invalid url that raises an OSError.
    def os_error(siteurl, url, folderpath):
        # If the folder and file do not yet exist; create them. The structure is like this: D:\<website>\<date>\599\599.txt
        try:
            if not os.path.exists('%s' % folderpath + '599'):
                os.makedirs('%s' % folderpath + '599\\')
                open(file = "%s\\" % folderpath + "599\\" + "599.csv", mode = "x")
            # Write to file and return to function start().
            try:
                with open(file = "%s\\" % folderpath + "599\\" + "599.csv", mode = "a") as f:
                    f.write("%s" % siteurl["BaseUrl"] + ";" + siteurl["Path"] + ";" + "%s" % url + ";\n")
                    print ("\nexcel file 559.csv added: " + siteurl["BaseUrl"] + siteurl["Path"] + url)
            except:
                crawler.GETError("crawler.os_error", siteurl)
                del siteurl, url, folderpath
                return
            del siteurl, url, folderpath
            return
        except:
            crawler.GETError("crawler.os_error", siteurl)
            del siteurl, url, folderpath
            return
    
    # This function gets called when function start() crawls an invalid url that raises an UnicodeEncodeError.
    def unicode_encode_error(siteurl, url, folderpath):
        # If the folder and file do not yet exist; create them. The structure is like this: D:\<website>\<date>\598\598.txt
        try:
            if not os.path.exists('%s' % folderpath + '598'):
                os.makedirs('%s' % folderpath + '598\\')
                open(file = "%s\\" % folderpath + "598\\" + "598.csv", mode = "x")
            # Write to file and return to function start().
            try:
                with open(file = "%s\\" % folderpath + "598\\" + "598.csv", mode = "a") as f:
                    f.write("%s" % siteurl["BaseUrl"] + ";" + siteurl["Path"] + ";" + "%s" % url + ";\n")
                    print ("\nexcel file 558.csv added: " + siteurl["BaseUrl"] + siteurl["Path"] + url)
            except:
                crawler.GETError("crawler.unicode_encode_error", siteurl)
                del siteurl, url, folderpath
                return
            del siteurl, url, folderpath
            return
        except:
            crawler.GETError("crawler.unicode_encode_error", siteurl)
            del siteurl, url, folderpath
            return

    def redis_request():
        while True:
            global QueKey
            global HeshKey
            global BufQueKey
            global xpathselector
            global QueryFilter
            global rootPath
            try:
                siteurl = crawler.lock()
                siteurl = json.loads(siteurl)
                folderpath = str(siteurl["FolderPath"])
                folderpath = folderpath.replace("D:\\", "")
                folderpath = folderpath.replace("\\", "/")
                folderpath = folderpath.lower()
                folderpath = "/mnt/d/" + folderpath
            except:
                crawler.GETError("crawler.redis_request", None, "fatal")

            if siteurl["Path"] == "":
                if os.path.exists('%s' % folderpath):
                    shutil.rmtree('%s' % folderpath)
                elif not os.path.exists(folderpath):
                    try:
                        os.makedirs(folderpath)
                        print("\nfolder path made: " + folderpath)
                    except:
                        pass
            else:
                pass

            if not os.path.exists(folderpath):
                try:
                    os.makedirs(folderpath)
                    print("\nfolder path made: " + folderpath)
                except:
                    pass

            robots = siteurl["BaseUrl"] + "/robots.txt"
            if robots != rootPath:

                #-----------------------------------------------------------------------------------------------------------------------------

                # file_name = "".join(os.path.join(folderpath) + '%s' % re.sub('[^-A-Za-z0-9]+', '', "robots") + ".txt")
                # if os.path.exists(file_name) == False:
                #     user_agent = user_agent_rotator.get_random_user_agent()
                #     opener.addheaders = [('Referer', "None"), ('Accept', '*/*'), ('User-agent', user_agent)]
                #     urllib.request.install_opener(opener)

                #     try:
                #         ssl._create_default_https_context = ssl._create_unverified_context
                #         urllib.request.urlretrieve(url = robots, filename = file_name)
                #     except:
                #         crawler.GETError("crawler.redis_request:robot.txt")
                #     del user_agent
                # try:
                    
                    #------------------------------------------------------------

                    # lines = []
                    # for line in open(file_name, "r"):
                    #     lines.append(line)
                    # rp.parse(lines)
                    # del lines
                    # gc.collect()

                    #------------------------------------------------------------

                    # lines = []
                    # with open(file_name, "r") as file:
                    #     for line in file:
                    #         lines.append(line)
                    #     rp.parse(lines)
                    #     file.close()
                    # del file, lines, line
                    # gc.collect()

                    #------------------------------------------------------------

                    # with open(file_name, "r") as robots_txt: # dit gebruikt te veel geheugen
                    #     robots_read = robots_txt.read()
                    #     lines = io.StringIO(robots_read).readlines()
                    #     rp.parse(lines)
                    #     robots_txt.close()
                    #     del lines, robots_read, robots_txt
                    #     gc.collect()

                    # ------------------------------------------------------------

                #-----------------------------------------------------------------------------------------------------------------------------

                try:
                    rp.set_url(siteurl["BaseUrl"] + "/robots.txt")
                    rp.read()
                    del rootPath
                    rootPath = robots

                #-----------------------------------------------------------------------------------------------------------------------------

                except:
                    pass
            del robots
            crawler.start(siteurl)
            restart = (time.time() - StartTime)
            if restart > 86400:
                crawler.program_restart()
            else:
                check = red.llen(QueKey)
                if check == 0:
                    time.sleep(10)
                    check = red.llen(QueKey)
                    if check == 0:
                        temp = red.get(siteurl["BaseUrl"] + "-Z")
                        if temp == None:
                            #-----------------------------------------------------------------------------------------------------

                            # red.set(siteurl["BaseUrl"] + "-Z", "Zipping.....")
                            # print("\nZipping files.....")
                            # shutil.make_archive(folderpath, "zip", folderpath)
                            # print("\tZipping files: Complete")
                            # print("\tCleaning files.....")
                            # shutil.rmtree(folderpath)
                            # print("\t\tCleaning files: Complete") 
                            # print("\n[X]    Completed crawling: {}\n".format(siteurl["BaseUrl"]))
                            # red.delete(siteurl["BaseUrl"] + "-Z")
                            # crawler.program_restart()

                            #-----------------------------------------------------------------------------------------------------

                            crawler.clean()
                        else:
                            crawler.clean()
            del restart, siteurl, QueKey, check, HeshKey, BufQueKey, QueryFilter, folderpath
            try:
                del temp
            except:
                pass
            gc.collect()

    def lock():
        global WebDomain
        global QueKey
        global ExpireKey
        global Expire
        global BufQueKey
        global CentralIndex
        global HeshKey
        global xpathselector
        global QueryFilter
        while True:
            time.sleep(random.uniform(0, 0.1))
            Central = red.llen("Central") # ophale hoeveelheid items Central
            if Central != 0:
                Central = red.llen("Central")
                if Central == CentralIndex: # rotatie resetten
                    CentralIndex = 0
                c = red.lindex("Central", CentralIndex) # item ophalen gebasseerd op index
                if c == None: # failsafe voor het geval dat het tog fout gaat
                    CentralIndex = 0
                    c = red.lindex("Central", CentralIndex)
                try:
                    locks = int(red.hget(c, "Locks"))  # opalen locks van hash
                except:
                    CentralIndex = CentralIndex + 1
                    continue
                # expire tijd ophalen en splitten naar een array
                Expire = red.hget(c, "Expire")
                Expire = Expire.replace(" ", "")
                Expire = Expire.split(",")
                for l in range(locks): # elke lock checken
                    time.sleep(random.uniform(0, 0.1))
                    ExpireKey = "{}{}".format(str(c), str(l))
                    locked = red.get(ExpireKey)
                    if locked == None:
                        red.set(ExpireKey, "unavailable")
                        red.expire(ExpireKey, 30) # expire key zetten op 30 seconden voor het geval dat (dit word later veranderd als het goed gaat)
                        QueKey = red.hget(c, "Queue")
                        BufQueKey = red.hget(c, "BufferQueue")
                        HeshKey = c  # key naar de hash defineren naar een wat meer begrijpelijke naam
                        try:
                            clear = red.llen(QueKey) # ophalen hoeveelheid item queue om te beslissen als reddis moet woerden schoon gemaakt
                        except:
                            crawler.program_restart()
                        if clear == 0:
                            time.sleep(5)
                            if red.llen(QueKey) == 0:
                                crawler.clean()
                                red.delete(ExpireKey)
                                CentralIndex = CentralIndex + 1
                            continue
                        elif clear == "NoneType" or clear == None:
                            time.sleep(5)
                            if red.llen(QueKey) == 0:
                                crawler.clean()
                                red.delete(ExpireKey)
                                CentralIndex = CentralIndex + 1
                            continue
                        siteurl = red.lpop(QueKey)  # ophalen queue item
                        if siteurl == None:  # kleine fail-safe voor het geval dat er iets fout gaat tijdens het ophalen/opzetten queue
                            red.delete(ExpireKey)
                            CentralIndex = CentralIndex + 1
                            continue
                        WebDomain = red.hget(c, "WebDomain")
                        xpathselector = red.hget(c, "Xpath")
                        QueryFilter = red.hget(c, "Filter")  # ophalen en omzetten filters naar array
                        QueryFilter = QueryFilter.replace(" ", "")
                        QueryFilter = QueryFilter.split(",")
                        CentralIndex = CentralIndex + 1
                        del c, l, Central, locks, locked
                        return siteurl
                del l, Expire, c, locked, locks
                CentralIndex = CentralIndex + 1  # eenmaal hier dan gaat hij kijken naar de volgende queue in central

            else:
                print("\n[X] Central contains no items")
                while red.llen("Central") == 0:
                    time.sleep(random.uniform(3, 5))

    def clean():
        global ExpireKey
        print("\nCleaning redis traces")
        try:
            locks = int(red.hget(HeshKey, "Locks"))
            for l in range(locks):
                red.delete(HeshKey + str(l))
            red.lrem("Central", 1, HeshKey)
            red.delete(HeshKey)
            red.delete(BufQueKey)
            crawler.program_restart()
        except:
            red.delete(ExpireKey)


    # will be started when the database needs to be updated
    def DBUpload(siteurl, StatusCode, FileLocation, WebDomain):
        try:
            WebAddress = siteurl["BaseUrl"] + siteurl["Path"]
            Origin = siteurl["Origin"]

            # executing the sql to add the selected information to the database
            cursor.execute("""
            INSERT INTO WebCrawler.dbo.slug (WebAddress, StatusCode, FileLocation, WebDomain, Origin)
            VALUES (?,?,?,?,?)""",
            WebAddress, StatusCode, FileLocation, WebDomain, Origin).rowcount

            cnxn.commit()
            
            # ordering the crawler to continue
            del WebAddress, StatusCode, FileLocation, WebDomain, Origin, siteurl
            return

        except:
            crawler.GETError("crawler.DBUpload", siteurl)


    # small part of code that restarts the code if needed
    def program_restart():
        try:
            python = sys.executable
            os.execl(python, python, * sys.argv)
        except:
            sys.exit()

    def GETError(location, EX, fatal=None):
        global QueKey
        global WebDomain
        global ExpireKey
        tr = traceback.format_exc()
        formatted_lines = traceback.format_exc().splitlines()
        e = formatted_lines[-1]

        if EX != None:
            print("\n[X] Error start \n")
            print(tr)
            print("[X] Error end \n")
            crawler.elastic_handler("Error", e, tr, location, "unavailable", "{}".format(EX["BaseUrl"] + EX["Path"]), "unavailable", WebDomain, EX["Origin"])
            del tr, formatted_lines, e, location, EX, fatal, WebDomain
            return
        elif fatal == None:
            if e == "KeyboardInterrupt": # dit is voor als ik het programma zelf onderbreek zodat het restart maar geen error in elasic zet
                red.delete(ExpireKey)
                try:
                    red.lpush(QueKey, json.dumps(EX))
                    crawler.program_restart()
                except:
                    crawler.program_restart()
            print("\n[X] Error start \n")
            print(tr)
            print("[X] Error end \n")
            crawler.elastic_handler("Error", e, tr, location)
            del tr, formatted_lines, e, location, EX, fatal
            return
        else:
            print("\n[X] Error start \n")
            print(tr)
            print("[X] Error end \n")
            crawler.elastic_handler("Fatal", e, tr, location)
            crawler.program_restart()

    def elastic_handler(level, message, exeption, error_location, StatusCode=None, WebAddress=None, FileLocation=None, WebDomain=None, Origin=None):
        mid = str(uuid4())
        doc = json.loads(json.dumps(official_doc))
        extra_doc = json.loads(json.dumps(additional_doc))
        now = datetime.utcnow()

        doc["@i"] = mid
        doc["@l"] = str(level)
        doc["@m"] = str(message)
        doc["@t"] = now.isoformat()
        doc["@x"] = str(exeption)
    
        doc["Application"] = "JIP Crawler"
        doc["SourceContext"] = error_location
        doc["EnvironmentUserName"] = "Ubuntu [LINUX]"
        doc["MachineName"] = "NotAvailable"

        if StatusCode != None:
            extra_doc["StatusCode"] = str(StatusCode)
            extra_doc["WebAddress"] = str(WebAddress)
            extra_doc["FileLocation"] = str(FileLocation)
            extra_doc["WebDomain"] = str(WebDomain)
            extra_doc["Origin"] = str(Origin)
            doc["AdditionalInfo"] = extra_doc
        else:
            doc["AdditionalInfo"] = {}

        try:
            # client.index(
            # index=index + "-" + str(now),
            # document=main_doc
            # )
            Elastic.rpush(ela_key, json.dumps(doc))
            del doc, mid, extra_doc, now
            del level, message, exeption, error_location, StatusCode, WebAddress, FileLocation, WebDomain, Origin
            return
        except:
            crawler.GETError("crawler.elastic_handler", None, "fatal")


try:
    crawler_proxy_http = "http://vvatcmgn-rotate:x5p2je0x97xc@209.127.184.202:80"
    crawler_proxy_https = "https://vvatcmgn-rotate:x5p2je0x97xc@209.127.184.202:80"
    proxies = {'http': crawler_proxy_http, 'https': crawler_proxy_https}
    # proxy = urllib.request.ProxyHandler(proxies)
    proxy = urllib.request.ProxyHandler({})
    opener = urllib.request.build_opener(proxy)
    user_agent = user_agent_rotator.get_random_user_agent()
    opener.addheaders = [('Referer', 'None'), ('Accept', '*/*'), ('User-agent', user_agent)]
    urllib.request.install_opener(opener)
    del user_agent

    crawler.redis_request()
except:
    try:
        red.delete(ExpireKey)
    except:
        pass
    crawler.GETError("fatal", None, "FATAL")
    crawler.program_restart()

# crawler_proxy_http = "http://vvatcmgn-rotate:x5p2je0x97xc@209.127.184.202:80"
# crawler_proxy_https = "https://vvatcmgn-rotate:x5p2je0x97xc@209.127.184.202:80"
# proxies = {'http': crawler_proxy_http, 'https': crawler_proxy_https}
# # proxy = urllib.request.ProxyHandler(proxies)
# proxy = urllib.request.ProxyHandler({})
# opener = urllib.request.build_opener(proxy)
# opener.addheaders = [('Referer', 'test'), ('Accept', '*/*'), ('User-agent', 'JIP Crawler')]
# urllib.request.install_opener(opener)

# crawler.redis_request()
