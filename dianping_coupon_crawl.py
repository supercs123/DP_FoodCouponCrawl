# -*- coding: UTF-8 -*-
from bs4 import BeautifulSoup
import re,time,random,urllib2,os,socket
from multiprocessing.dummy  import Pool


headers={'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.86 Safari/537.36',
        'Cookie':'_hc.v="\"beb15290-2bc8-452c-ab2d-a1d1f7f96f95.1476797445\""; \
        __utma=1.1972886557.1483260919.1490016662.1490970687.8; \
        _lxsdk_cuid=15ea4ac73ccc8-0ce33493b6de0d-e313761-100200-15ea4ac73ccc8; \
        _lxsdk=15ea4ac73ccc8-0ce33493b6de0d-e313761-100200-15ea4ac73ccc8; \
        s_ViewType=10; aburl=1; _tr.u=IQOYcZcFkVNgF1sD; \
        dper=b04546a7f9ba1c1c2a703311e3f57e88e36444c34796285b652ccfa529dbc92e; \
        ll=7fd06e815b796be3df069dec7836c3df; ua=supercs123; \
        ctu=34430638c3fbc59558454073183ed838bd76200ecf6a49b8d63e4295ffe6b18c; \
        uamo=13641731392; Hm_lvt_185e211f4a5af52aaffe6d9c1a2737f4=1521340006,1521341246,1521342682,1521349647;\
        Hm_lpvt_185e211f4a5af52aaffe6d9c1a2737f4=1521350665; tg_list_scroll=313; \
        JSESSIONID=2793719CCBC84ED2F831B828C4D051F5'}

cookieVals={"utma":"1.1972886557.1483260919.1490016662.1490970687.8",				
		 "_hc.v":"\"beb15290-2bc8-452c-ab2d-a1d1f7f96f95.1476797445\"",
		"_lx_utm": "utm_source%3De.dianping.com%26utm_medium%3Dreferral%26utm_content%3D%252Fshopclaim%252Fintroduce%252Fsearch",
		"_lxsdk": "15ea4ac73ccc8-0ce33493b6de0d-e313761-100200-15ea4ac73ccc8",
		"_lxsdk_cuid": "15ea4ac73ccc8-0ce33493b6de0d-e313761-100200-15ea4ac73ccc8",
		"_lxsdk_s": "16236ee233b-9f0-ec7-88f%7C%7C21",
		"_tr.u": "IQOYcZcFkVNgF1sD",
		"aburl": "1",
		"ctu": "34430638c3fbc59558454073183ed838bd76200ecf6a49b8d63e4295ffe6b18c",
		#"cy": "11",
		#"cye": "ningbo"
		"dper":"b04546a7f9ba1c1c2a703311e3f57e88e36444c34796285b652ccfa529dbc92e",
		"ll":"7fd06e815b796be3df069dec7836c3df",
		"s_ViewType": "10",
		"ua":"supercs123",
		"uamo":"13641731392"
		}
#headers['Cookie']=';'.join('%s=%s'%(k,v) for k,v in cookieVals.items())

#headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.89 Safari/537.36',
#           'Cookie':'s_ViewType=10;Domain=.dianping.com;Expires=Mon, 17-Jul-2017 06:18:46 GMT;Path=/;JSESSIONID=DF651374389129A9149326BA93EABCA0;_hc.v="\"34fcfbbf-b319-4355-bdae-9112b3012952.1468736483\"";PHOENIX_ID=0a0303bc-155f781c895-1283432;='}
#proxies = ['http://121.69.33.158:8080',
#           'http://61.135.217.3:80',
#           'http://61.135.217.9:80',
#           'http://61.135.217.15:80',
#           'http://61.135.217.13:80',
#           'http://61.135.217.12:80',
#           'http://203.195.204.168:8080',
#           'http://61.135.217.10:80',
#           'http://61.135.217.16:80']
#

cityNm = '' #城市名全局变量

def change_proxy():
    '''代理IP智能切换'''
    proxy =  random.choice(proxies)
    if proxy is None:
        proxy_support = urllib2.ProxyHandler({})
    else:
        proxy_support = urllib2.ProxyHandler({'http':proxy})
    opener = urllib2.build_opener(proxy_support,urllib2.HTTPHandler)
    urllib2.install_opener(opener)
    print u'切换代理:%s'%(u'本机'if proxy is None else proxy)

def multiple_replace(text,adict):
    rx = re.compile('|'.join(map(re.escape,adict)))
    def one_xlat(match):
        return adict[match.group(0)]
    return rx.sub(one_xlat,text)


def connect(url):
    '''网络请求连接并返回数据部分'''
    timeoutNo = 50
    maxTryNms = 3  #连接失败则反复尝试的最大次数 
    for tryTms in range(maxTryNms):
        try:
            req = urllib2.Request(url,headers = headers)
            response = urllib2.urlopen(req,timeout = timeoutNo).read()
            print u'正在爬取: %s'%url
            return response
        except urllib2.URLError, e :  #处理URL连接错误  
            if tryTms <= maxTryNms-1:
                continue
            else:
                if hasattr(e,"reason"):
                    print u"错误原因：",e.reason
                    if re.match(r'.*11004.*',str(e.reason)):
                        print u'网络连接出错，请检查！！\n'
                        text = raw_input(u"是否继续[y/n]?")
                        if text.lower() == 'y':
                            continue  
                        else:
                            return None
                    else:
                        print u'%s爬取失败'%url
                        return None
        except:                     #处理其它类型的错误
            if tryTms <= maxTryNms-1:
                continue
            else:
                print u'%s爬取失败'%url
                return None
            
                              

def getRegionFoodType(url):
    '''根据选定行政区和菜系要求后的页面，得到该页面的页数，三者组合后得到所有URL地址，逐个传递给getPageInfo函数进行爬取'''
    #global adminRegion_glb
    response = connect(url)
    if response is not None:
        datasoup = BeautifulSoup(response,"lxml")
        regionTags = datasoup.find(attrs={"data-id":"classify-group"}).ul.find_all('a') #行政区代码
        #print regionTags
        foodTypeTags = datasoup.find(attrs={"data-id":"classify-item"}).find_all('a')  #菜系类型代码
        adminRegions = [re.search('region_\d+s\d+',region["href"]).group() for region in regionTags]  #得到行政区代码e.g. 3s13036
        foodTyps = [re.search('category_\d+',foodType["href"]).group() for foodType in foodTypeTags[1:]]  #得到菜系,排除“不限"菜系的情况
        for foodtp in foodTyps:
            for region in adminRegions:
                url_new = url.split('-')[0]+'-'+foodtp+'-'+region+'?desc=1&sort=sale&pageIndex=0'   #菜系和行政区代码组合并按销量降序排列
                getPageInfo(url_new)
				
def getPageInfo(url):
    time.sleep(random.uniform(0.1,1))
    response = connect(url)
    if response is not None:
        '''首先将第一页的数据爬取下来，再根据得到的页码数，继续往下爬取'''
        datasoup = BeautifulSoup(response,"lxml")
        #爬取页面优惠券信息列表
        storeList = datasoup.find_all('div',class_="tg-floor-item-wrap")
        for store in storeList:
            '''调用爬取页面详细信息的函数，抓取店面的团购数据并存储'''
            getStoreDetailInfo(store)
            #得到接下来的页码数据
        pageTags = datasoup.find('div',class_='tg-paginator-wrap')
        if pageTags is not None:
            pageNo = pageTags.find_all('a',class_='tg-paginator-link')
            maxPgNo = max([int(pg.text) for pg in pageNo])
            curPage = int(pageTags.find('span',class_='tg-paginator-selected').text)
            #print "Max Page Number: %d ,Current page number: %d \n"%(maxPgNo,curPage)
            #print curPage
            if maxPgNo > curPage:
                urlNew = re.sub('pageIndex=\d+','pageIndex='+str(curPage),url,1)         #将页码组合进url,生成最后的爬取URL
                getPageInfo(urlNew)                              #开始爬取页面商户信息                      
    else:
        with open(os.getcwd()+'/'+'SingleFailedPageUrl.txt','a') as fw:
            fw.write(url+'\n')
                 
			 
def getStoreDetailInfo(mcht):
    '''获取每个商户详细信息'''
    global cityNm
    adict = {",":"^^",
             "，":"^^",
             "\\":"",
             "￥":"",
             "\n":"",
             "！":"^^"}

       #输出的保存文件名
    outFileName = cityNm+'_Coupon_Info.csv'
    mchInfo = mcht.find('a',class_='tg-floor-title')
    if mchInfo is not None:
        mchName = mchInfo.h3.text.encode('UTF-8') #商户名
        mchName = multiple_replace(mchName,adict).strip()  #去除特殊字符
        mchDesc = mchInfo.h4.text.encode('UTF-8')
        mchDesc = multiple_replace(mchDesc,adict).strip()  #团购券描述文字
    #presPrice = mcht.find('span',class_='tg-floor-price-new').text.encode('UTF-8') #现价
    presPrice = mcht.find('em').text.encode('UTF-8')
    #oriPrice = mcht.find('span',class_='tg-floor-price-old').text.encode('UTF-8')  #原价
    oriPrice = mcht.find('del').text.encode('UTF-8')
    soldTag = mcht.find('span',class_='tg-floor-sold').text
    sold = re.search('\d+',soldTag).group() if re.search('\d+',soldTag) is not None else '0'  #已售
    #存储数据
    data = ','.join([cityNm,mchName.decode('utf8'),presPrice,oriPrice,sold,mchDesc.decode('utf8')])+'\n'
    with open(outFileName,'a') as fw:
        fw.write(data.encode('gbk','ignore'))


def loadCityList(filename):
    city = {} #dict:{'cityname':'cityid'}
    with open(filename,'r') as fr:
        for line in fr.readlines()[1:]:
            li = line.strip().split('\t')
            if city.has_key(li[1].decode('GBK')):
                continue
            else:
                city[li[1].decode('GBK')] = li[0]
    return city

def oriFailedPageReCrawl():
    '''针对爬取过程中获取页码数据的原始页面失败导致的错误页面，重新进行爬取'''
    maxTryTms = 2
    filename = os.getcwd()+'/'+'OriFailedPageUrl.txt'
    for tm in range(maxTryTms):  
    #尝试爬取文件内容多次
        resList = []
        if os.path.exists(filename):
            with open(filename,'r') as fr:
                for line in fr.readlines():
                    response = connect(line.strip())
                    if response is not None:
                        datasoup = BeautifulSoup(response,"lxml")
                        pageTags = datasoup.find('div',class_='page')
                        if pageTags is not None:
                            pageNum = [pgTag.text for pgTag in pageTags]
                            for pgNo in range(1,max(pageNum)+1):
                                urlNew = line +'p'+str(pgNo)
                                getMchInfo(urlNew)
                    else:
                        #再次不成功则保存链接 
        		        if len(line) != 0:
                                    resList.append(line)
            if len(resList) != 0:    #仍有未爬取链接的情况
                with open(filename,'w') as fw:  #覆盖源文件内容
                    for item in resList:
                        fw.write(str(item)+'\n')
            else:                    #全部都爬取结束则删除文件
                os.remove(filename)

def singlFailedURLReCrawl():
    '''针对爬取过程中单个page页面爬取错误的重新爬取'''
    maxTryTms = 2
    filename = os.getcwd()+'/'+'SingleFailedPageUrl.txt'
    for tm in range(maxTryTms):
        resList = []
        if os.path.exists(filename):
            with open(filename,'r') as fr:
                for line in fr.readlines():
                    time.sleep(random.uniform(0.1,1))
                    response = connect(url)
                    if response is not None:
                        datasoup = BeautifulSoup(response,"lxml")
                        adminRegion_glb = datasoup.find('div',id = "region-nav").find('a',class_="cur").text.encode('utf-8')
                        storeList = datasoup.find_all('div',class_="txt")
                        for store in storeList:
                            getStoreDetailInfo(store)
                    else:
		        if len(url) != 0 :
                            resList.append(line)
            if len(resList) != 0:
                with open(filename,'w') as fw:
                    for item in resList:
                        fw.write('%s\n'%item)
            else:
                os.remove(filename)
        
                    

if __name__=="__main__":
    start = time.clock()  #程序开始时间
    filename ="cityList.txt"
    cityDict = loadCityList(filename)
    #print cityDict
    for cityNm in cityDict.keys():
        city = cityDict[cityNm]
        url = 'http://t.dianping.com/list/'+str(city)+'-category_1'
        getRegionFoodType(url)
   # oriFailedPageReCrawl()
    #singlFailedURLReCrawl()
    elapsed = str(time.clock()-start)
    print u"\n爬取结束,所花时间%s,请按任意键退出..."%elapsed
    raw_input()
