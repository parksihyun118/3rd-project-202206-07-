import requests
import scrapy
from scrapy.http import TextResponse
from fake_useragent import UserAgent
import pandas as pd
import re
import pymongo


def get_criticlist(res, scorelist) :
    criticlist = []
    for i in range(1,len(scorelist)+20):
        temp=''
        # critic 중에서 유명한 critic은 anchor태그를 통해서 새로운 링크를 넘어가므로, a/text로 받아왔습니다 .
        try :
            temp = res.xpath(f'//*[@class="reviews critic_reviews"]/li[{i}]//*[@class="review_critic"]/div/a/text()').extract()[0]
        except :
            temp = ''
        if len(temp)<1 :
            try :
                # 반면 유명하지 않은 critic은 anchor태그가 아닌 div태그로 작성되어있어 div/text로 받아왔습니다 .
                temp = res.xpath(f'//*[@class="reviews critic_reviews"]/li[{i}]//*[@class="review_critic"]/div/text()').extract()[0]
            except :
                pass
        if temp!='' :
            criticlist.append(temp)
    return criticlist


def get_scorelist(res) :
    scorelist = res.xpath(
        '//*[@id="main"]/div[5]/div/ol/li/div/div/div/div/div/div[1]/div[1]/div[2]/div/text()'
    ).extract()
    return scorelist


def get_contentlist(res, scorelist):
    contentlist = []
    # score같은 경우에는 모든 사이트에서 오류 없이 제대로 크롤링되었기 때문에 scorelist의 길이와 contentlist의 길이가 같은지를 기준으로 크롤링이 제대로 이루어졌는지 여부를 파악했습니다 .
    for i in range(1, len(scorelist) + 20):
        temp_contentlist = ''
        temp_content = ''
        try:
            temp_contentlist = res.xpath(
                f'//*[@class="body product_reviews"]/ol/li[{i}]//*[@class="review_body"]/text()'
            ).extract()
            temp_content = ' '.join(temp_contentlist).replace('  ', '').replace('\n', '').replace('\r', '')
        except:
            temp_content = ''
        if len(temp_content) == 0:
            pass
        else:
            contentlist.append(temp_content)

    # content가 담겨있는 xpath가 제대로 담기지 않았을 경우에는 다른 방식으로 리뷰를 받아왔습니다 .
    if len(contentlist) < len(scorelist):
        contentlist = []
        temp_contentlist = res.xpath(
            '//*[@class="body product_reviews"]/ol/li//*[@class="review_body"]/text()').extract()
        for temp in temp_contentlist:
            temp = temp.strip()
            contentlist.append(temp)

    return contentlist


def get_gamename(res) :
    gamename = res.xpath('//*[@id="main"]/div[1]/div[2]/a/h1/text()').extract()[0]
    return gamename


def get_platform(tempurl) :
    try :
        platform = tempurl.split('/')[2]
    except :
        platform = ''
    return platform


fakeuser = UserAgent(verify_ssl=False).chrome
header = {'User-Agent':fakeuser}

# 받아올 url 리스트를 미리 크롤링해서 csv파일로 받아왔습니다 .
urldf = pd.read_csv('meta_url.csv')
urllist = urldf['url']
faillist = []


BASE_URL = 'https://www.metacritic.com'

# df는 전체
df = pd.DataFrame(columns = ['game_name','platform', 'critic_name','critic_score', 'critic_content'])
for tempurl in urllist :
    url = BASE_URL+tempurl+'/critic-reviews'
    tempdf = pd.DataFrame(columns =  ['game_name','platform', 'critic_name','critic_score', 'critic_content'])
    platform = get_platform(tempurl)
    try :
        req = requests.get(url, headers=header)
        res = TextResponse(req.url, body = req.text, encoding = 'utf-8')
        gamename = get_gamename(res)
        scorelist = get_scorelist(res)
        contentlist = get_contentlist(res, scorelist)
        criticlist = get_criticlist(res, scorelist)
        tempdf['criticname'] = criticlist
        tempdf['criticscore'] = scorelist
        tempdf['criticcontent'] = contentlist
        tempdf['title'] = gamename
        tempdf['platform'] = platform
        df = pd.concat([df, tempdf])
        print('success'+tempurl)
    except :
        print('ERROR!!!!!!!!'+tempurl)
        faillist.append(tempurl)

df.to_csv('metacrawl_meta_final.csv')

# 실패한 링크는 따로 csv로 링크를 저장해서 디버깅했습니다 .
faildf = pd.DataFrame(columns = ['url'])
faildf['url'] = faillist
faildf.to_csv('metafail.csv')

# df를 MongoDB에 업로드해주었습니다 .
client = pymongo.MongoClient('mongodb://admin2:1111@ec2-54-248-99-240.ap-northeast-1.compute.amazonaws.com')
db = client.metacritic
db.metacritic_meta.insert_many(df.to_dict('records'))