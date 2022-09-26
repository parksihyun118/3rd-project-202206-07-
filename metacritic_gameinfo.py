import requests
import scrapy
from scrapy.http import TextResponse
from fake_useragent import UserAgent
import pandas as pd
import pymongo


def get_title(res) : # 제목
    try :
        title = res.xpath('//*[@class="product_title"]/a/h1/text()').extract()[0]
    except :
        title = ''
    return title

def get_platform(tempurl) : # platform 같은 경우에는 따로 페이지에서 받아올 필요 없이, url에 다 표기가 되어있어서 그대로 사용했습니다 .
    try :
        platform = tempurl.split('/')[2]
    except :
        platform = ''
    return platform

def get_releasedate(res) : # 출시일
    try :
        releasedate = res.xpath('//*[@class="summary_detail release_data"]/span[2]/text()').extract()[0]
    except :
        releasedate = ''
    return releasedate

def get_metascore(res) : # 메타스코어는 비평가들의 평점인데 최소 4명 이상의 비평가가 평점을 남겨야 표기가 됩니다 .
    # 따라서 아예 평점이 0개인 경우, 1명이상 4명 미만인 경우, 그리고 4명 이상인 경우를 구분했습니다 .
    # 단순 구분을 넘어서 평점을 메긴 평론가의 수에따라 페이지의 구조 (xpath) 구조가 달라졌기 떄문에 try-except 문도 사용했습니다 .
    try :
        metascore = res.xpath('//*[@class="score_summary metascore_summary"]//*[@class="metascore_anchor"]/span/text()').extract()[0]
    except :
        try :
            metascore = res.xpath('//*[@itemprop="ratingValue"]/text()').extract()[0]
        except :
            metascore = ''
    return metascore


def get_metareviews(res):
    try:
        metareviews = res.xpath(
            '//*[@class="score_summary metascore_summary"]/div/div[2]/p/span[2]/a/span/text()'
        ).extract()[0].strip()

    except:
        metareviews = ''
    return metareviews

def get_userscore(res) :
    try :
        userscore = res.xpath(
            '//*[@class="userscore_wrap feature_userscore"]//*[@class="metascore_anchor"]/div/text()'
        ).extract()[0]
    except :
        userscore = ''
    return userscore

def get_userreviews(res) :
    try :
        userreviews = res.xpath(
            '//*[@class="score_summary"]//*[@class="summary"]/p/span[2]/a/text()'
        ).extract()[0].replace('Ratings', '').strip()
    except :
        userreviews = ''
    return userreviews


def get_summary(res) : # summary같은 경우에는 span태그가 여러개로 구성된 경우도 있었고, 페이지의 xpath가 게임마다 조금 다른 경우도 있었습니다 .
    # 여러 개로 구성된 경우같은경우에는 첫 번째 문단만 가져오는 경우가 생겼습니다 .
    # 따라서 여러 개의 문단인 경우와 단일문단인 경우를 구분해었으며,
    # try - except 문으로는 빈 summary를 return한 경우에 오류발생을 못시켰습니다 .
    # 따라서 summary의 length 가 비정상적으로 짧은 경우에는 페이지의 구조가 다르다고 판단하여 2안을 사용해서 크롤링했습니다 .
    try :
        temp_summary = res.xpath('//*[@class="summary_detail product_summary"]/span[2]/span/text()').extract()
        summary = ''.join(temp_summary)
    except :
        summary = ''
    if len(summary)<3 :
        try :
            temp_summary = res.xpath('//*[@class="summary_detail product_summary"]/span[2]/span/span[2]/text()').extract()
            summary = ''.join(temp_summary)
        except :
            summary = ''
    return summary


def get_developer(res) : # 제작사같은 경우에도 유명한 제작사는 anchor 태그를 통해 제작사 정보로 넘어갔습니다 .
    # 그러나 비유명 제작사같은 경우에는 span태그를 통해서 소개하고 있었습니다 .
    # 따라서 anchor태그로 try해본 다음에, anchor 태그를 찾을 수 없으면 비유명 제작사라고 판단, span태그로 찾아주었습니다 .
    try :
        developer = res.xpath('//*[@class="summary_detail developer"]/span[2]/a/text()').extract()[0]
    except :
        try :
            developer = res.xpath('//*[@class="summary_detail developer"]/span[2]/span/text()').extract()[0]
        except :
            developer =''
    return developer

def get_genre(res) :
    # 장르 하나하나마다 span태그가 따로 결정되어있었기 때문에, list를 쪼개서 쉼표로 구분한 하나의 문자열로 합쳐주었습니다 .
    try :
        temp_genre = res.xpath(
            '//*[@class="summary_detail product_genre"]/span/text()').extract()[1:]
        genre = ', '.join(temp_genre)
    except :
        genre = ''
    return genre

def get_players(res) :
    try :
        players = res.xpath('//*[@class="summary_detail product_players"]/span[2]/text()').extract()[0]
    except :
        players = ''
    return players


def get_age(res) :
    try :
        age = res.xpath(
        '//*[@class="summary_detail product_rating"]//*[@class="data"]/text()').extract()[0]
    except :
        age = ''
    return age


fakeuser = UserAgent(verify_ssl=False).chrome
header = {'User-Agent':fakeuser}

# gameurl.py를 통해서 저장한 게임 상세 페이지 list가 담긴 platformurl.csv를 불러왔습니다 .
urldf = pd.read_csv('platformurl.csv')
urllist = urldf['url']
faillist = []


BASE_URL = 'https://www.metacritic.com'

# 먼저 빈 list들을 만들고 각각 게임의 정보를 받아온 뒤 한번에 dataframe으로 취합해주었습니다 .
title_list = []
platform_list = []
releasedate_list = []
metascore_list = []
metareviews_list = []
userscore_list = []
userreviews_list = []
summary_list = []
developer_list = []
genre_list = []
players_list = []
age_list = []

for tempurl in urllist :
    try :
        url = BASE_URL+tempurl # urllist 자체가 전체 url이 아니라 BASE_URL 뒤에 붙어있어야 하는 형태였습니다 .
        req = requests.get(url, headers=header)
        res = TextResponse(req.url, body = req.text, encoding = 'utf-8')
        # 받아온 response를 통해서 title, platform을 비롯한 12개의 정보를 list에 담았습니다 .
        title_list.append(get_title(res))
        platform_list.append(get_platform(tempurl))
        releasedate_list.append(get_releasedate(res))
        metascore_list.append(get_metascore(res))
        metareviews_list.append(get_metareviews(res))
        userscore_list.append(get_userscore(res))
        userreviews_list.append(get_userreviews(res))
        summary_list.append(get_summary(res))
        developer_list.append(get_developer(res))
        genre_list.append(get_genre(res))
        players_list.append(get_players(res))
        age_list.append(get_age(res))
        print('SUCCESS'+tempurl)
    except :
        # 정상적으로 list에 게임정보를 담지 못한 url은 따로 list만들어서
        faillist.append(tempurl)
        print('ERROR'+tempurl)


# 받아온 11개의 list를 dataframe으로 변환했습니다 .
df = pd.DataFrame(columns = ['title', 'platform', 'releasedate', 'metascore', 'metareviews',
                             'userscore', 'userreviews', 'summary', 'developer', 'genre', 'players', 'age'])
df['title'] = title_list
df['platform'] = platform_list
df['releasedate'] = releasedate_list
df['metascore'] = metascore_list
df['metareviews'] = metareviews_list
df['userscore'] = userscore_list
df['userreviews'] = userreviews_list
df['summary'] = summary_list
df['developer'] = developer_list
df['genre'] = genre_list
df['players'] = players_list
df['age'] = age_list
# 받아온 dataframe을 중간저장하는 느낌으로 csv파일로 저장했습니다 .
df.to_csv('metacrawl_gameinfo_final_01.csv')

# 실패한 게임들의 url을 따로 모아서 csv파일로 저장해서 디버그를 실시했습니다 .
faildf = pd.DataFrame(columns =  ['url'])
faildf['url'] = faillist
faildf.to_csv('infofail.csv')

# df를 AWS 상의 MongoDB에 업로드해주었습니다 .
client = pymongo.MongoClient('mongodb://admin2:1111@ec2-54-248-99-240.ap-northeast-1.compute.amazonaws.com')
db = client.metacritic

# MongoDB에 dataframe
db.metacritic_info.insert_many(df.to_dict('records'))