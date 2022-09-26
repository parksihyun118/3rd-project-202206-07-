import requests
import scrapy
from scrapy.http import TextResponse
from fake_useragent import UserAgent
import pandas as pd
import re
import pymongo


def get_warning(res) : # 웹페이지 자체가 404 에러로 인해서 접속 안되는 경우를 미리 구별했습니다 .
    try :
        warning = res.xpath('//*[@class="review_top review_top_l"]/p/text()').extract()[0].strip().split(' - ')[0]
    except :
        try :
            warning = res.xpath('//*[@class="module errorcode_module error404_module"]//*[@class="error_code"]/text()').extract()[0]
        except :
            warning = ''
    return warning

def get_platform(tempurl) :
    try :
        platform = tempurl.split('/')[2]
    except :
        platform = ''
    return platform


def get_gamename(res) :
    gamename = res.xpath('//*[@class="product_title"]/a/h1/text()').extract()[0]
    return gamename


def get_userlist(res): # user중에서 현재 활동중인 user는 anchor태그를 통해서 작성 게시글을 볼 수 있었습니다 .
    # 반면 탈퇴하거나 찾을 수 없는 user는 span 태그를 통해 user name이 작성되어있었습니다 .
    # 구조가 다른 만큼 한 번
    userlist = []
    for i in range(1, 101):
        user = ''
        try:
            user = res.xpath(
                f'//*[@class="reviews user_reviews"]/li[{i}]//*[@class="name"]/span/text()').extract()[0]

        except:
            user = ''
        if len(user) < 1:
            try:
                user = res.xpath(
                    f'//*[@class="reviews user_reviews"]/li[{i}]//*[@class="name"]/a/text()'
                ).extract()[0]

            except:
                user = ''
                break

        if user != '':
            userlist.append(user)

    return userlist


def get_scorelist(res) :
    scorelist = res.xpath(
        '//*[@id="main"]/div[5]/div[2]/div/ol/li/div/div/div/div/div/div[1]/div[1]/div[2]/div/text()'
    ).extract()
    return scorelist


def get_contentlist(res, scorelist):
    contentlist = []
    # 누락되는 content가 생기지 않도록 score의 개수보다 조금 더 for문을 돌렸습니다.
    # xpath를 통해 받아온 text는 문단마다 상이한 간주하기 때문에, 동일한 li 태그 안에 있는 text는 하나의 리뷰로 묶었습니다 .

    for i in range(1, len(scorelist) + 20):
        temp_contentlist = ''
        try:
            # 긴 리뷰같은 경우에는 blurb_expanded 클래스 내부에 전체 리뷰가 따로 저장되어있었습니다 .
            temp_contentlist = res.xpath(
                f'//*[@class="reviews user_reviews"]/li[{i}]//*[@class="blurb blurb_expanded"]/text()'
            ).extract()
            temp_content = ' '.join(temp_contentlist)
        except:
            temp_contentlist = ''

        if len(temp_content) < 1:
            # 짧은 리뷰 같은 경우에는 blurb_expanded 클래스에 저장되어 있지 않았기 때문에,
            # temp_content에 빈 문자열이 들어가 있습니다 . 따라서 length가 1 미만인 경우에는 다른 방식으로 문자를 받았습니다 .
            try:
                temp_contentlist = res.xpath(
                    f'//*[@class="reviews user_reviews"]/li[{i}]//*[@class="review_body"]/span/text()').extract()
                temp_content = ' '.join(temp_contentlist)

                if len(temp_content) < 1:
                    try:
                        temp_contentlist = res.xpath(
                            f'//*[@class="reviews user_reviews"]/li[{i}]//*[@class="review_body"]/strong/text()').extract()
                        temp_content = ' '.join(temp_contentlist)
                    except:
                        pass
            except:
                try:
                    temp_contentlist = res.xpath(
                        f'//*[@class="reviews user_reviews"]/li[{i}]//*[@class="review_body"]/strong/text()').extract()
                    temp_content = ' '.join(temp_content)
                except:
                    pass
        if len(temp_content) < 1:
            # 짧은 리뷰 중에서도 strong/text 에 리뷰가 들어가 있는 경우가 있는 반면, strong없이 text 태그에 들어가 있는 경우도 있었습니다 .
            try:
                temp_contentlist = ''
                res.xpath(f'//*[@class="reviews user_reviews"]/li[{i}]//*[@class="review_body"]/text()').extract()[0]
                temp_content = temp_contentlist.strip()
                contentlist.append(temp_content)

            except:
                pass
        else:
            # 해당 번쨰의 리뷰인 temp_content를 conttenlist에 추가해주었습니다 .
            contentlist.append(temp_content)

    return contentlist


BASE_URL = 'https://www.metacritic.com'

fakeuser = UserAgent(verify_ssl=False).chrome
header = {'User-Agent':fakeuser}

urldf = pd.read_csv('user_url.csv')

urllist = urldf['url'] # 실제로 크롤링 할 때는 urldf를 iloc로 slicing하여 5000개씩 총 5개를 한번에 돌렸습니다 .
faillist = []


# 기본 취합할 DataFrame을 작성하고, 이에 따른
df = pd.DataFrame(columns=['gamename', 'platform', 'user', 'score', 'content'])
for link in urllist:
    tempurl = BASE_URL + link + '/user-reviews?page='
    tempdf = pd.DataFrame(columns=['gamename', 'platform', 'user', 'score', 'content'])
    platform = get_platform(link)
    try:
        # 같은 url인데도 크롤링을 계속 진행하다보면 response를 정상적으로 받아오지 못하는 오류가 있었습니다 .
        # 동일한 url 뒤에 /를 입력하면 80% 이상이 해결되었습니다 .
        for i in range(500):
            url = tempurl + str(i)
            print(url)
            req = requests.get(url, headers=header)
            res = TextResponse(req.url, body=req.text, encoding='utf-8')

            if len(res.text) < 1:
                url += '/'
                req = requests.get(url, headers=header)
                res = TextResponse(req.url, body=req.text, encoding='utf-8')

                if len(res.text) < 1:
                    url += '/'
                    req = requests.get(url, headers=header)
                    res = TextResponse(req.url, body=req.text, encoding='utf-8')

                    if len(res.text) < 1:
                        url += '/'
                        req = requests.get(url, headers=header)
                        res = TextResponse(req.url, body=req.text, encoding='utf-8')

            # 받아온 response에서 크롤링을 할 수 없는 경우가 크게 두 가지 있었습니다 .
            # 첫 번쨰는 게임 정보 자체에서 404 에러를 내는 경우입니다 .
            # 두 번째는 마지막장을 넘는 리뷰 페이지라서 리뷰가 없는 경우였습니다 .

            warning = get_warning(res)

            if warning == 'There are no user reviews yet':
                print(url + ' is last page')
                break
            elif warning == '404':
                print(url + ' is 404 ERROR')
                break
            else:
                # 앞에서 언급한 두 가지 오류가 아닌 경우에만 크롤링을 진행하였습니다 .
                try:
                    # temptempdf은 해당 페이지에서 받아오는 리뷰 정보를 담은 dataframe입니다 .
                    # tempdf는 해당 게임의 모든 리뷰를 취합한 dataframe입니다 .
                    # df는 모든 게임의 리뷰를 취합한 dataframe입니다 .
                    temptempdf = pd.DataFrame(columns=['gamename', 'platform', 'user', 'score', 'content'])
                    gamename = get_gamename(res)
                    userlist = get_userlist(res)
                    scorelist = get_scorelist(res)
                    contentlist = get_contentlist(res, scorelist)
                    temptempdf['user'] = userlist
                    temptempdf['score'] = scorelist
                    temptempdf['content'] = contentlist
                    temptempdf['gamename'] = gamename
                    temptempdf['platform'] = platform
                    tempdf = pd.concat([tempdf, temptempdf], ignore_index='True')
                    faillist.append(url)
                    print('ERROR!!!!!!!!!!!!!!' + url)
                except :
                    pass
        df = pd.concat([df, tempdf], ignore_index='True')
        print(f'successfully csv saved {gamename}')
    except:
        pass
    try:
        # 데이터 양이 워낙 커서 해당 게임의 리뷰 정보를 오류날 수 있을까봐 게임 이름별로 csv파일을 만들어서 관리했습니다 .
        gamename = re.sub(r"[^a-zA-Z0-9]", "", gamename)
        tempdf.to_csv(f'C:/Users/SEOKWON/mc_python/gameproject_practice/data/user/{gamename}.csv')
    except:
        print('ERROR!!!!!!!!!!!!!!!' + link)


df.to_csv('metacrawl_20_final_01.csv')

faildf = pd.DataFrame(columns =  ['url'])
faildf['url'] = faillist
# faillist는 csv로 만들어서 다시 디버깅해서 csv파일에 취합했다 .
faildf.to_csv('userfail1.csv')

# df는 AWS 상의 mongoDB에 업로드했습니다 .
client = pymongo.MongoClient('mongodb://admin2:1111@ec2-54-248-99-240.ap-northeast-1.compute.amazonaws.com')
db = client.metacritic
db.metacritic_user.insert_many(df.to_dict('records'))