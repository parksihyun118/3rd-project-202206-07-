import requests
import scrapy
from scrapy.http import TextResponse
from fake_useragent import UserAgent
import pandas as pd

# 크롤링할 때 차단당하는 것을 막기위해 fake_useragent를 사용한 무작위 chrome header를 생성했습니다 .
fakeuser = UserAgent(verify_ssl=False).chrome
header = {'User-Agent':fakeuser}

# platform 별로 몇 번의 for문을 돌릴지 계산하기 위해 마지막 페이지 넘버를 받아왔습니다 .
def get_lastpage(res) :
    lastpage = int(res.xpath('//*[@class="page last_page"]/a/text()').extract()[0])
    return lastpage

# 게임 목록 페이지를 넘기면서 해당 페이지에 있는 모든 게임 목록들의 상세페이지 링크를 anchor 태그를 통해 list로 return하는 함수입니다 .
def get_link(res) :
    links = res.xpath(
            '//*[@id="main_content"]/div[1]/div[2]/div/div[1]/div/div/table/tr/td[2]/a/@href'
        ).extract()
    return links

# 메타크리틱 게임목록 URL 분석
BASE_URL = 'https://www.metacritic.com/' # 기본 베이스 url
GAME_URL = 'browse/games' # 영화 등 창작물 여러 종류가 있는데, game을 선택했습니다 .
DATE_URL = '/release-date/available' # 출시일 기준으로 모든 날짜 불러왔습니다 .
PLATFORM_URL = '/pc' # 여기서는 pc를 예로 들었지만 이 부분을 for문을 통해 pc, switch, ios, playstion 4,5로 변환했습니다 .
SORT_URL = '/date?view=condensed' # 정렬 기준을 날짜로 고정했습니다 .
# 평점으로 할 시에는 평점이 계속 바뀌어서 중복크롤링 내지는 누락크롤링될 가능성이 있다고 느꼈습니다 .
PAGE_URL = '&page=' # 페이지
PAGE_NUM = '0' # 페이지넘버를 통해서 이 부분을 for문을 통해 get_lastpage()까지 구할 예정


platformlist = ['/pc', '/switch', '/ios', '/ps4', '/ps5']
linklist = []
faillist = []

for platform in platformlist:

    try:
        url = BASE_URL + GAME_URL + DATE_URL + platform + SORT_URL + PAGE_URL + PAGE_NUM
        req = requests.get(url, headers=header) # fakeuser_agent로 받아온 header를 넣었습니다 .
        res = TextResponse(req.url, body=req.text, encoding='utf-8')
        lastpage = get_lastpage(res) # 새로운 platform의 링크를 받을 때마다, 마지막 페이지 링크를 받아서 for문 범위를 설정했습니다.
    except:
        faillist.append(url)

    for page in range(lastpage):

        try:
            url = BASE_URL + GAME_URL + DATE_URL + platform + SORT_URL + PAGE_URL + str(page)
            req = requests.get(url, headers=header)
            res = TextResponse(req.url, body=req.text, encoding='utf-8')
            link = get_link(res) # list 형태로 받아와서
            linklist += link # 기존 list 에 +=로 취합했습니다 .
        except:
            faillist.append(url)

# url이 담긴 list를 csv파일로 저장해서, 해당 링크 내의 게임정보, 그리고 리뷰들을 크롤링할 때 빠르게 불러올 수 있게 했습니다 .
df = pd.DataFrame(columns = ['url'])
df['url'] = linklist
df.to_csv('platformurl.csv')
faillist # faillist를 확인했는데 빈 리스트라서 굳이 따로 csv로 저장하지 않았습니다 .