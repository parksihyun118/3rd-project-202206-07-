import pandas as pd
from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
import pymysql
import re


# 메타크리틱과 스팀의 PC 게임에 관계성을 부여하기 위해 스팀 기준으로 메타크리틱에 appid를 부여했습니다 .
sdf = pd.read_csv('steam_info.csv', index_col = 0).reset_index()
mdf = pd.read_csv('preprocessed_meta_info.csv', index_col = 0).reset_index()

sdf = sdf.drop(columns='index', axis=1)
mdf = mdf.drop(columns='index', axis=1)

# re.sub 을 이용해서 알파벳 대/소문자, 숫자만으로 title을 재구성하였습니다 .
pattern = '[^a-z^A-Z^0-9]'

# title을 list로 받아서 특수문자와 공백을 제거한 뒤 대문자화 해주고, 이를 newtitle이라는 새로운 컬럼으로 넣었습니다 .
sdf['newtitle'] = sdf['title'].apply(lambda x : re.sub(pattern, '', str(x)).upper())
mdf['newtitle'] = mdf['title'].apply(lambda x : re.sub(pattern, '', str(x)).upper())

# sdf(steam dataframe)은 metacritic 에 appid만 부여하는데 이용할 것이므로 필요없는 칼럼을 날려주었습니다 .
tempsdf = sdf.copy()[['newtitle', 'appid']]

# steam에는 pc게임만 저장되어있으므로 metacritic에도 pc게임만 조건을 주어 남겼습니다 .
pcmdf = mdf.copy()[mdf['platform']=='pc']

# 크롤링 도중 중복된 데이터가 들어갔을 수 있으니 전부 중복제거를 했습니다 .
tempsdf = tempsdf.drop_duplicates()
pcmdf = pcmdf.drop_duplicates('newtitle')

merged_df = pd.merge(pcmdf, tempsdf, on='newtitle', how='inner')
merged_df = merged_df.rename(columns={'newtitle': 'full_name'})

# 혹시 몰라서 csv 파일로 백업을 진행했습니다 .
merged_df.to_csv('mergeinfo.csv')

# 기존에 크롤링했던 review들도 전부 merged_df를 기준으로, appid가 부여되지 않은 리뷰는 전부 날린 채 mart에 넣어 모델에 넣어주었습니다 .
user_review = pd.read_csv('metacrawl_user_final.csv')
meta_review = pd.read_csv('metacrawl_meta_final.csv')

user_review = user_review.drop('Unnamed: 0', axis =1 )
meta_review = meta_review.drop('Unnamed: 0', axis =1 )
merged_df = merged_df.drop('Unnamed: 0', axis =1 )

merged_df = merged_df[['title', 'appid']]

pc_user_review = user_review.copy()[user_review['platform']=='pc']
userinner = pd.merge(pc_user_review, merged_df, on='title', how='inner')
userinner.to_csv('meta_user_mart.csv')

pc_meta_review = meta_review.copy()[meta_review['platform']=='pc']
metainner = pd.merge(pc_meta_review, merged_df, on='title', how='inner')
metainner.to_csv('meta_meta_mart.csv')
