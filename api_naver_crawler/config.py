# naver 검색 API 
client_id = 'mED4D4EUjgsTr2u8cXt8'
client_secret = 'MGbfmUsCI4'

# KRX API 설정
krx_api_key = "563557F197FE48C4A7BE975404CAD25BDC2771B6"  # 제공해주신 KRX API 키

# 분석 설정
ANALYSIS_CONFIG = {
    # 분석 기간 설정
    'START_DATE': '2015-01-01',
    'END_DATE': '2024-12-31',
    
    # API 호출 제한 설정
    'MAX_ARTICLES_PER_COMPANY': 1000,  # 기업당 최대 기사 수
    'MAX_VARIATIONS_PER_COMPANY': 5,   # 기업당 검색 변형 최대 개수
    'REQUEST_DELAY': 0.1,              # API 요청 간 지연시간 (초)
    
    # 본문 추출 설정
    'EXTRACT_FULL_CONTENT': True,      # 기사 본문 전체 추출 여부
    'MAX_CONTENT_LENGTH': 5000,        # 본문 최대 길이
    
    # 키워드 매칭 설정
    'MIN_KEYWORD_LENGTH': 2,           # 최소 키워드 길이
    'USE_WORD_BOUNDARY': True,         # 단어 경계 고려 여부
}