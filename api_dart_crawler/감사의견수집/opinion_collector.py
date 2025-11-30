# opinion_collector.py
# ⭐ 중단 지점부터 자동 재개 버전

import os
import time
import requests
import pandas as pd
from tqdm import tqdm
from datetime import datetime
import zipfile
import io
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import sqlite3
import sys
import functools

# ⭐ 실시간 로깅
sys.stdout = open('output.log', 'a', buffering=1, encoding='utf-8')
sys.stderr = sys.stdout
print = functools.partial(print, flush=True)

# ======================= 설정 =======================
API_KEY = '71b71bdff252cfaa4db039cab02d4284fc3b9f6b'
BASE_SAVE_DIR = r"C:\Users\User\Downloads\Downloads\financial-total-crawler\audit_opinion_data"
os.makedirs(BASE_SAVE_DIR, exist_ok=True)

# ⭐ SQLite DB 경로
DB_PATH = os.path.join(BASE_SAVE_DIR, "dart_collection.db")

DEBUG_MODE = False

MAX_WORKERS = 5
REQUEST_DELAY = 0.3
BATCH_SIZE = 100
BATCH_DELAY = 30

request_lock = threading.Lock()
last_request_time = [time.time()]

def rate_limited_request(func, *args, **kwargs):
    with request_lock:
        elapsed = time.time() - last_request_time[0]
        if elapsed < REQUEST_DELAY:
            time.sleep(REQUEST_DELAY - elapsed)
        result = func(*args, **kwargs)
        last_request_time[0] = time.time()
        return result

current_year = datetime.now().year
start_year = current_year - 10
end_year = current_year

print(f"수집 기간: {start_year}년 ~ {end_year}년")
print(f"⚡ 자동 재개 모드: {MAX_WORKERS}개 동시, DB 자동 저장")

# ======================= API 엔드포인트 =======================
API_CORP_CODE = "https://opendart.fss.or.kr/api/corpCode.xml"
API_LIST = "https://opendart.fss.or.kr/api/list.json"
API_FNLTT = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
API_DIVIDEND = "https://opendart.fss.or.kr/api/alotMatter.json"
API_STOCK = "https://opendart.fss.or.kr/api/stockTotqySttus.json"
API_EXECUTIVE = "https://opendart.fss.or.kr/api/exctvSttus.json"
API_AUDIT = "https://opendart.fss.or.kr/api/accnutAdtorNmNdAdtOpinion.json"

REPORT_DEADLINES = {
    '11011': {'name': '사업보고서', 'days': 90, 'month': 3, 'day': 31},
    '11012': {'name': '반기보고서', 'days': 45, 'month': 8, 'day': 14},
    '11013': {'name': '1분기보고서', 'days': 45, 'month': 5, 'day': 15},
    '11014': {'name': '3분기보고서', 'days': 45, 'month': 11, 'day': 14}
}

TARGET_COMPANIES = """AK홀딩스
BF랩스
BGF에코머티리얼즈
CJ CGV
CJ ENM
CJ대한통운
CJ씨푸드
DL이앤씨
DS단석
EG
FSN
GKL
GS리테일
HB인베스트먼트
HD현대
HD현대마린솔루션
HD현대마린엔진
HD현대에너지솔루션
HD현대일렉트릭
HD현대중공업
HLB파나진
HS애드
HS효성
IBKS제21호스팩
IBKS제22호스팩
IBKS제24호스팩
JW생명과학
KBI메탈
KB손해보험
KB제27호스팩
KB제31호스팩
KC그린홀딩스
KC코트렐
KH 건설
KH 미래물산
KH 필룩스
KISCO홀딩스
KR모터스
KSS해운
KTis
KT지니뮤직
KX하이텍
LB세미콘
LG씨엔에스
LG유플러스
LG전자
LIG넥스원
LK삼양
LS마린솔루션
LS머트리얼즈
LS에코에너지
LX인터내셔널
M83
MH에탄올
NAVER
NHN KCP
NH올원리츠
NICE
NICE평가정보
NPX
PKC
RFHIC
RF머트리얼즈
SGC E&C
SGC에너지
SJG세종
SKAI
SK네트웍스
SK바이오사이언스
SK스퀘어
SK증권
SK증권제11호스팩
SM C&C
SPC삼립
THE E&M
TP
강원랜드
강원에너지
경남제약
경농
경보제약
계룡건설산업
계양전기
고스트스튜디오
고영
공구우먼
교보12호기업인수목적
교보17호스팩
국도화학
국민은행
국보
국영지앤엠
국일신동
국일제지
그린리소스
그린생명과학
글로벌에스엠
글로벌텍스프리
글로본
금호건설
금호타이어
기가레인
나노씨엠에스
나노캠텍
나노팀
나라셀라
나라엠앤디
나우IB
나이스정보통신
남성
남해화학
내츄럴엔도텍
네이처셀
네패스
넥스턴바이오
넥스트칩
넥스틴
넥스틸
넷마블
노블엠앤비
누리플랜
뉴로메카
뉴온
다날
다우기술
다원넥스뷰
대동스틸
대림통상
대산F&B
대성에너지
대우건설
대웅
대웅제약
대원제약
대유에이텍
대주산업
대한전선
덕산네오룩스
덕산하이메탈
덕신이피씨
데이원컴퍼니
덴티움
동국S&C
동국산업
동국생명과학
동국홀딩스
동아엘텍
동아타이어공업
동양이엔피
동인기연
동일기연
동진쎄미켐
두산로보틱스
두올
듀켐바이오
드래곤플라이
드림라인
디비금융스팩12호
디비금융제13호스팩
디앤디파마텍
디어유
디에이피
디와이피엔에프
딜리
딥노이드
라메디텍
라온시큐어
라온테크
라이콤
레뷰코퍼레이션
로보스타
롯데손해보험
롯데칠성음료
롯데케미칼
롯데하이마트
루미르
리가켐바이오
리더스코스메틱
리파인
매일유업
머큐리
메가스터디교육
메드팩토
메디아나
메디앙스
메디톡스
메디포스트
메리츠화재해상보험
메타케어
명문제약
모니터랩
모델솔루션
모베이스전자
모코엠시스
모티브링크
무궁화인포메이션테크놀로지
미래아이앤지
미래에셋비전스팩4호
미래에셋생명
미래에셋증권
미투온
미트박스
바이브컴퍼니
바이오솔루션
바이젠셀
바텍
백금T&A
밸로프
뱅크웨어글로벌
버넥트
베노티앤알
벡트
보라티알
보령
블루엠텍
블루콤
비디아이
비아이매트릭스
비엔케이제2호스팩
비유테크놀러지
비즈니스온커뮤니케이션
비케이홀딩스
비투엔
비트컴퓨터
삐아
사람인
사이냅소프트
사조동아원
사조씨푸드
사피엔반도체
산돌
삼기에너지솔루션즈
삼성FN리츠
삼성SDI
삼성기업인수목적6호
삼성물산
삼성바이오로직스
삼성에스디에스
삼성전자
삼성카드
삼성화재해상보험
삼양엔씨켐
삼영무역
삼영이엔씨
삼일씨엔에스
삼일제약
삼정펄프
삼진
삼표시멘트
삼호개발
삼화왕관
상상인제4호스팩
상상인증권
상신이디피
상지건설
새로닉스
샌즈랩
샤페론
서산
서연탑메탈
서울보증보험
서진시스템
선진뷰티사이언스
성안머티리얼스
성호전자
세니젠
세림B&G
세명전기
세방전지
세아메카닉스
세아제강지주
세원물산
세이브존I&C
셀레믹스
셀레스트라
셀바이오휴먼텍
소프트캠프
수성웹툰
스타플렉스
스튜디오삼익
스틱인베스트먼트
시스웍
시큐레터
시큐브
시프트업
신도리코
신라교역
신라섬유
신라에스지
신성델타테크
신성이엔지
신세계푸드
신스틸
신시웨이
신영스팩10호
신일전자
신진에스엠
신한제14호스팩
신한제15호스팩
신한카드
신흥
심텍
싸이버원
싸이토젠
쌍방울
써니전자
쎄노텍
쏘닉스
쏠리드
씨싸이트
씨앤지하이테크
씨에스베어링
씨에스윈드
아모그린텍
아모레퍼시픽
아모센스
아미노로직스
아센디오
아스테라시스
아시아경제
아시아나항공
아이빔테크놀로지
아이스크림미디어
아이씨에이치
아이언디바이스
아이에스티이
아이에이치큐
아이지넷
아이패밀리에스씨
아주스틸
아진전자부품
아진카인텍
알로이스
알엔투테크놀로지
알톤
알티캐스트
압타바이오
앤씨앤
에스디생명공학
에스비비테크
에스에너지
에스에프씨
에스에프에이
에스엠벡셀
에스엠씨지
에스와이스틸텍
에스제이엠홀딩스
에스지헬스케어
에스케이브로드밴드
에스퓨얼셀
에어부산
에이디칩스
에이디테크놀로지
에이럭스
에이스테크
에이에스텍
에이엘티
에이유브랜즈
에이직랜드
에이치엠씨제7호스팩
에이치이엠파마
에이텀
에이테크솔루션
에이텍
에이티세미콘
에이팩트
에코바이브
에코바이오
에코아이
에코캡
에코프로머티
에코플라스틱
에프앤가이드
엑셀세라퓨틱스
엑스게이트
엑시콘
엔시스
엔씨소프트
엔에스이엔엠
엔에이치기업인수목적23호
엔에이치스팩30호
엔에프씨
엔투텍
엔피
엘아이에스
엘앤씨바이오
엘앤케이바이오
엘에스일렉트릭
엠디바이스
엠에스오토텍
엠젠솔루션
엠케이전자
엠플러스
엠피씨플러스
엣지파운드리
연우
오공
오르비텍
오름테라퓨틱
오리콤
오브젠
오픈놀
온코크로스
올릭스
옵투스제약
와이엔텍
와이엠텍
와이즈넛
와이즈버즈
우리넷
우리바이오
우리벤처파트너스
우양
우양에이치씨
우전
워트
원익IPS
원익QnC
원익홀딩스
원일특강
원풍물산
웰크론한텍
웰킵스하이텍
웹케시
위메이드맥스
윈스테크넷
유니셈
유비케어
유성티엔에스
유신
유안타제17호스팩
유진스팩10호
유진스팩11호
유투바이오
유틸렉스
윤성에프앤씨
율호
이건산업
이노뎁
이닉스
이상네트웍스
이스트소프트
이아이디
이엔셀
이엔플러스
이엠앤아이
이지케어텍
이큐셀
인바디
인바이오젠
인베니아
인성정보
인천도시가스
인카금융서비스
인텍플러스
인트론바이오
일월지엠엘
일진디스플
일진전기
자이글
자이에스앤디
장원테크
제너셈
제노레이
제놀루션
제우스
제이스코홀딩스
제이시스메디칼
제이씨현시스템
제이앤케이인더스트리
제이앤티씨
제이엔비
제이엠아이
제일약품
제일엠앤에스
제테마
조선내화
조일알미늄
종근당바이오
종근당홀딩스
주성코퍼레이션
중앙백신
지누스
지니너스
지더블유바이텍
지란지교시큐리티
지씨셀
지아이이노베이션
지아이텍
지앤비에스 에코
지에스엔텍
지에스이
지엔코
지엘팜텍
지유온
지투파워
진시스템
진양폴리우레탄
진코스텍
차바이오텍
창해에탄올
체리부로
초록뱀미디어
카이노스메드
카카오
카티스
캔버스엔
캠시스
컨텍
컴투스홀딩스
컴퍼니케이
케어젠
케이만금세기차륜집단유한공사
케이바이오
케이비제21호기업인수목적
케이비제26호기업인수목적
케이비증권
케이비캐피탈
케이사인
케이씨
케이씨씨
케이씨티
케이아이엔엑스
케이알엠
케이엔에스
케이웨더
케이이엠텍
케이카
케이티
케이티스카이라이프
케이티앤지
케이피엠테크
케일럼
켐트로닉스
코닉오토메이션
코람코라이프인프라리츠
코스온
코스텍시스
코어라인소프트
코오롱
콜마비앤에이치
콜마홀딩스
큐라티스
큐로셀
큐로홀딩스
큐리옥스바이오시스템즈
큐알티
크라우드웍스
크래프톤
크레버스
크레오에스지
크루셜텍
크리스에프앤씨
클로봇
키움제11호스팩
키움제6호기업인수목적
키이스트
킵스파마
타이거일렉
탑엔지니어링
태영건설
태웅
태웅로직스
테고사이언스
테라사이언스
테크윙
토박스코리아
토비스
토탈소프트
투비소프트
트윔
티디에스팜
티비에이치글로벌
티씨케이
티에스아이
티엑스알로보틱스
티엔엔터테인먼트
티피씨글로벌
파두
파로스아이바이오
파마리서치
파미셀
파수
파커스
판타지오
팜스코
팬엔터테인먼트
팬젠
펨트론
펩트론
평화홀딩스
포스뱅크
포스코인터내셔널
포톤
푸드나무
퓨런티어
퓨쳐켐
프레스티지바이오로직스
프레스티지바이오파마
프롬바이오
프리엠스
플래스크
피씨디렉트
피아이이
피앤씨테크
피에스텍
피엔에이치테크
피엔티엠에스
피제이메탈
피제이전자
피코그램
픽셀플러스
핀텔
하나26호스팩
하나30호스팩
하나31호스팩
하나32호스팩
하나34호스팩
하나금융지주
하나마이크론
하나머티리얼즈
하우리
하이브
하이비젼시스템
하이퍼코퍼레이션
한국가구
한국비티비
한국전력공사
한국제12호스팩
한국제13호스팩
한국특강
한미반도체
한샘
한세엠케이
한양디지텍
한양이엔지
한온시스템
한울반도체
한울비앤씨
한울소재과학
한익스프레스
한일시멘트
한전기술
한주라이트메탈
한진칼
한켐
한텍
한화
한화리츠
한화에어로스페이스
한화오션
한화플러스제3호기업인수목적
한화플러스제5호스팩
해성에어로보틱스
현대글로비스
현대모비스
현대바이오
현대백화점
현대오토에버
현대자동차
현대지에프홀딩스
현대퓨처넷
현대홈쇼핑
형지엘리트
호전실업
화인베스틸
화인써키트
화일약품
화천기계
효성중공업
후성
휴니드테크놀로지스
휴림에이텍
휴맥스
휴비스
휴스토리
휴온스
흥국에프엔비
흥아해운
희림""".strip().split('\n')

# ======================= DB 초기화 =======================
def init_database():
    """SQLite DB 초기화"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS disclosures (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            기업명 TEXT,
            종목코드 TEXT,
            사업연도 INTEGER,
            보고서코드 TEXT,
            보고서명 TEXT,
            접수번호 TEXT UNIQUE,
            접수일자 TEXT,
            법정기한 TEXT,
            지연일수 INTEGER,
            지연여부 TEXT,
            재무제표_완성도 TEXT,
            재무제표_항목수 INTEGER,
            핵심계정_발견 TEXT,
            배당정보_존재 TEXT,
            주식정보_존재 TEXT,
            임원정보_존재 TEXT,
            감사의견_존재 TEXT,
            감사의견 TEXT,
            감사인 TEXT,
            감사보고서_특기사항 TEXT,
            강조사항 TEXT,
            핵심감사사항 TEXT,
            종합품질점수 TEXT,
            수집시각 TEXT
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS completed_companies (
            기업명 TEXT PRIMARY KEY,
            종목코드 TEXT,
            완료시각 TEXT,
            수집건수 INTEGER
        )
    ''')
    
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_company ON disclosures(기업명)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_rcept ON disclosures(접수번호)')
    
    conn.commit()
    conn.close()
    print(f"✅ DB 초기화 완료: {DB_PATH}")

def is_company_completed(company_name):
    """기업 수집 완료 여부 확인"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM completed_companies WHERE 기업명 = ?', (company_name,))
    result = cursor.fetchone()[0]
    conn.close()
    return result > 0

def save_to_db(df, company_name, stock_code):
    """DB에 저장"""
    conn = sqlite3.connect(DB_PATH)
    
    # 중복 제거
    df['수집시각'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    try:
        df.to_sql('disclosures', conn, if_exists='append', index=False)
        
        # 완료 표시
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO completed_companies 
            (기업명, 종목코드, 완료시각, 수집건수) 
            VALUES (?, ?, ?, ?)
        ''', (company_name, stock_code, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), len(df)))
        
        conn.commit()
        print(f"💾 {company_name}: {len(df)}건 DB 저장 완료")
    except sqlite3.IntegrityError:
        print(f"⚠️ {company_name}: 중복 데이터 스킵")
    finally:
        conn.close()

def get_progress_stats():
    """진행 상황 통계"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('SELECT COUNT(*) FROM completed_companies')
    completed = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM disclosures')
    total_records = cursor.fetchone()[0]
    
    conn.close()
    return completed, total_records

# ======================= 유틸 함수 (기존과 동일) =======================
def fetch_corp_mapping(api_key, max_retries=3):
    params = {'crtfc_key': api_key}
    for attempt in range(max_retries):
        try:
            print(f"🌐 기업 코드 매핑 중... (시도 {attempt + 1}/{max_retries})")
            r = requests.get(API_CORP_CODE, params=params, timeout=120)
            r.raise_for_status()
            
            with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                xml_filename = z.namelist()[0]
                with z.open(xml_filename) as f:
                    root = ET.fromstring(f.read())
            
            corp_mapping = {}
            for corp in root.findall('list'):
                corp_name = corp.find('corp_name').text
                corp_code = corp.find('corp_code').text
                stock_code_node = corp.find('stock_code')
                
                if stock_code_node is not None and stock_code_node.text:
                    stock_code = stock_code_node.text.strip()
                    if stock_code:
                        corp_mapping[corp_name] = {
                            'corp_code': corp_code,
                            'stock_code': stock_code
                        }
            
            print(f"✅ {len(corp_mapping)}개 기업 매핑 완료")
            return corp_mapping
            
        except Exception as e:
            print(f"⚠️ 오류 (시도 {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 10
                print(f"   {wait_time}초 후 재시도...")
                time.sleep(wait_time)
    return {}

def calculate_deadline(bsns_year, reprt_code):
    info = REPORT_DEADLINES.get(reprt_code)
    if not info:
        return None
    try:
        if reprt_code == '11011':
            deadline = datetime(int(bsns_year) + 1, info['month'], info['day'])
        else:
            deadline = datetime(int(bsns_year), info['month'], info['day'])
        return deadline
    except:
        return None

def calculate_delay_days(rcept_dt_str, deadline):
    if not rcept_dt_str or not deadline:
        return None
    try:
        rcept_dt = datetime.strptime(rcept_dt_str, '%Y%m%d')
        delay = (rcept_dt - deadline).days
        return delay if delay > 0 else 0
    except:
        return None

# ======================= API 호출 (기존과 동일) =======================
def fetch_disclosure_list(api_key, corp_code, bsns_year, reprt_code):
    report_name = REPORT_DEADLINES[reprt_code]['name']
    params = {
        'crtfc_key': api_key,
        'corp_code': corp_code,
        'bgn_de': f"{bsns_year}0101",
        'end_de': f"{int(bsns_year)+1}1231",
        'pblntf_ty': 'A',
        'page_count': 100
    }
    
    try:
        r = rate_limited_request(requests.get, API_LIST, params=params, timeout=30)
        r.raise_for_status()
        result = r.json()
        
        if result.get('status') != '000':
            return []
        
        if result.get('list'):
            filtered = [item for item in result['list'] 
                       if report_name in item.get('report_nm', '')]
            return filtered
        
        return []
    except:
        return []

def check_financial_data(api_key, corp_code, bsns_year, reprt_code):
    params = {
        'crtfc_key': api_key,
        'corp_code': corp_code,
        'bsns_year': str(bsns_year),
        'reprt_code': reprt_code,
        'fs_div': 'CFS'
    }
    
    try:
        r = rate_limited_request(requests.get, API_FNLTT, params=params, timeout=30)
        result = r.json()
        
        if result.get('status') == '000' and result.get('list'):
            items = result['list']
            key_accounts = ['자산총계', '부채총계', '자본총계', '매출액', '당기순이익']
            found_accounts = []
            
            for item in items:
                account_nm = item.get('account_nm', '')
                for key in key_accounts:
                    if key in account_nm and item.get('thstrm_amount'):
                        found_accounts.append(key)
                        break
            
            return {
                'has_financial_data': True,
                'total_accounts': len(items),
                'key_accounts_found': len(set(found_accounts)),
                'key_accounts_total': len(key_accounts),
                'completeness_ratio': len(set(found_accounts)) / len(key_accounts)
            }
        
        return {
            'has_financial_data': False,
            'total_accounts': 0,
            'key_accounts_found': 0,
            'key_accounts_total': 5,
            'completeness_ratio': 0.0
        }
    except:
        return {'has_financial_data': False, 'total_accounts': 0, 
                'key_accounts_found': 0, 'key_accounts_total': 5, 'completeness_ratio': 0.0}

def check_dividend_data(api_key, corp_code, bsns_year, reprt_code):
    params = {
        'crtfc_key': api_key,
        'corp_code': corp_code,
        'bsns_year': str(bsns_year),
        'reprt_code': reprt_code
    }
    try:
        r = rate_limited_request(requests.get, API_DIVIDEND, params=params, timeout=30)
        result = r.json()
        return result.get('status') == '000' and bool(result.get('list'))
    except:
        return False

def check_stock_data(api_key, corp_code, bsns_year, reprt_code):
    params = {
        'crtfc_key': api_key,
        'corp_code': corp_code,
        'bsns_year': str(bsns_year),
        'reprt_code': reprt_code
    }
    try:
        r = rate_limited_request(requests.get, API_STOCK, params=params, timeout=30)
        result = r.json()
        return result.get('status') == '000' and bool(result.get('list'))
    except:
        return False

def check_executive_data(api_key, corp_code, bsns_year, reprt_code):
    params = {
        'crtfc_key': api_key,
        'corp_code': corp_code,
        'bsns_year': str(bsns_year),
        'reprt_code': reprt_code
    }
    try:
        r = rate_limited_request(requests.get, API_EXECUTIVE, params=params, timeout=30)
        result = r.json()
        return result.get('status') == '000' and bool(result.get('list'))
    except:
        return False

def check_audit_data(api_key, corp_code, bsns_year, reprt_code):
    params = {
        'crtfc_key': api_key,
        'corp_code': corp_code,
        'bsns_year': str(bsns_year),
        'reprt_code': reprt_code
    }
    
    try:
        r = rate_limited_request(requests.get, API_AUDIT, params=params, timeout=30)
        result = r.json()
        
        if result.get('status') == '000' and result.get('list'):
            items = result['list']
            audit_info = {
                'exists': True,
                'opinion': items[0].get('adt_opinion', ''),
                'auditor': items[0].get('adtor', ''),
                'special_matter': items[0].get('adt_reprt_spcmnt_matter', ''),
                'emphasis_matter': items[0].get('emphs_matter', ''),
                'core_matter': items[0].get('core_adt_matter', '')
            }
            return audit_info
        
        return {
            'exists': False,
            'opinion': '',
            'auditor': '',
            'special_matter': '',
            'emphasis_matter': '',
            'core_matter': ''
        }
    except:
        return {
            'exists': False,
            'opinion': '',
            'auditor': '',
            'special_matter': '',
            'emphasis_matter': '',
            'core_matter': ''
        }

# ======================= 분석 함수 =======================
def analyze_disclosure_quality(api_key, corp_code, corp_name, stock_code):
    rows = []
    consecutive_failures = 0
    FAILURE_THRESHOLD = 5
    
    for year in range(end_year, start_year - 1, -1):
        for reprt_code, info in REPORT_DEADLINES.items():
            
            if consecutive_failures >= FAILURE_THRESHOLD:
                if not rows:
                    return None
                return pd.DataFrame(rows)
            
            disclosure_list = fetch_disclosure_list(api_key, corp_code, year, reprt_code)
            
            if not disclosure_list:
                consecutive_failures += 1
                continue
            
            consecutive_failures = 0
            
            for disclosure in disclosure_list:
                rcept_no = disclosure.get('rcept_no')
                rcept_dt = disclosure.get('rcept_dt')
                
                deadline = calculate_deadline(year, reprt_code)
                delay_days = calculate_delay_days(rcept_dt, deadline)
                
                financial = check_financial_data(api_key, corp_code, year, reprt_code)
                has_dividend = check_dividend_data(api_key, corp_code, year, reprt_code)
                has_stock = check_stock_data(api_key, corp_code, year, reprt_code)
                has_executive = check_executive_data(api_key, corp_code, year, reprt_code)
                audit_info = check_audit_data(api_key, corp_code, year, reprt_code)
                
                quality_components = [
                    financial['has_financial_data'],
                    has_dividend,
                    has_stock,
                    has_executive,
                    audit_info['exists']
                ]
                quality_score = sum(quality_components) / len(quality_components) * 100
                
                row = {
                    '기업명': corp_name,
                    '종목코드': stock_code,
                    '사업연도': year,
                    '보고서코드': reprt_code,
                    '보고서명': info['name'],
                    '접수번호': rcept_no,
                    '접수일자': rcept_dt,
                    '법정기한': deadline.strftime('%Y%m%d') if deadline else '',
                    '지연일수': delay_days,
                    '지연여부': 'Y' if (delay_days and delay_days > 0) else 'N',
                    '재무제표_완성도': f"{financial['completeness_ratio']:.2%}",
                    '재무제표_항목수': financial['total_accounts'],
                    '핵심계정_발견': f"{financial['key_accounts_found']}/{financial['key_accounts_total']}",
                    '배당정보_존재': 'Y' if has_dividend else 'N',
                    '주식정보_존재': 'Y' if has_stock else 'N',
                    '임원정보_존재': 'Y' if has_executive else 'N',
                    '감사의견_존재': 'Y' if audit_info['exists'] else 'N',
                    '감사의견': audit_info['opinion'],
                    '감사인': audit_info['auditor'],
                    '감사보고서_특기사항': audit_info['special_matter'],
                    '강조사항': audit_info['emphasis_matter'],
                    '핵심감사사항': audit_info['core_matter'],
                    '종합품질점수': f"{quality_score:.1f}"
                }
                
                rows.append(row)
    
    if not rows:
        return None
    return pd.DataFrame(rows)

def process_single_company(company_data):
    company_name, corp_info = company_data
    
    # ⭐ 이미 완료된 기업은 스킵
    if is_company_completed(company_name):
        return (company_name, None, 0, True)
    
    try:
        df = analyze_disclosure_quality(
            API_KEY,
            corp_info['corp_code'],
            company_name,
            corp_info['stock_code']
        )
        
        if df is not None and not df.empty:
            # ⭐ 즉시 DB 저장
            save_to_db(df, company_name, corp_info['stock_code'])
            return (company_name, df, len(df), False)
        
        return (company_name, None, 0, False)
    except Exception as e:
        print(f"❌ {company_name} 오류: {e}")
        return (company_name, None, 0, False)

# ======================= 메인 =======================
def main():
    print("🚀 DART 공시 품질 분석기 (자동 재개 + DB 저장)")
    print("="*60)
    
    # ⭐ DB 초기화
    init_database()
    
    # 진행 상황 확인
    completed, total_records = get_progress_stats()
    print(f"📊 현재 진행: {completed}개 기업 완료, {total_records}건 수집됨\n")
    
    time.sleep(5)
    
    corp_mapping = fetch_corp_mapping(API_KEY)
    if not corp_mapping:
        return
    
    target_corp_info = {}
    for company_name in TARGET_COMPANIES:
        company_name = company_name.strip()
        if company_name in corp_mapping:
            target_corp_info[company_name] = corp_mapping[company_name]
    
    print(f"\n수집 대상: {len(target_corp_info)}개 기업")
    print(f"배치 크기: {BATCH_SIZE}개씩, 배치 간 {BATCH_DELAY}초 대기\n")
    
    success_count = 0
    skipped_count = 0
    
    companies_list = list(target_corp_info.items())
    total_batches = (len(companies_list) + BATCH_SIZE - 1) // BATCH_SIZE
    
    for batch_idx in range(total_batches):
        batch_start = batch_idx * BATCH_SIZE
        batch_end = min((batch_idx + 1) * BATCH_SIZE, len(companies_list))
        batch_companies = companies_list[batch_start:batch_end]
        
        print(f"\n📦 배치 {batch_idx + 1}/{total_batches} 처리 중 ({len(batch_companies)}개 기업)")
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_company = {
                executor.submit(process_single_company, company): company[0] 
                for company in batch_companies
            }
            
            with tqdm(total=len(batch_companies), desc=f"배치 {batch_idx + 1}") as pbar:
                for future in as_completed(future_to_company):
                    company_name, df, count, was_skipped = future.result()
                    
                    if was_skipped:
                        skipped_count += 1
                        tqdm.write(f"⏭️ {company_name}: 이미 완료됨")
                    elif df is not None:
                        success_count += 1
                        tqdm.write(f"✅ {company_name}: {count}건 (DB 저장 완료)")
                    else:
                        tqdm.write(f"❌ {company_name}: 실패")
                    
                    pbar.update(1)
        
        if batch_idx < total_batches - 1:
            print(f"\n⏸️  {BATCH_DELAY}초 대기 중...")
            time.sleep(BATCH_DELAY)
    
    # ⭐ CSV 파일 생성 (DB에서)
    print("\n📄 CSV 파일 생성 중...")
    conn = sqlite3.connect(DB_PATH)
    
    final_df = pd.read_sql_query('SELECT * FROM disclosures ORDER BY 수집시각', conn)
    
    if not final_df.empty:
        save_path = os.path.join(BASE_SAVE_DIR, "disclosure_quality_full.csv")
        final_df.to_csv(save_path, index=False, encoding='utf-8-sig')
        
        summary_df = pd.read_sql_query('''
            SELECT 기업명, 종목코드, 완료시각, 수집건수
            FROM completed_companies
            ORDER BY 완료시각
        ''', conn)
        
        summary_path = os.path.join(BASE_SAVE_DIR, "disclosure_quality_summary.csv")
        summary_df.to_csv(summary_path, index=False, encoding='utf-8-sig')
        
        print(f"\n{'='*60}")
        print(f"✅ 완료! 전체 {len(final_df)}건")
        print(f"성공: {success_count}개 | 스킵: {skipped_count}개 | 실패: {len(target_corp_info) - success_count - skipped_count}개")
        print(f"저장: {BASE_SAVE_DIR}")
        print(f"DB: {DB_PATH}")
    else:
        print("\n❌ 수집된 데이터 없음")
    
    conn.close()

if __name__ == "__main__":
    start_time = time.time()
    main()
    elapsed = time.time() - start_time
    print(f"\n총 소요 시간: {elapsed/60:.1f}분")