import requests
import pandas as pd
import os
import re
import time
import asyncio
import aiohttp
import sqlite3
from datetime import datetime
from tqdm.asyncio import tqdm as async_tqdm
from tqdm import tqdm
import logging
from typing import Dict, List, Set, Tuple, Optional
from bs4 import BeautifulSoup
from pykrx import stock
import config

# --- 로깅 설정 (수정됨 ) ---
# 'asctime' 뒤의 공백 제거
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- 데이터베이스 관리 클래스 ---
class DatabaseManager:
    """SQLite DB를 사용하여 처리 상태와 결과를 관리합니다."""
    def __init__(self, db_path='news_analysis_cache.db'):
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path)
        self._initialize_db()

    def _initialize_db(self):
        """데이터베이스 테이블을 초기화합니다."""
        with self.conn:
            self.conn.execute('''
                CREATE TABLE IF NOT EXISTS companies (
                    ticker TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    market TEXT,
                    status TEXT DEFAULT 'pending' CHECK(status IN ('pending', 'completed', 'failed'))
                )
            ''')
            self.conn.execute('''
                CREATE TABLE IF NOT EXISTS articles (
                    link TEXT PRIMARY KEY,
                    ticker TEXT,
                    pub_date TEXT,
                    title TEXT,
                    description TEXT,
                    full_content TEXT,
                    FOREIGN KEY(ticker) REFERENCES companies(ticker)
                )
            ''')
            self.conn.execute('''
                CREATE TABLE IF NOT EXISTS keyword_matches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    link TEXT,
                    category TEXT,
                    keyword TEXT,
                    FOREIGN KEY(link) REFERENCES articles(link)
                )
            ''')

    def register_companies(self, companies_df: pd.DataFrame):
        """DB에 분석 대상 기업 목록을 등록합니다."""
        # DataFrame의 컬럼명을 DB 스키마에 맞게 변경
        companies_df.rename(columns={'종목코드': 'ticker', '종목명': 'name', '시장구분': 'market'}, inplace=True)
        
        with self.conn:
            # to_sql에 전달할 컬럼만 선택
            df_to_insert = companies_df[['ticker', 'name', 'market']]
            df_to_insert.to_sql('companies', self.conn, if_exists='append', index=False, 
                                chunksize=1000, method=self._upsert_company)
        logger.info(f"{len(companies_df)}개 기업이 DB에 등록/업데이트되었습니다.")

    def _upsert_company(self, table, conn, keys, data_iter):
        """DataFrame의 to_sql을 위한 UPSERT 로직 (수정됨)"""
        # conn은 실제로는 cursor 객체이므로 그대로 사용
        cursor = conn
        for data in data_iter:
            row = dict(zip(keys, data))
            cursor.execute(
                "INSERT INTO companies (ticker, name, market) VALUES (?, ?, ?) ON CONFLICT(ticker) DO NOTHING",
                (row['ticker'], row['name'], row['market'])
            )

    def get_pending_companies(self) -> List[Tuple[str, str]]:
        """처리되지 않은 기업 목록을 가져옵니다."""
        with self.conn:
            cursor = self.conn.cursor()
            cursor.execute("SELECT ticker, name FROM companies WHERE status = 'pending'")
            return cursor.fetchall()

    def update_company_status(self, ticker: str, status: str):
        """기업의 처리 상태를 업데이트합니다."""
        with self.conn:
            self.conn.execute("UPDATE companies SET status = ? WHERE ticker = ?", (status, ticker))

    def save_articles_and_matches(self, ticker: str, articles: List[Dict]):
        """수집된 기사와 매칭된 키워드를 DB에 저장합니다."""
        article_data = []
        match_data = []
        for art in articles:
            article_data.append((
                art['link'], ticker, art['pubDate'], art['title'], 
                art['description'], art.get('full_content', '')
            ))
            for category, keywords in art.get('matched_categories', {}).items():
                for keyword in keywords:
                    match_data.append((art['link'], category, keyword))
        
        with self.conn:
            self.conn.executemany(
                "INSERT OR IGNORE INTO articles (link, ticker, pub_date, title, description, full_content) VALUES (?, ?, ?, ?, ?, ?)",
                article_data
            )
            self.conn.executemany(
                "INSERT INTO keyword_matches (link, category, keyword) VALUES (?, ?, ?)",
                match_data
            )

    def get_all_results(self) -> pd.DataFrame:
        """DB에서 모든 분석 결과를 종합하여 DataFrame으로 반환합니다."""
        query = """
        SELECT
            c.name AS '종목명',
            c.ticker AS '종목코드',
            c.market AS '시장구분',
            a.pub_date AS '기사날짜',
            a.title AS '기사제목',
            a.link AS '기사링크',
            km.category AS '매칭카테고리',
            km.keyword AS '매칭키워드'
        FROM companies c
        JOIN articles a ON c.ticker = a.ticker
        JOIN keyword_matches km ON a.link = km.link
        WHERE c.status = 'completed'
        """
        return pd.read_sql_query(query, self.conn)

    def close(self):
        self.conn.close()

# --- 비동기 웹 요청 관리 클래스 ---
class AsyncRequestManager:
    """aiohttp를 사용하여 웹 요청을 비동기적으로 관리합니다."""
    def __init__(self, max_concurrent=20, delay=0.1 ):
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.delay = delay

    async def _fetch(self, session: aiohttp.ClientSession, url: str ) -> str:
        """단일 URL에 대한 비동기 GET 요청을 수행합니다."""
        await asyncio.sleep(self.delay)
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        try:
            async with self.semaphore:
                async with session.get(url, headers=headers, timeout=15) as response:
                    if response.status == 200:
                        return await response.text()
                    else:
                        logger.warning(f"본문 수집 실패 (상태 코드: {response.status}): {url}")
                        return ""
        except asyncio.TimeoutError:
            logger.warning(f"본문 수집 시간 초과: {url}")
            return ""
        except Exception as e:
            logger.error(f"본문 수집 중 예외 발생: {url}, 오류: {e}")
            return ""

    async def fetch_all(self, urls: List[str]) -> List[str]:
        """여러 URL에 대해 비동기적으로 콘텐츠를 가져옵니다."""
        async with aiohttp.ClientSession( ) as session:
            tasks = [self._fetch(session, url) for url in urls]
            return await async_tqdm.gather(*tasks, desc="기사 본문 비동기 수집")

# --- 뉴스 분석기 클래스 (최적화 버전) ---
class OptimizedNewsAnalyzer:
    def __init__(self):
        self.CLIENT_ID = config.client_id
        self.CLIENT_SECRET = config.client_secret
        self.BASE_URL = 'https://openapi.naver.com/v1/search/news.json'
        
        self.db_manager = DatabaseManager( )
        self.async_requester = AsyncRequestManager()
        
        # 설정값
        self.START_DATE = '2015-01-01'
        self.END_DATE = '2024-12-31'
        self.MAX_ARTICLES_PER_COMPANY = 500
        self.MAX_VARIATIONS_PER_COMPANY = 3
        self.REQUEST_DELAY = 0.1
        
        # 데이터 자산 키워드
        self.data_asset_keywords = {
            '기본': ['데이터', '데이터자산', '빅데이터', 'AI', '인공지능', '머신러닝', '딥러닝', '알고리즘', '데이터분석', 'DB', '데이터베이스'],
            '인프라': ['클라우드', '데이터센터', '데이터 플랫폼', '데이터 레이크', '데이터 웨어하우스', 'ETL', 'API', '분산저장', '엣지컴퓨팅', '슈퍼컴퓨터', 'GPU', 'NPU'],
            '활용분야': ['자연언어처리', '컴퓨터비전', '추천시스템', '예측모델링', '헬스 데이터', '유전체 데이터', '바이오데이터', 'IoT 데이터', '자율주행 데이터', 'CRM 데이터', 'ERP 데이터', '로그데이터', '사용자 행동 데이터'],
            '무형연계자산': ['소프트웨어 자산', '지식재산권', '디지털 트윈', '메타버스 자산'],
            '신흥영역': ['생성형AI', 'LLM', '합성데이터', '데이터 라벨링', '데이터 거버넌스', '데이터 품질', '데이터 윤리', '데이터 보안', '데이터 프라이버시']
        }
        self.all_keywords_flat = [kw.lower() for sublist in self.data_asset_keywords.values() for kw in sublist]
        self.collected_links_session: Set[str] = set()

    def get_all_listed_companies(self) -> pd.DataFrame:
        """PyKRX를 통해 전체 상장기업 목록을 가져옵니다."""
        logger.info("PyKRX를 통해 전체 상장기업 목록 조회 중...")
        all_companies = []
        today = datetime.now().strftime('%Y%m%d')
        
        for market in ["KOSPI", "KOSDAQ", "KONEX"]:
            logger.info(f"{market} 상장기업 조회 중...")
            try:
                tickers = stock.get_market_ticker_list(market=market, date=today)
                for ticker in tqdm(tickers, desc=f"{market} 기업 정보 수집"):
                    try:
                        name = stock.get_market_ticker_name(ticker)
                        all_companies.append({'종목명': name, '종목코드': ticker, '시장구분': market})
                        time.sleep(0.01)
                    except Exception:
                        continue
            except Exception as e:
                logger.warning(f"{market} 조회 오류: {e}")
        
        df = pd.DataFrame(all_companies)
        logger.info(f"총 {len(df)}개 상장기업 정보 수집 완료")
        return df

    def _clean_html_tags(self, text: str) -> str:
        """HTML 태그 및 특수문자 제거"""
        clean = re.sub('<.*?>', '', text)
        html_entities = {'&lt;': '<', '&gt;': '>', '&amp;': '&', '&quot;': '"', '&#39;': "'", '&nbsp;': ' '}
        for entity, char in html_entities.items():
            clean = clean.replace(entity, char)
        return re.sub(r'\s+', ' ', clean).strip()

    def generate_company_search_variations(self, company_name: str) -> List[str]:
        """기업 검색어 변형 생성"""
        variations = {company_name}
        clean_name = re.sub(r'주식회사$|㈜|주식회사|（주）|\(주\)', '', company_name).strip()
        if clean_name and len(clean_name) >= 2:
            variations.add(clean_name)
        return list(variations)[:self.MAX_VARIATIONS_PER_COMPANY]

    def search_news_for_company(self, company_name: str) -> List[Dict]:
        """특정 기업에 대한 뉴스 기사를 Naver API로 검색합니다."""
        headers = {"X-Naver-Client-Id": self.CLIENT_ID, "X-Naver-Client-Secret": self.CLIENT_SECRET}
        variations = self.generate_company_search_variations(company_name)
        articles = []
        
        for query in variations:
            if len(articles) >= self.MAX_ARTICLES_PER_COMPANY:
                break
            
            for start in range(1, 1001, 100): # Naver API는 최대 1000개까지 지원
                params = {'query': query, 'display': 100, 'start': start, 'sort': 'sim'}
                try:
                    time.sleep(self.REQUEST_DELAY)
                    response = requests.get(self.BASE_URL, headers=headers, params=params, timeout=10)
                    if response.status_code != 200:
                        logger.warning(f"'{query}' API 오류 (코드: {response.status_code})")
                        if response.status_code == 429: time.sleep(1)
                        break
                    
                    data = response.json().get('items', [])
                    if not data:
                        break

                    for item in data:
                        link = item['link']
                        if link in self.collected_links_session:
                            continue
                        
                        try:
                            pub_date = datetime.strptime(item['pubDate'], '%a, %d %b %Y %H:%M:%S +0900')
                        except ValueError:
                            continue # 날짜 형식이 다르면 건너뛰기
                        
                        if not (self.START_DATE <= pub_date.strftime('%Y-%m-%d') <= self.END_DATE):
                            continue

                        articles.append({
                            'pubDate': pub_date.strftime('%Y-%m-%d'),
                            'title': self._clean_html_tags(item['title']),
                            'description': self._clean_html_tags(item['description']),
                            'link': link,
                        })
                        self.collected_links_session.add(link)
                        if len(articles) >= self.MAX_ARTICLES_PER_COMPANY:
                            break
                except requests.RequestException as e:
                    logger.error(f"'{query}' API 요청 오류: {e}")
                    break
        return articles

    def parse_full_content(self, html: str) -> str:
        """HTML에서 기사 본문을 추출합니다."""
        if not html: return ""
        soup = BeautifulSoup(html, 'html.parser')
        selectors = ['#dic_area', '.news_end', '.article_body', '#articletxt', '.content', 'article']
        content_elem = next((soup.select_one(s) for s in selectors if soup.select_one(s)), None)
        
        if content_elem:
            for unwanted in content_elem.select('script, style, .ad, .advertisement, .social, .share, .comment, .footer'):
                unwanted.decompose()
            return re.sub(r'\s+', ' ', content_elem.get_text(strip=True))[:5000]
        return ""

    def match_keywords_in_text(self, text: str) -> Dict[str, List[str]]:
        """주어진 텍스트에서 카테고리별 키워드를 찾습니다."""
        matched = {category: [] for category in self.data_asset_keywords}
        text_lower = text.lower()
        for category, keywords in self.data_asset_keywords.items():
            for keyword in keywords:
                if keyword.lower() in text_lower:
                    matched[category].append(keyword)
        return {k: v for k, v in matched.items() if v}

    async def analyze_single_company(self, ticker: str, name: str):
        """단일 기업에 대한 뉴스 분석을 비동기적으로 수행합니다."""
        try:
            logger.info(f"[{name} ({ticker})] 분석 시작")
            self.collected_links_session.clear()
            
            articles = self.search_news_for_company(name)
            if not articles:
                logger.info(f"[{name}] 관련 뉴스 없음. 처리 완료.")
                self.db_manager.update_company_status(ticker, 'completed')
                return

            pre_filtered_articles = []
            for art in articles:
                text_preview = f"{art['title']} {art['description']}".lower()
                if any(kw in text_preview for kw in self.all_keywords_flat):
                    pre_filtered_articles.append(art)
            
            if not pre_filtered_articles:
                logger.info(f"[{name}] 데이터 자산 관련 키워드를 포함한 기사 없음. 처리 완료.")
                self.db_manager.update_company_status(ticker, 'completed')
                return

            logger.info(f"[{name}] {len(articles)}개 중 {len(pre_filtered_articles)}개 기사 본문 수집 대상.")

            urls_to_fetch = [art['link'] for art in pre_filtered_articles]
            html_contents = await self.async_requester.fetch_all(urls_to_fetch)

            final_articles = []
            for art, html in zip(pre_filtered_articles, html_contents):
                full_content = self.parse_full_content(html)
                art['full_content'] = full_content
                
                full_text = f"{art['title']} {art['description']} {full_content}"
                matched_categories = self.match_keywords_in_text(full_text)
                
                if matched_categories:
                    art['matched_categories'] = matched_categories
                    final_articles.append(art)
            
            if final_articles:
                self.db_manager.save_articles_and_matches(ticker, final_articles)
                logger.info(f"✓ [{name}] 데이터 자산 관련 기사 {len(final_articles)}건 발견 및 저장 완료.")
            else:
                logger.info(f"[{name}] 본문 분석 후 최종적으로 관련 기사 없음.")

            self.db_manager.update_company_status(ticker, 'completed')

        except Exception as e:
            logger.error(f"[{name}] 분석 중 심각한 오류 발생: {e}", exc_info=True)
            self.db_manager.update_company_status(ticker, 'failed')

    async def run_analysis(self):
        """전체 분석 프로세스를 실행합니다."""
        logger.info("DB에서 'pending' 상태의 기업 목록을 가져옵니다.")
        pending_companies = self.db_manager.get_pending_companies()
        
        if not pending_companies:
            logger.info("모든 기업이 이미 처리되었습니다. 새로고침을 원하시면 DB를 초기화하세요.")
            if input("새로 기업 목록을 가져와 DB에 등록할까요? (y/n): ").lower() == 'y':
                companies_df = self.get_all_listed_companies()
                if not companies_df.empty:
                    if input("기존 기업 상태를 모두 'pending'으로 초기화하고 진행할까요? (y/n): ").lower() == 'y':
                        with self.db_manager.conn:
                            self.db_manager.conn.execute("UPDATE companies SET status = 'pending'")
                        logger.info("모든 기업 상태를 'pending'으로 초기화했습니다.")
                    self.db_manager.register_companies(companies_df)
                    pending_companies = self.db_manager.get_pending_companies()
                else:
                    logger.error("기업 목록을 가져오지 못했습니다.")
                    return
            else:
                return

        logger.info(f"총 {len(pending_companies)}개 기업에 대한 분석을 시작합니다.")
        tasks = [self.analyze_single_company(ticker, name) for ticker, name in pending_companies]
        
        for f in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="전체 기업 분석 진행률"):
            await f

        logger.info("모든 기업 분석이 완료되었습니다.")

    def save_results_to_excel(self, filename: str = None):
        """DB에서 최종 결과를 가져와 Excel 파일로 저장합니다."""
        logger.info("DB에서 최종 결과 집계 중...")
        results_df = self.db_manager.get_all_results()
        
        if results_df.empty:
            logger.warning("저장할 분석 결과가 없습니다.")
            return

        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"데이터자산_뉴스분석_결과_{timestamp}.xlsx"

        summary = results_df.groupby(['종목명', '종목코드', '시장구분']).agg(
            총_기사수=('기사링크', 'nunique'),
            매칭_키워드수=('매칭키워드', 'nunique'),
            활용_카테고리수=('매칭카테고리', 'nunique')
        ).reset_index().sort_values('총_기사수', ascending=False)

        detailed_articles = results_df.groupby(['종목명', '기사날짜', '기사제목', '기사링크']).agg(
            매칭_키워드수=('매칭키워드', 'nunique'),
            매칭_카테고리=('매칭카테고리', lambda x: ', '.join(x.unique()))
        ).reset_index().sort_values('매칭_키워드수', ascending=False)

        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            summary.to_excel(writer, sheet_name='기업별_요약', index=False)
            detailed_articles.to_excel(writer, sheet_name='상세_기사목록', index=False)
        
        logger.info(f"결과가 {filename}에 성공적으로 저장되었습니다.")

    def close(self):
        self.db_manager.close()

# --- 메인 실행 함수 ---
async def main():
    analyzer = None
    try:
        analyzer = OptimizedNewsAnalyzer()
        await analyzer.run_analysis()
        analyzer.save_results_to_excel()
    except Exception as e:
        logger.error(f"메인 실행 중 오류 발생: {e}", exc_info=True)
    finally:
        if analyzer:
            analyzer.close()
        logger.info("분석 프로그램 종료.")

if __name__ == "__main__":
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
