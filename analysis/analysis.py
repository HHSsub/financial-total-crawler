import sqlite3
import pandas as pd
from datetime import datetime
import os
from sentiment_analyzer import ScalableSentimentAnalyzer  # 새로 만든 모듈 import

DB_PATH = os.path.join(os.path.dirname(__file__), 'news_fulltext_extract_20250915_171004.db')

def ensure_sentiment_columns(conn):
    cursor = conn.cursor()
    try:
        cursor.execute("ALTER TABLE articles ADD COLUMN sentiment_positive REAL DEFAULT NULL")
        cursor.execute("ALTER TABLE articles ADD COLUMN sentiment_negative REAL DEFAULT NULL")
        cursor.execute("ALTER TABLE articles ADD COLUMN sentiment_date TEXT DEFAULT NULL")
        conn.commit()
    except sqlite3.OperationalError:
        # 컬럼이 이미 있을 경우 무시
        pass

def fetch_articles_to_analyze(conn):
    query = """
    SELECT a.link, c.name, c.ticker, a.pub_date, a.title, a.full_content
    FROM articles a
    JOIN companies c ON a.ticker = c.ticker
    JOIN (
        SELECT link, COUNT(DISTINCT keyword) AS keyword_count
        FROM keyword_matches
        GROUP BY link
        HAVING keyword_count >= 3
    ) km ON a.link = km.link
    WHERE a.full_content IS NOT NULL AND a.full_content != ''
    AND (a.sentiment_positive IS NULL OR a.sentiment_negative IS NULL)
    """
    return pd.read_sql_query(query, conn)

def update_sentiment_result(conn, link, pos_prob, neg_prob):
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE articles SET sentiment_positive=?, sentiment_negative=?, sentiment_date=?
        WHERE link=?
    """, (pos_prob, neg_prob, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), link))
    conn.commit()

def main():
    conn = sqlite3.connect(DB_PATH)
    ensure_sentiment_columns(conn)

    # 감성분석기 객체 생성 및 모델 로드
    analyzer = ScalableSentimentAnalyzer()
    analyzer.load_all_models()

    articles = fetch_articles_to_analyze(conn)
    print(f"분석 대상 기사 수: {len(articles)}")

    batch_size = 20
    for start_idx in range(0, len(articles), batch_size):
        batch = articles.iloc[start_idx:start_idx + batch_size]
        for _, row in batch.iterrows():
            full_text = row['full_content']
            res = analyzer.analyze_sentiment(full_text)
            if 'probabilities' in res:
                pos = res['probabilities']['positive']
                neg = res['probabilities']['negative']
                update_sentiment_result(conn, row['link'], pos, neg)
                print(f"분석 완료: 링크 {row['link']} => 긍정 {pos:.4f}, 부정 {neg:.4f}")
            else:
                print(f"분석 실패: 링크 {row['link']}")
        print(f"{start_idx + len(batch)} / {len(articles)} 완료")

    # 최종 결과 CSV 저장
    df_all = pd.read_sql_query("""
    SELECT c.name AS 종목명, c.ticker AS 종목코드, a.pub_date AS 기사날짜,
           a.title AS 기사제목, a.link AS 기사링크, a.full_content AS 본문,
           a.sentiment_positive AS 긍정확률, a.sentiment_negative AS 부정확률,
           a.sentiment_date AS 분석일시
    FROM articles a
    JOIN companies c ON a.ticker = c.ticker
    WHERE a.sentiment_positive IS NOT NULL
    ORDER BY a.pub_date DESC
    """, conn)
    export_filename = f"news_sentiment_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df_all.to_csv(export_filename, index=False, encoding='utf-8-sig')
    print(f"CSV 파일로 저장 완료: {export_filename}")

    conn.close()

if __name__ == "__main__":
    main()
