# analysis.py
# GPU 최적화된 배치 감성분석 처리기

import sqlite3
import pandas as pd
from datetime import datetime
import os
import time
import logging
from typing import Dict, List
import torch
from sentiment_analyzer import SmartSentimentAnalyzer

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('gpu_sentiment_analysis.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

DB_PATH = os.path.join(os.path.dirname(__file__), 'news_fulltext_extract_20250915_171004.db')


class GPUBatchSentimentProcessor:
    """GPU 최적화된 배치 감성분석 처리기"""
    
    def __init__(self, db_path: str = DB_PATH, gpu_batch_size: int = 16):
        self.db_path = db_path
        self.gpu_batch_size = gpu_batch_size
        self.processed_count = 0
        self.total_count = 0
        
        # GPU 감성분석기 초기화
        try:
            self.analyzer = SmartSentimentAnalyzer(force_gpu=True)
            logger.info("✅ GPU 감성분석기 초기화 완료")
        except RuntimeError as e:
            logger.error(f"❌ GPU 초기화 실패: {e}")
            raise
        
    def ensure_sentiment_columns(self):
        """감성분석 결과 컬럼 추가"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        columns_to_add = [
            ("sentiment_positive", "REAL DEFAULT NULL"),
            ("sentiment_negative", "REAL DEFAULT NULL"), 
            ("sentiment_date", "TEXT DEFAULT NULL"),
            ("sentiment_confidence", "REAL DEFAULT NULL"),
            ("sentiment_score", "REAL DEFAULT NULL"),
            ("detected_company", "TEXT DEFAULT NULL"),
            ("detected_ticker", "TEXT DEFAULT NULL"),
            ("keyword_source", "TEXT DEFAULT NULL"),
            ("analysis_version", "TEXT DEFAULT NULL"),
            ("gpu_device", "TEXT DEFAULT NULL"),
            ("processing_time", "REAL DEFAULT NULL"),
            ("mixed_precision", "INTEGER DEFAULT NULL")
        ]
        
        for column_name, column_def in columns_to_add:
            try:
                cursor.execute(f"ALTER TABLE articles ADD COLUMN {column_name} {column_def}")
                logger.info(f"컬럼 추가: {column_name}")
            except sqlite3.OperationalError as e:
                if "duplicate column name" in str(e).lower():
                    continue
                else:
                    logger.warning(f"컬럼 추가 실패 ({column_name}): {e}")
        
        conn.commit()
        conn.close()
        
    def fetch_articles_to_analyze(self) -> pd.DataFrame:
        """분석할 기사 조회"""
        conn = sqlite3.connect(self.db_path)
        
        query = """
        SELECT DISTINCT
            a.link, 
            c.name as company_name, 
            c.ticker, 
            a.pub_date, 
            a.title, 
            a.full_content,
            a.sentiment_positive,
            a.analysis_version
        FROM articles a
        JOIN companies c ON a.ticker = c.ticker
        JOIN (
            SELECT link, COUNT(DISTINCT keyword) AS keyword_count
            FROM keyword_matches
            GROUP BY link
            HAVING keyword_count >= 3
        ) km ON a.link = km.link
        WHERE a.full_content IS NOT NULL 
        AND TRIM(a.full_content) != ''
        AND LENGTH(a.full_content) > 100
        AND (
            a.sentiment_positive IS NULL 
            OR a.analysis_version != 'gpu_smart_v2.0'
            OR a.analysis_version IS NULL
        )
        ORDER BY a.pub_date DESC
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        logger.info(f"GPU 분석 대상 기사: {len(df)}개")
        return df
    
    def analyze_batch_gpu(self, batch_df: pd.DataFrame) -> List[Dict]:
        """GPU 배치 분석"""
        if len(batch_df) == 0:
            return []
        
        texts = batch_df['full_content'].tolist()
        companies = batch_df['company_name'].tolist()
        tickers = batch_df['ticker'].tolist()
        links = batch_df['link'].tolist()
        
        results = []
        
        try:
            start_time = time.time()
            
            # GPU 배치 분석 실행
            batch_results = self.analyzer.batch_analyze_gpu(texts, companies, tickers)
            
            end_time = time.time()
            batch_time = end_time - start_time
            
            # 결과 매핑
            for i, (link, result) in enumerate(zip(links, batch_results)):
                if 'error' in result:
                    logger.warning(f"분석 실패 - {link}: {result['error']}")
                    continue
                
                processing_time = batch_time / len(batch_results)
                
                result_data = {
                    'link': link,
                    'sentiment_positive': result['probabilities']['positive'],
                    'sentiment_negative': result['probabilities']['negative'],
                    'sentiment_confidence': result['confidence'],
                    'sentiment_score': result['score'],
                    'detected_company': result.get('target_company'),
                    'detected_ticker': result.get('target_ticker'),
                    'keyword_source': result['keyword_analysis']['details'].get('keyword_source', 'unknown'),
                    'analysis_version': 'gpu_smart_v2.0',
                    'gpu_device': str(result.get('gpu_info', {}).get('device', 'unknown')),
                    'processing_time': processing_time,
                    'mixed_precision': 1 if result.get('gpu_info', {}).get('mixed_precision', False) else 0,
                    'sentiment_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                
                results.append(result_data)
                self.processed_count += 1
            
            logger.info(f"GPU 배치 완료: {len(results)}개 성공, 시간: {batch_time:.2f}초")
            
        except Exception as e:
            logger.error(f"GPU 배치 분석 오류: {e}")
            self.analyzer.gpu_manager.clear_gpu_cache()
            
        return results
    
    def update_sentiment_results(self, results: List[Dict]):
        """분석 결과 DB 업데이트"""
        if not results:
            return
            
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        update_query = """
        UPDATE articles SET 
            sentiment_positive = ?,
            sentiment_negative = ?,
            sentiment_confidence = ?,
            sentiment_score = ?,
            detected_company = ?,
            detected_ticker = ?,
            keyword_source = ?,
            analysis_version = ?,
            gpu_device = ?,
            processing_time = ?,
            mixed_precision = ?,
            sentiment_date = ?
        WHERE link = ?
        """
        
        update_data = []
        for result in results:
            if result:
                update_data.append((
                    result['sentiment_positive'],
                    result['sentiment_negative'], 
                    result['sentiment_confidence'],
                    result['sentiment_score'],
                    result['detected_company'],
                    result['detected_ticker'],
                    result['keyword_source'],
                    result['analysis_version'],
                    result['gpu_device'],
                    result['processing_time'],
                    result['mixed_precision'],
                    result['sentiment_date'],
                    result['link']
                ))
        
        if update_data:
            cursor.executemany(update_query, update_data)
            conn.commit()
            logger.info(f"DB 업데이트: {len(update_data)}건")
        
        conn.close()
    
    def run_gpu_analysis(self):
        """GPU 메인 분석 실행"""
        start_time = time.time()
        
        logger.info("🚀 GPU 기반 스마트 감성분석 시작")
        logger.info("=" * 50)
        
        # GPU 체크
        if not torch.cuda.is_available():
            raise RuntimeError("❌ CUDA가 사용 불가능합니다!")
        
        logger.info(f"🔥 GPU: {torch.cuda.get_device_name(0)}")
        logger.info(f"💾 GPU 메모리: {torch.cuda.get_device_properties(0).total_memory / 1024**3:.1f}GB")
        
        # 준비 작업
        self.ensure_sentiment_columns()
        
        # 모델 로드
        logger.info("📦 GPU에 모델 로딩...")
        loaded_models = self.analyzer.load_all_models()
        if not loaded_models:
            raise RuntimeError("❌ GPU 모델 로딩 실패!")
        
        logger.info(f"✅ GPU 로드된 모델: {loaded_models}")
        
        # 분석 대상 기사 조회
        articles_df = self.fetch_articles_to_analyze()
        if len(articles_df) == 0:
            logger.info("분석할 기사가 없습니다.")
            return
            
        self.total_count = len(articles_df)
        logger.info(f"📊 총 {self.total_count}개 기사 GPU 분석 시작")
        
        # GPU 배치별 처리
        for start_idx in range(0, len(articles_df), self.gpu_batch_size):
            batch_df = articles_df.iloc[start_idx:start_idx + self.gpu_batch_size]
            
            batch_num = start_idx // self.gpu_batch_size + 1
            total_batches = (len(articles_df) - 1) // self.gpu_batch_size + 1
            
            logger.info(f"🔄 GPU 배치 {batch_num}/{total_batches} 처리중... ({start_idx+1}-{min(start_idx+len(batch_df), len(articles_df))})")
            
            # GPU 메모리 상태 확인
            if batch_num % 5 == 1:
                self.analyzer.gpu_manager.monitor_gpu_memory()
            
            # GPU 배치 분석
            batch_results = self.analyze_batch_gpu(batch_df)
            
            # 결과 저장
            self.update_sentiment_results(batch_results)
            
            # 진행률 출력
            progress = (self.processed_count / self.total_count) * 100
            logger.info(f"📈 진행률: {self.processed_count}/{self.total_count} ({progress:.1f}%)")
            
            # 주기적 GPU 메모리 정리
            if batch_num % 10 == 0:
                logger.info("🧹 GPU 메모리 정리...")
                self.analyzer.gpu_manager.clear_gpu_cache()
        
        # 최종 결과 내보내기
        self.export_final_results()
        
        end_time = time.time()
        elapsed_time = end_time - start_time
        
        logger.info("=" * 50)
        logger.info("🎉 GPU 분석 완료!")
        logger.info(f"⏰ 총 처리시간: {elapsed_time:.2f}초 ({elapsed_time/60:.1f}분)")
        logger.info(f"📊 처리된 기사: {self.processed_count}개")
        logger.info(f"🚀 GPU 평균속도: {self.processed_count/elapsed_time:.2f}개/초")
        
        # 최종 GPU 상태
        self.analyzer.gpu_manager.monitor_gpu_memory()
    
    def export_final_results(self):
        """GPU 분석 결과 CSV 내보내기"""
        conn = sqlite3.connect(self.db_path)
        
        export_query = """
        SELECT 
            c.name AS 종목명,
            c.ticker AS 종목코드,
            a.pub_date AS 기사날짜,
            a.title AS 기사제목,
            a.link AS 기사링크,
            SUBSTR(a.full_content, 1, 200) || '...' AS 본문요약,
            a.sentiment_positive AS 긍정확률,
            a.sentiment_negative AS 부정확률,
            a.sentiment_confidence AS 신뢰도,
            a.sentiment_score AS 감성점수,
            a.detected_company AS 탐지된기업,
            a.detected_ticker AS 탐지된종목코드,
            a.keyword_source AS 키워드소스,
            a.analysis_version AS 분석버전,
            a.gpu_device AS GPU디바이스,
            a.processing_time AS 처리시간,
            CASE WHEN a.mixed_precision = 1 THEN 'Yes' ELSE 'No' END AS 혼합정밀도,
            a.sentiment_date AS 분석일시
        FROM articles a
        JOIN companies c ON a.ticker = c.ticker
        WHERE a.sentiment_positive IS NOT NULL
        AND a.analysis_version = 'gpu_smart_v2.0'
        ORDER BY a.pub_date DESC, a.sentiment_confidence DESC
        """
        
        df_results = pd.read_sql_query(export_query, conn)
        conn.close()
        
        if len(df_results) > 0:
            export_filename = f"gpu_sentiment_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df_results.to_csv(export_filename, index=False, encoding='utf-8-sig')
            logger.info(f"💾 GPU 결과 CSV 저장: {export_filename} ({len(df_results)}건)")
            
            # GPU 분석 통계 출력
            self.print_gpu_analysis_stats(df_results)
        else:
            logger.warning("내보낼 GPU 결과가 없습니다.")
    
    def print_gpu_analysis_stats(self, df: pd.DataFrame):
        """GPU 분석 결과 통계"""
        logger.info("📊 GPU 분석 결과 통계")
        logger.info(f"   총 GPU 분석 기사: {len(df)}개")
        
        # GPU 성능 통계
        if '처리시간' in df.columns and df['처리시간'].notna().any():
            avg_time = df['처리시간'].mean()
            total_time = df['처리시간'].sum()
            logger.info(f"   평균 처리시간: {avg_time:.4f}초/기사")
            logger.info(f"   총 처리시간: {total_time:.2f}초")
        
        # 혼합 정밀도 사용률
        if '혼합정밀도' in df.columns:
            mixed_precision_count = len(df[df['혼합정밀도'] == 'Yes'])
            mixed_precision_rate = (mixed_precision_count / len(df)) * 100
            logger.info(f"   혼합 정밀도 사용률: {mixed_precision_rate:.1f}%")
        
        # 감성 분포
        positive_articles = len(df[df['감성점수'] > 0])
        negative_articles = len(df[df['감성점수'] < 0])
        neutral_articles = len(df[df['감성점수'] == 0])
        
        logger.info("   감성 분포:")
        logger.info(f"     긍정: {positive_articles}개 ({positive_articles/len(df)*100:.1f}%)")
        logger.info(f"     부정: {negative_articles}개 ({negative_articles/len(df)*100:.1f}%)")
        logger.info(f"     중립: {neutral_articles}개 ({neutral_articles/len(df)*100:.1f}%)")
        
        # 종목별 상위 10개 통계
        if len(df) > 0:
            company_stats = df.groupby('종목명').agg({
                '긍정확률': ['count', 'mean'],
                '부정확률': 'mean',
                '신뢰도': 'mean'
            }).round(4)
            
            logger.info("   상위 10개 종목 통계:")
            for company, stats in company_stats.head(10).iterrows():
                count = stats[('긍정확률', 'count')]
                pos_avg = stats[('긍정확률', 'mean')]
                neg_avg = stats[('부정확률', 'mean')]
                conf_avg = stats[('신뢰도', 'mean')]
                logger.info(f"     {company}: {count}건, 긍정:{pos_avg:.3f}, 부정:{neg_avg:.3f}, 신뢰도:{conf_avg:.3f}")


def main():
    """메인 실행 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description='GPU 기반 스마트 감성분석 실행')
    parser.add_argument('--gpu_batch_size', type=int, default=16, help='GPU 배치 크기 (기본값: 16)')
    parser.add_argument('--db_path', type=str, default=DB_PATH, help='데이터베이스 경로')
    
    args = parser.parse_args()
    
    # GPU 사용 가능성 체크
    if not torch.cuda.is_available():
        logger.error("❌ CUDA가 사용 불가능합니다!")
        logger.error("GPU 드라이버와 CUDA 설치를 확인하세요.")
        return
    
    logger.info(f"🔥 GPU 감지: {torch.cuda.get_device_name(0)}")
    logger.info(f"🔢 CUDA 버전: {torch.version.cuda}")
    logger.info(f"🐍 PyTorch 버전: {torch.__version__}")
    
    # GPU 처리기 생성 및 실행
    try:
        processor = GPUBatchSentimentProcessor(
            db_path=args.db_path,
            gpu_batch_size=args.gpu_batch_size
        )
        
        logger.info(f"⚙️ GPU 배치 크기: {args.gpu_batch_size}")
        
        # GPU 분석 실행
        processor.run_gpu_analysis()
        
        logger.info("🎉 GPU 감성분석 완료!")
        
    except RuntimeError as e:
        logger.error(f"❌ GPU 런타임 오류: {e}")
        logger.error("GPU 메모리 부족이거나 CUDA 설정 문제일 수 있습니다.")
    except KeyboardInterrupt:
        logger.warning("⚠️ 사용자에 의해 중단되었습니다.")
    except Exception as e:
        logger.error(f"❌ 처리 중 오류 발생: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()