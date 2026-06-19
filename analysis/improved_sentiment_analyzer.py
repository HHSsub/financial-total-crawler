# sentiment_analyzer.py
# GPU 최적화된 동적 키워드 생성 감성분석 시스템

import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification, pipeline
import warnings
import re
import numpy as np
from typing import Dict, List, Tuple, Optional
import hashlib
from datetime import datetime
import requests
import json
import sqlite3
from dataclasses import dataclass
import gc
import logging
import time
import sys

# GPU 관련 import
try:
    import pynvml
    import nvidia_ml_py3 as nvml
    GPU_LIBS_AVAILABLE = True
except ImportError:
    GPU_LIBS_AVAILABLE = False
    print("WARNING: GPU 라이브러리가 설치되지 않았습니다. nvidia-ml-py3, pynvml 설치 필요")

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class GPUManager:
    """GPU 관리 및 모니터링 클래스"""
    
    def __init__(self, skip_gpu_check=False):
        self.skip_gpu_check = skip_gpu_check
        self.gpu_available = self._check_gpu_availability()
        self.device_count = 0
        self.devices = []
        
        if self.gpu_available:
            self._initialize_gpu()
    
    def _check_gpu_availability(self) -> bool:
        """GPU 사용 가능성 체크"""
        if not torch.cuda.is_available():
            # 사장님 요청: GPU 없다고 죽지 말고 조용히 CPU로 가기
            return False
        
        if not GPU_LIBS_AVAILABLE:
            print("⚠️ GPU 모니터링 라이브러리 없음 (선택사항)")
            return True
        
        return True
    
    def _initialize_gpu(self):
        """GPU 초기화"""
        try:
            if GPU_LIBS_AVAILABLE:
                pynvml.nvmlInit()
                nvml.nvmlInit()
            
            self.device_count = torch.cuda.device_count()
            
            print(f"🚀 GPU 정보")
            print(f"   사용 가능한 GPU: {self.device_count}개")
            
            for i in range(self.device_count):
                gpu_name = torch.cuda.get_device_name(i)
                gpu_memory = torch.cuda.get_device_properties(i).total_memory / (1024**3)
                print(f"   GPU {i}: {gpu_name} ({gpu_memory:.1f}GB)")
                
                self.devices.append({
                    'id': i,
                    'name': gpu_name,
                    'memory_total': gpu_memory
                })
            
            # 기본 GPU 설정
            torch.cuda.set_device(0)
            print(f"   기본 GPU: GPU 0")
            
        except Exception as e:
            logger.warning(f"GPU 초기화 경고: {e}")
    
    def get_optimal_device(self) -> torch.device:
        """최적의 GPU 디바이스 반환 (없으면 CPU)"""
        if not self.gpu_available:
            return torch.device("cpu")
        
        # 메모리 사용량이 가장 적은 GPU 선택
        best_gpu = 0
        min_memory_used = float('inf')
        
        for i in range(self.device_count):
            memory_used = torch.cuda.memory_allocated(i) / (1024**3)
            if memory_used < min_memory_used:
                min_memory_used = memory_used
                best_gpu = i
        
        device = torch.device(f'cuda:{best_gpu}')
        logger.info(f"선택된 GPU: {device} (사용중: {min_memory_used:.2f}GB)")
        return device
    
    def monitor_gpu_memory(self):
        """GPU 메모리 모니터링"""
        if not self.gpu_available:
            return
        
        for i in range(self.device_count):
            allocated = torch.cuda.memory_allocated(i) / (1024**3)
            cached = torch.cuda.memory_reserved(i) / (1024**3)
            total = self.devices[i]['memory_total']
            
            print(f"   GPU {i}: 할당 {allocated:.2f}GB, 캐시 {cached:.2f}GB, 전체 {total:.1f}GB")
    
    def clear_gpu_cache(self):
        """GPU 캐시 정리"""
        if self.gpu_available:
            torch.cuda.empty_cache()
            gc.collect()
            logger.info("GPU 캐시 정리 완료")
    
    def optimize_gpu_settings(self):
        """GPU 최적화 설정"""
        if not self.gpu_available:
            return
        
        torch.backends.cudnn.benchmark = True
        torch.backends.cudnn.deterministic = False
        torch.cuda.empty_cache()
        
        logger.info("GPU 최적화 설정 완료")


# ── 명사 추출용 불용어 (iter2 실험에서 검증된 세트) ──────────────────
_NOUN_STOPWORDS = {
    "계약", "사업", "결정", "발생", "확대", "단행", "완료", "진행",
    "구축", "도입", "관련", "기반", "운영", "서비스", "시스템", "기업",
    "반영", "체결", "제공", "통해", "위해", "대한", "등의", "부문",
    "관련", "현황", "분야", "방식", "부분", "경우", "상황", "방향",
    "내용", "대상", "결과", "과정", "활동", "지원", "강화", "추진"
}


def _extract_key_nouns(phrase: str, min_len: int = 2) -> list:
    """키워드 문장에서 의미 있는 핵심 명사 추출 (불용어 제거)"""
    tokens = re.findall(r'[가-힣A-Za-z]{2,}', phrase)
    return [t for t in tokens if t not in _NOUN_STOPWORDS and len(t) >= min_len]


def _noun_coexist_hit(text_nospace: str, nouns: list, min_match_ratio: float = 0.6) -> bool:
    """핵심 명사 중 min_match_ratio 이상이 텍스트에 포함되면 히트 (iter-2 검증 방식)"""
    if len(nouns) < 2:
        # 단일 명사는 substring 직접 매칭
        return bool(nouns) and nouns[0].lower() in text_nospace
    matched = sum(1 for n in nouns if n.lower() in text_nospace)
    return matched / len(nouns) >= min_match_ratio


class DynamicKeywordGenerator:
    """동적 키워드 생성기 — KRX CSV 업종 직접 조회 + Noun Coexist 매핑"""
    
    def __init__(self):
        self.cache = {}
        self.industry_keywords_map = {}  # 업종명 → {positive, negative, neutral}
        self.ticker_industry_map = {}    # ticker → 업종명 (KRX CSV 기반)
        self._load_keywords_from_excel()
        self._load_krx_ticker_map()
        
    def _load_keywords_from_excel(self):
        """엑셀 Sheet2(index=1) 고정 사용 — 50개 키워드/업종, 파싱 실패 셀 JSON 블록 자동 복구"""
        import os, re
        import pandas as pd
        import json
        
        excel_path = os.path.join(os.path.dirname(__file__), "산업군별_키워드", "산업군별_llm키워드추출.xlsx")
        
        if not os.path.exists(excel_path):
            logger.warning(f"⚠️ 키워드 엑셀 파일을 찾을 수 없습니다: {excel_path}")
            return
            
        try:
            # ★ Sheet2(index=1) 고정 — Sheet1은 절대 사용 안 함
            df = pd.read_excel(excel_path, sheet_name=1)
            logger.info(f"✅ 엑셀 Sheet2 로드: {len(df)}행, 컬럼={list(df.columns)}")
            
            ind_col = next((c for c in df.columns if '업종' in str(c)), None)
            kw_col  = next((c for c in df.columns if 'LLM' in str(c) or '키워드' in str(c) or 'keyword' in str(c).lower()), None)
            
            if not ind_col or not kw_col:
                logger.error(f"❌ 컬럼 인식 실패: {list(df.columns)}")
                return
            
            loaded_count = 0
            for _, row in df.iterrows():
                industry_name = str(row.get(ind_col, '')).strip()
                raw = row.get(kw_col, '')
                
                if not industry_name or industry_name in ('nan', 'None', ''):
                    continue
                
                # ── JSON 파싱 (실패 시 셀 내부 JSON 블록 정규식 추출 복구) ──
                kw_data = None
                
                if isinstance(raw, dict):
                    kw_data = raw
                elif isinstance(raw, str) and raw.strip():
                    try:
                        kw_data = json.loads(raw)
                    except json.JSONDecodeError:
                        # LLM 프롬프트 텍스트 등이 앞에 붙어있는 경우 → JSON 블록만 추출
                        match = re.search(r'\{[\s\S]+\}', raw)
                        if match:
                            try:
                                kw_data = json.loads(match.group())
                                logger.info(f"  ♻️ '{industry_name}' JSON 블록 복구 성공")
                            except json.JSONDecodeError:
                                logger.warning(f"  ⚠️ '{industry_name}' JSON 복구 실패 — base 키워드 사용")
                        else:
                            logger.warning(f"  ⚠️ '{industry_name}' JSON 블록 없음 — base 키워드 사용")
                
                if kw_data is None:
                    continue
                
                pos_kws = kw_data.get("positive_events", [])
                neg_kws = kw_data.get("negative_events", [])
                self.industry_keywords_map[industry_name] = {
                    "positive": pos_kws,
                    "negative": neg_kws,
                    "neutral": kw_data.get("neutral_structural_terms", []),
                    "positive_nouns": [_extract_key_nouns(kw) for kw in pos_kws],
                    "negative_nouns": [_extract_key_nouns(kw) for kw in neg_kws],
                    "risk_patterns": []
                }
                loaded_count += 1
                logger.debug(f"  업종 '{industry_name}': pos={len(pos_kws)}, neg={len(neg_kws)}")
            
            if loaded_count == 0:
                logger.error("❌ 키워드 로드 결과 0개")
                logger.error(f"   첫 행 샘플: {df.iloc[0].to_dict() if len(df) > 0 else 'empty'}")
            else:
                logger.info(f"✅ {loaded_count}개 산업군 키워드 로드 완료 (Sheet2)")
            
        except Exception as e:
            logger.error(f"❌ 키워드 엑셀 로딩 실패: {e}")

    def _load_krx_ticker_map(self):
        """KRX CSV에서 ticker → 업종 매핑 로드 (name regex 폐기)"""
        import os
        import pandas as pd
        
        csv_path = os.path.join(os.path.dirname(__file__),
                                "market_table_quarterly(KRX시가총액정보_상장주식수_dart기준기업).csv")
        if not os.path.exists(csv_path):
            logger.warning("⚠️ KRX CSV 없음 — ticker→업종 매핑 비활성화")
            return
        
        try:
            df = pd.read_csv(csv_path, encoding="utf-8-sig", dtype=str)
            # 컬럼명 후보: '종목코드', '티커', 'ticker', '업종', '업종명', '업종분류'
            code_col = next((c for c in df.columns if '종목코드' in c or '티커' in c or c.lower() == 'ticker'), None)
            ind_col  = next((c for c in df.columns if '업종' in c), None)
            
            if not code_col or not ind_col:
                logger.warning(f"⚠️ KRX CSV 컬럼 인식 실패: {list(df.columns)}")
                return
            
            for _, row in df.iterrows():
                ticker = str(row[code_col]).strip().zfill(6)
                industry = str(row[ind_col]).strip()
                self.ticker_industry_map[ticker] = industry
            
            logger.info(f"✅ KRX ticker 매핑 로드: {len(self.ticker_industry_map)}개")
        except Exception as e:
            logger.error(f"❌ KRX CSV 로딩 실패: {e}")

    def get_industry_by_ticker(self, ticker: str) -> str:
        """ticker로 KRX 업종 직접 조회 — 매핑 실패 시 None 반환"""
        if not ticker:
            return None
        padded = str(ticker).strip().zfill(6)
        return self.ticker_industry_map.get(padded, None)

    def _generate_keywords_rule_based(self, company_info: Dict) -> Dict:
        """엑셀 기반 또는 규칙 기반 키워드 생성"""
        industry = company_info.get('industry', '일반')
        
        # 1. 엑셀에서 로드된 고도화된 키워드 우선 적용
        if industry in self.industry_keywords_map:
            data = self.industry_keywords_map[industry]
            return {
                "positive_keywords": data["positive"],
                "negative_keywords": data["negative"],
                "industry_specific_positive": data.get("positive", []),
                "industry_specific_negative": data.get("negative", []),
                "risk_patterns": data.get("risk_patterns", [])
            }

        # 2. 하드코딩된 기본 산업별 키워드 (fallback)
        industry_keywords = {
            "IT/소프트웨어": {
                "positive": ["디지털전환", "클라우드", "AI도입", "자동화", "효율화", "플랫폼확장", 
                           "데이터분석", "보안강화", "시스템업그레이드", "기술혁신", "솔루션", "스마트"],
                "negative": ["보안취약점", "데이터유출", "시스템오류", "해킹", "서비스중단", 
                           "버그", "호환성문제", "성능저하", "장애"],
                "risk_patterns": [r'보안.*?취약점', r'데이터.*?유출', r'시스템.*?오류', r'서비스.*?중단']
            },
            "통신": {
                "positive": ["5G구축", "네트워크확장", "서비스개선", "커버리지확대", "통신품질향상", "인프라", "기지국"],
                "negative": ["통신장애", "서비스중단", "네트워크오류", "기지국문제", "음영지역"],
                "risk_patterns": [r'통신.*?장애', r'네트워크.*?마비', r'서비스.*?중단']
            },
            "금융": {
                "positive": ["디지털뱅킹", "핀테크", "금융혁신", "서비스확대", "수익증대", "대출", "투자"],
                "negative": ["금융사고", "부실채권", "손실", "금융감독", "리스크증가", "연체"],
                "risk_patterns": [r'금융.*?사고', r'부실.*?채권', r'손실.*?확대']
            },
            "제조": {
                "positive": ["생산증대", "품질향상", "자동화", "효율화", "수출증가", "공장", "생산라인"],
                "negative": ["생산중단", "품질문제", "리콜", "안전사고", "원자재상승"],
                "risk_patterns": [r'생산.*?중단', r'품질.*?문제', r'리콜.*?결정']
            },
            "전자": {
                "positive": ["신제품", "기술혁신", "반도체", "디스플레이", "배터리", "스마트폰"],
                "negative": ["리콜", "결함", "폭발", "화재", "불량"],
                "risk_patterns": [r'제품.*?결함', r'배터리.*?폭발', r'리콜.*?결정']
            }
        }
        
        # 기본 키워드
        base_positive = ["성장", "확대", "증가", "개선", "혁신", "성공", "달성", "향상", "상승", "호조"]
        base_negative = ["감소", "하락", "문제", "위험", "손실", "악화", "중단", "실패", "우려", "부진"]
        
        industry_data = industry_keywords.get(industry, {"positive": [], "negative": [], "risk_patterns": []})
        
        return {
            "positive_keywords": base_positive + industry_data["positive"],
            "negative_keywords": base_negative + industry_data["negative"],
            "industry_specific_positive": industry_data["positive"],
            "industry_specific_negative": industry_data["negative"],
            "risk_patterns": industry_data["risk_patterns"]
        }

    def get_industry_keywords(self, ticker: str, company_name: str = None) -> Dict:
        """
        ticker → KRX CSV 업종 → XLSX 키워드 세트 반환.
        매핑 실패 시 base_keywords 반환.
        반환 구조: {positive, negative, positive_nouns, negative_nouns, industry, source}
        """
        cache_key = f"kw_{ticker}_{company_name}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        # Step 1: ticker → KRX 업종
        industry = self.get_industry_by_ticker(ticker) if ticker else None
        source = "krx_csv"
        
        # Step 2: KRX 업종 → XLSX 매핑 (exact 또는 partial)
        kw_data = None
        if industry:
            # Exact match 먼저
            if industry in self.industry_keywords_map:
                kw_data = self.industry_keywords_map[industry]
            else:
                # Partial match (예: "전기·전자" ↔ "전기전자")
                for iname, idata in self.industry_keywords_map.items():
                    if iname.replace('·', '').replace(' ', '') in industry.replace('·', '').replace(' ', '') or \
                       industry.replace('·', '').replace(' ', '') in iname.replace('·', '').replace(' ', ''):
                        kw_data = idata
                        industry = iname
                        break
        
        if kw_data:
            result = {
                "positive": kw_data["positive"],
                "negative": kw_data["negative"],
                "positive_nouns": kw_data["positive_nouns"],
                "negative_nouns": kw_data["negative_nouns"],
                "industry": industry,
                "source": source
            }
        else:
            # Fallback: base keywords (단일 명사 수준, Noun Coexist에서도 작동)
            result = self._get_base_keyword_data()
            result["industry"] = industry or "unknown"
            result["source"] = "base_fallback"
        
        self.cache[cache_key] = result
        return result

    def _get_base_keyword_data(self) -> Dict:
        """Noun Coexist 호환 base keywords"""
        pos = ["성장", "확대", "증가", "개선", "혁신", "성공", "달성", "향상", "상승", "호조",
               "수주", "계약", "출시", "인증", "수상", "투자유치", "흑자", "매출증가"]
        neg = ["감소", "하락", "부진", "악화", "우려", "타격", "충격", "위기",
               "손실", "적자", "문제", "어려움", "침체", "위험", "실패", "중단"]
        return {
            "positive": pos,
            "negative": neg,
            "positive_nouns": [_extract_key_nouns(k) for k in pos],
            "negative_nouns": [_extract_key_nouns(k) for k in neg],
        }


class SmartSentimentAnalyzer:
    """GPU 최적화된 스마트 감성분석기"""
    
    def __init__(self, force_gpu: bool = False):
        # GPU 관리자 초기화
        self.gpu_manager = GPUManager(skip_gpu_check=not force_gpu)
        self.force_gpu = force_gpu
        
        if force_gpu and not self.gpu_manager.gpu_available:
            logger.error("❌ GPU가 필수(--force-gpu)로 설정되었으나 사용할 수 없습니다!")
            raise RuntimeError("GPU가 필수이지만 사용할 수 없습니다!")
        
        # GPU 최적화 설정
        self.gpu_manager.optimize_gpu_settings()
        self.device = self.gpu_manager.get_optimal_device()
        
        self.models = {}
        self.keyword_generator = DynamicKeywordGenerator()
        self.analysis_cache = {}
        
        logger.info(f"GPU 디바이스: {self.device}")

        # GPU 최적화된 앙상블 모델
        self.ensemble_models = {
            # klue-roberta-large 제거: 1.3GB, EC2 디스크 부족 (451MB 여유)
            "korean-finbert": {
                "name": "snunlp/KR-FinBERT-SC",
                "description": "한국어 금융 특화 BERT",
                "type": "transformer",
                "weight": 0.6,  # 0.3 → 0.6 (klue 제거 후 재분배)
                "max_length": 512,
                "batch_size": 8
            },
            "multilingual-roberta": {
                "name": "cardiffnlp/twitter-xlm-roberta-base-sentiment-multilingual",
                "description": "다국어 XLM-RoBERTa",
                "type": "pipeline",
                "weight": 0.4,  # 0.2 → 0.4 (klue 제거 후 재분배)
                "max_length": 512,
                "batch_size": 16
            }
        }

        # 설정
        self.config = {
            "keyword_weight": 0.5,
            "model_weight": 0.5,
            "dynamic_keyword_enabled": True,
            "gpu_batch_size": 16,
            "use_mixed_precision": True,
            "gpu_memory_fraction": 0.8,
            # neutral_threshold: 긍/부정 판정 민감도
            # 0.05 → pos/neg 비율 차이가 5%만 넘어도 긍/부정으로 판정
            "neutral_threshold": 0.05,
            # Noun Coexist 매칭 비율 (iter-2 실험에서 ratio=0.6이 94.8% 달성)
            "noun_coexist_ratio": 0.6
        }

    def reverse_sentence_order(self, text: str) -> str:
        """강건성 테스트용: 문장 순서 뒤바꾸기"""
        sentences = re.split(r'[.!?]\s+', text.strip())
        sentences = [s.strip() for s in sentences if s.strip()]

        if len(sentences) <= 1:
            return text

        reversed_sentences = sentences[::-1]

        result = '. '.join(reversed_sentences)
        if not result.endswith('.'):
            result += '.'

        return result

    def get_text_hash(self, text: str) -> str:
        """텍스트 해시 생성"""
        return hashlib.md5(text.encode('utf-8')).hexdigest()

    def normalize_text(self, text: str) -> str:
        """텍스트 정규화 및 노이즈 제거 고도화"""
        # 1. 일반적인 노이즈 제거 (이메일, URL, 기자명 등)
        text = re.sub(r'출처\s*[:：]\s*.*$', '', text, flags=re.MULTILINE)
        text = re.sub(r'http[s]?://[^\s]+', '', text)
        text = re.sub(r'www\.[^\s]+', '', text)
        text = re.sub(r'기자\s*[:：]?\s*[가-힣]+', '', text)
        text = re.sub(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', '', text)
        
        # 2. 웹 브라우징/포털 노이즈 제거 (추가됨)
        noise_patterns = [
            r'링크복사하기', r'SNS 공유', r'바로가기', r'구독하기', r'구독하세요', r'제보',
            r'Copyright', r'무단\s*전재', r'재배포\s*금지', r'ⓒ', r'\[\s*바로가기\s*\]',
            r'네이버\s*뉴스', r'카카오톡', r'페이스북', r'트위터', r'유튜브',
            r'뉴스레터', r'매콤달콤', r'이렇게\s*만들죠', r'▶',
            r'mk\.co\.kr', r'한국경제\s*TV', r'연합뉴스\s*TV', r'조선일보',
        ]
        for pattern in noise_patterns:
            text = re.sub(pattern, ' ', text, flags=re.IGNORECASE)
            
        # 3. 특수문자 정리
        text = re.sub(r'[^\w\s가-힣]', ' ', text)
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text

    def extract_company_from_text(self, text: str) -> Tuple[Optional[str], Optional[str]]:
        """텍스트에서 기업명과 종목코드 추출"""
        # 종목코드 패턴
        ticker_pattern = r'\b\d{6}\b'
        tickers = re.findall(ticker_pattern, text)
        
        # 회사명 패턴
        company_patterns = [
            r'([가-힣]+(?:전자|금융|은행|통신|시스템|텍|테크|바이오|제약|케미칼|건설|그룹|홀딩스))',
            r'([가-힣]{2,}(?:주식회사|㈜))',
        ]
        
        companies = []
        for pattern in company_patterns:
            matches = re.findall(pattern, text)
            companies.extend(matches)
        
        if companies:
            company_counts = {}
            for company in companies:
                company_counts[company] = company_counts.get(company, 0) + 1
            most_mentioned = max(company_counts.items(), key=lambda x: x[1])[0]
            ticker = tickers[0] if tickers else None
            return most_mentioned, ticker
        
        return None, tickers[0] if tickers else None

    def get_industry_keyword_data(self, company_name: str, ticker: str) -> Dict:
        """ticker → KRX 업종 → XLSX 키워드 데이터 반환 (Noun Coexist 호환)"""
        return self.keyword_generator.get_industry_keywords(ticker, company_name)

    def smart_keyword_analysis(self, text: str, company_name: str = None, ticker: str = None) -> Dict:
        """
        Noun Coexist 기반 키워드 분석 (iter-2 실험에서 ratio=0.6 → 94.8% 히트 검증).
        순수 substring 매칭 폐기 — XLSX 키워드는 문장형이라 0~1% 히트였음.
        """
        kw_data = self.get_industry_keyword_data(company_name, ticker)
        industry = kw_data.get("industry", "unknown")
        source   = kw_data.get("source", "unknown")
        ratio    = self.config["noun_coexist_ratio"]
        
        pos_kws   = kw_data.get("positive", [])
        neg_kws   = kw_data.get("negative", [])
        pos_nouns = kw_data.get("positive_nouns", [])
        neg_nouns = kw_data.get("negative_nouns", [])
        
        text_nospace = text.replace(" ", "").lower()
        
        pos_matches = []
        neg_matches = []
        
        for kw, nouns in zip(pos_kws, pos_nouns):
            if nouns and _noun_coexist_hit(text_nospace, nouns, ratio):
                pos_matches.append(kw)
        
        for kw, nouns in zip(neg_kws, neg_nouns):
            if nouns and _noun_coexist_hit(text_nospace, nouns, ratio):
                neg_matches.append(kw)
        
        total_pos    = len(pos_matches)
        total_neg    = len(neg_matches)
        total_signals = total_pos + total_neg
        
        # 키워드 매핑률 경고 (배치 로그용)
        if total_signals == 0:
            logger.debug(f"KEYWORD_NOMATCH: ticker={ticker} industry={industry} source={source}")
        
        if total_signals == 0:
            score = 2.0  # 매칭 없음 → 모델에 위임 (analyze_sentiment에서 100% 모델 사용)
        else:
            pos_ratio = total_pos / total_signals
            neg_ratio = total_neg / total_signals
            neutral_threshold = self.config["neutral_threshold"]
            
            if abs(pos_ratio - neg_ratio) <= neutral_threshold:
                score = 2.0
            elif pos_ratio > neg_ratio:
                # 강도 연속 스케일: pos_ratio 0.5→1.0 을 2.0→3.0으로 매핑
                score = 2.0 + (pos_ratio - 0.5) * 2.0
                score = min(3.0, max(2.0, score))
            else:
                # 부정 강도: neg_ratio 0.5→1.0 을 2.0→1.0으로 매핑
                score = 2.0 - (neg_ratio - 0.5) * 2.0
                score = min(2.0, max(1.0, score))
        
        return {
            "score": score,
            "details": {
                "pos_matches": pos_matches,
                "neg_matches": neg_matches,
                "risk_matches": [],
                "keyword_source": source,
                "industry": industry,
                "total_pos": total_pos,
                "total_neg": total_neg
            }
        }

    def load_model(self, model_key: str) -> bool:
        """GPU 최적화된 모델 로딩"""
        if model_key not in self.ensemble_models:
            return False

        try:
            model_info = self.ensemble_models[model_key]
            logger.info(f"GPU에 모델 로딩: {model_info['description']}")

            if model_info["type"] == "pipeline":
                self.models[model_key] = pipeline(
                    "sentiment-analysis",
                    model=model_info["name"],
                    device=-1 if not self.gpu_manager.gpu_available else self.device.index,
                    torch_dtype=torch.float32 if not self.gpu_manager.gpu_available else (torch.float16 if self.config["use_mixed_precision"] else torch.float32),
                    return_all_scores=True,
                    truncation=True,
                    max_length=512
                )
                
            else:
                tokenizer = AutoTokenizer.from_pretrained(
                    model_info["name"],
                    use_fast=True
                )
                
                # CPU 모드거나 accelerate가 없을 때를 대비한 안전한 로딩
                model_kwargs = {
                    "torch_dtype": torch.float32 if not self.gpu_manager.gpu_available else (torch.float16 if self.config["use_mixed_precision"] else torch.float32)
                }
                
                # GPU 사용 시에만 device_map 사용 (accelerate 필요)
                if self.gpu_manager.gpu_available:
                    model_kwargs["device_map"] = "auto"

                model = AutoModelForSequenceClassification.from_pretrained(
                    model_info["name"],
                    **model_kwargs
                )
                
                if not self.gpu_manager.gpu_available:
                    model = model.to("cpu")
                else:
                    model = model.to(self.device)
                
                model.eval()
                
                if self.gpu_manager.gpu_available and hasattr(model, 'half') and self.config["use_mixed_precision"]:
                    model = model.half()

                self.models[model_key] = {
                    "tokenizer": tokenizer,
                    "model": model,
                    "info": model_info
                }

            logger.info(f"모델 로딩 완료: {model_key}")
            return True

        except Exception as e:
            logger.error(f"모델 로딩 실패 ({model_key}): {e}")
            self.gpu_manager.clear_gpu_cache()
            return False

    def load_all_models(self) -> List[str]:
        """모든 모델 로드"""
        loaded_models = []
        for model_key in self.ensemble_models.keys():
            if self.load_model(model_key):
                loaded_models.append(model_key)
        return loaded_models

    def analyze_with_single_model(self, text: str, model_key: str) -> Dict:
        """GPU 최적화된 단일 모델 분석 - 3단계 점수 반환"""
        if model_key not in self.models:
            return None

        model_data = self.models[model_key]
        model_info = self.ensemble_models[model_key]

        try:
            max_length = model_info.get("max_length", 512)
            batch_size = model_info.get("batch_size", 8)
            
            chunks = []
            if len(text) > max_length - 50:
                sentences = re.split(r'[.!?]\s+', text)
                current_chunk = ""
                
                for sentence in sentences:
                    if len(current_chunk + sentence) < max_length - 50:
                        current_chunk += sentence + ". "
                    else:
                        if current_chunk:
                            chunks.append(current_chunk.strip())
                        current_chunk = sentence + ". "
                
                if current_chunk:
                    chunks.append(current_chunk.strip())
            else:
                chunks = [text]

            # Pipeline 모델
            if callable(model_data):
                all_results = []
                
                for i in range(0, len(chunks), batch_size):
                    batch_chunks = chunks[i:i + batch_size]
                    batch_chunks = [chunk for chunk in batch_chunks if chunk.strip()]
                    
                    if not batch_chunks:
                        continue
                    
                    try:
                        with torch.no_grad():
                            batch_results = model_data(batch_chunks)
                        
                        for result_set in batch_results:
                            if isinstance(result_set, list):
                                all_results.extend(result_set)
                            else:
                                all_results.append(result_set)
                                
                    except Exception:
                        continue
                
                if not all_results:
                    return {"score": 2}
                
                # ── multilingual-roberta: 3-class (negative/neutral/positive = LABEL_0/LABEL_1/LABEL_2) ──
                # return_all_scores=True이므로 result_set은 3개 레이블 전체가 list로 옴
                # 각 chunk의 score를 레이블별로 분리해서 평균냄
                pos_chunk_scores = []
                neg_chunk_scores = []
                
                # all_results에는 각 label별 {label, score} dict가 섞여있음
                # chunk 단위로 pos/neg를 직접 추출
                for result in all_results:
                    label = result.get("label", "").upper()
                    score = result.get("score", 0.0)
                    
                    if label in ["POSITIVE", "POS", "LABEL_2"]:
                        pos_chunk_scores.append(score)
                    elif label in ["NEGATIVE", "NEG", "LABEL_0"]:
                        neg_chunk_scores.append(score)
                    # LABEL_1 (neutral)은 pos/neg 판정에 참여하지 않음
                
                if not pos_chunk_scores and not neg_chunk_scores:
                    return {"score": 2}
                
                avg_positive = np.mean(pos_chunk_scores) if pos_chunk_scores else 0.0
                avg_negative = np.mean(neg_chunk_scores) if neg_chunk_scores else 0.0
                
                # 3단계 점수로 변환
                total = avg_positive + avg_negative
                if total == 0:
                    return {"score": 2}

                pos_ratio = avg_positive / total
                neg_ratio = avg_negative / total
                
                # 중립 임계값 적용
                neutral_threshold = self.config["neutral_threshold"]
                
                if abs(pos_ratio - neg_ratio) <= neutral_threshold:
                    score = 2  # 중립
                elif pos_ratio > neg_ratio:
                    # 긍정 강도에 따라 점수 조정
                    score = 2 + (pos_ratio - 0.5) * 2  # 2-3 사이
                    score = min(3, max(2, score))
                else:
                    # 부정 강도에 따라 점수 조정
                    score = 2 - (neg_ratio - 0.5) * 2  # 1-2 사이
                    score = min(2, max(1, score))

                return {"score": score}

            # Transformer 모델
            else:
                tokenizer = model_data["tokenizer"]
                model = model_data["model"]

                all_probs = []
                
                for i in range(0, len(chunks), batch_size):
                    batch_chunks = chunks[i:i + batch_size]
                    batch_chunks = [chunk for chunk in batch_chunks if chunk.strip()]
                    
                    if not batch_chunks:
                        continue
                    
                    try:
                        inputs = tokenizer(
                            batch_chunks, 
                            return_tensors="pt", 
                            truncation=True, 
                            max_length=max_length, 
                            padding=True,
                            return_attention_mask=True
                        )
                        
                        inputs = {k: v.to(self.device) for k, v in inputs.items()}

                        with torch.no_grad():
                            if self.config["use_mixed_precision"]:
                                with torch.cuda.amp.autocast():
                                    outputs = model(**inputs)
                            else:
                                outputs = model(**inputs)
                            
                            probs = torch.softmax(outputs.logits, dim=1).cpu().numpy()
                            all_probs.extend(probs)

                    except RuntimeError as e:
                        if "out of memory" in str(e):
                            logger.warning("GPU 메모리 부족, 배치 크기 감소")
                            self.gpu_manager.clear_gpu_cache()
                            batch_size = max(1, batch_size // 2)
                            continue
                        else:
                            raise e

                if not all_probs:
                    return {"score": 2}

                avg_probs = np.mean(all_probs, axis=0)

                # ── KR-FinBERT-SC: 3-class 모델 ──────────────────────────────
                # label 순서: [0]=negative, [1]=neutral, [2]=positive
                # 절대로 avg_probs[0]을 positive로 쓰면 안 됨!
                if len(avg_probs) >= 3:
                    negative_prob = float(avg_probs[0])
                    # neutral(1)은 pos/neg 점수 계산에서 제외 (중립 영향 최소화)
                    positive_prob = float(avg_probs[2])
                elif len(avg_probs) == 2:
                    # 혹시 2-class 모델이 로드된 경우 (negative, positive)
                    negative_prob = float(avg_probs[0])
                    positive_prob = float(avg_probs[1])
                else:
                    return {"score": 2}

                total = positive_prob + negative_prob
                if total == 0:
                    return {"score": 2}

                pos_ratio = positive_prob / total
                neg_ratio = negative_prob / total
                
                # 3단계 점수로 변환
                neutral_threshold = self.config["neutral_threshold"]
                
                if abs(pos_ratio - neg_ratio) <= neutral_threshold:
                    score = 2  # 중립
                elif pos_ratio > neg_ratio:
                    # 긍정 강도에 따라 점수 조정
                    score = 2 + (pos_ratio - 0.5) * 2  # 2-3 사이
                    score = min(3, max(2, score))
                else:
                    # 부정 강도에 따라 점수 조정
                    score = 2 - (neg_ratio - 0.5) * 2  # 1-2 사이
                    score = min(2, max(1, score))

                return {"score": score}

        except Exception as e:
            logger.error(f"GPU 모델 분석 실패 ({model_key}): {e}")
            self.gpu_manager.clear_gpu_cache()
            return {"score": 2}

    def ensemble_analysis(self, text: str) -> Dict:
        """앙상블 분석 - 3단계 점수 반환"""
        model_results = {}

        for model_key, model_info in self.ensemble_models.items():
            if model_key in self.models:
                result = self.analyze_with_single_model(text, model_key)
                if result:
                    model_results[model_key] = {
                        "score": result["score"],
                        "weight": model_info["weight"]
                    }

        if not model_results:
            return {"score": 2}

        total_weight = sum(data["weight"] for data in model_results.values())

        weighted_score = sum(
            data["score"] * data["weight"]
            for data in model_results.values()
        ) / total_weight

        return {
            "score": weighted_score,
            "model_details": {model_key: data["score"] for model_key, data in model_results.items()}
        }

    def analyze_sentiment(self, text: str, hint_company: str = None, hint_ticker: str = None, use_keywords: bool = True, reverse_order: bool = False) -> Dict:
        """메인 감성분석 함수 - 3단계 점수 시스템 (Pure LLM 모드 지원)"""
        # 강건성 테스트: 문장 순서 뒤바꾸기는 정규화 전 원문에 적용
        if reverse_order:
            text = self.reverse_sentence_order(text)

        processed_text = self.normalize_text(text)

        if not processed_text:
            return {"error": "분석할 텍스트가 없습니다."}

        # 캐싱 확인 (use_keywords, reverse_order 상태도 키에 포함)
        text_hash = self.get_text_hash(f"{processed_text}_{use_keywords}_{reverse_order}")
        if text_hash in self.analysis_cache:
            return self.analysis_cache[text_hash]

        # 텍스트에서 기업 정보 추출
        extracted_company, extracted_ticker = self.extract_company_from_text(processed_text)
        
        target_company = hint_company or extracted_company
        target_ticker = hint_ticker or extracted_ticker

        # 앙상블 분석 (LLM 모델 자체 평가)
        ensemble_result = self.ensemble_analysis(processed_text)
        
        # 가중치 설정
        if use_keywords:
            # 키워드 분석 먼저 실행
            keyword_result = self.smart_keyword_analysis(processed_text, target_company, target_ticker)
            keyword_score = keyword_result["score"]
            
            # 키워드가 하나도 매칭 안 됐으면 → 모델에 100% 위임
            # (하드코딩 목록에 없는 표현도 중립으로 끌려내려오지 않게)
            details = keyword_result.get("details", {})
            has_any_match = bool(
                details.get("pos_matches") or
                details.get("neg_matches") or
                details.get("risk_matches")
            )
            if has_any_match:
                keyword_weight = self.config["keyword_weight"]
                model_weight = self.config["model_weight"]
            else:
                # 키워드 무매칭 → 모델만 사용
                keyword_weight = 0.0
                model_weight = 1.0
        else:
            # Pure LLM 모드: 모델 점수만 100% 반영
            keyword_weight = 0.0
            model_weight = 1.0
            keyword_score = 2.0
            keyword_result = {"score": 2.0, "details": {"note": "Pure LLM mode enabled - keywords bypassed"}}

        # 최종 스코어 계산 (bias 없음 — normalization/stability 완전 제거)
        final_score = (ensemble_result["score"] * model_weight +
                      keyword_score * keyword_weight)

        # 최종 점수를 1-3 범위로 제한
        final_score = max(1, min(3, final_score))

        # 감성 라벨 결정 — 경계 2.4/1.6으로 설정 (너무 빡빡한 2.5/1.5 완화)
        if final_score >= 2.4:
            sentiment = "positive"
        elif final_score <= 1.6:
            sentiment = "negative"
        else:
            sentiment = "neutral"

        # 신뢰도 계산 (중립에서 멀수록 높은 신뢰도)
        confidence = abs(final_score - 2) / 1.0

        result = {
            "sentiment": sentiment,
            "score": round(final_score, 4),
            "confidence": round(confidence, 4),
            "target_company": target_company,
            "target_ticker": target_ticker,
            "ensemble_score": round(ensemble_result["score"], 4),
            "keyword_score": round(keyword_result["score"], 4),
            "keyword_analysis": {
                "score": round(keyword_result["score"], 4),
                "details": keyword_result["details"]
            },
            "model_details": ensemble_result.get("model_details", {}),
            "config": self.config,
            "analysis_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "gpu_info": {
                "device": str(self.device),
                "mixed_precision": self.config["use_mixed_precision"]
            }
        }

        # 결과 캐싱
        self.analysis_cache[text_hash] = result
        
        # 캐시 크기 제한
        if len(self.analysis_cache) > 500:
            oldest_key = next(iter(self.analysis_cache))
            del self.analysis_cache[oldest_key]

        # 주기적 GPU 메모리 정리
        if len(self.analysis_cache) % 50 == 0:
            self.gpu_manager.clear_gpu_cache()

        return result

    def batch_analyze_gpu(self, texts: List[str], companies: List[str] = None, tickers: List[str] = None, use_keywords: bool = True) -> List[Dict]:
        """GPU 최적화된 배치 분석"""
        if not texts:
            return []
        
        logger.info(f"GPU 배치 분석 시작: {len(texts)}개 텍스트 (Keywords: {use_keywords})")
        
        companies = companies or [None] * len(texts)
        tickers = tickers or [None] * len(texts)
        
        results = []
        batch_size = self.config["gpu_batch_size"]
        
        for i in range(0, len(texts), batch_size):
            batch_texts = texts[i:i + batch_size]
            batch_companies = companies[i:i + batch_size]
            batch_tickers = tickers[i:i + batch_size]
            
            batch_results = []
            for text, company, ticker in zip(batch_texts, batch_companies, batch_tickers):
                try:
                    result = self.analyze_sentiment(text, company, ticker, use_keywords=use_keywords)
                    batch_results.append(result)
                except Exception as e:
                    logger.error(f"배치 분석 오류: {e}")
                    batch_results.append({"error": str(e)})
            
            results.extend(batch_results)
            
            # 배치간 GPU 메모리 정리
            if i % (batch_size * 3) == 0:
                self.gpu_manager.clear_gpu_cache()
        
        logger.info(f"GPU 배치 분석 완료: {len(results)}개 결과")
        return results


# 테스트 실행 부분
def main():
    """CLI 인자 지원 및 GPU 테스트"""
    import argparse
    parser = argparse.ArgumentParser(description="GPU 기반 동적 키워드 생성 감성분석 시스템")
    parser.add_argument("--pure-llm", action="store_true", help="키워드 분석 없이 모델 점수만 반영")
    parser.add_argument("--test", action="store_true", help="내장 테스트 케이스 실행")
    parser.add_argument("--cpu", action="store_true", help="GPU 대신 CPU 사용 (로컬 테스트용)")
    parser.add_argument("--db", type=str, default="news_fulltext_extract_20250915_171004.db", help="대상 DB 경로")
    args = parser.parse_args()

    # 사장님 요청: 기본적으로 GPU 시도하되 없으면 조용히 CPU로 전환 (강제성 제거)
    if args.cpu:
        print("💻 사용자에 의해 CPU 모드로 강제 설정되었습니다.", flush=True)
        force_gpu = False
    else:
        # 하드웨어 체크 실패 시 크래시를 방지하기 위해 SmartSentimentAnalyzer 내부에서 처리
        force_gpu = False # False로 주어야 RuntimeError가 발생하지 않음
        print("🚀 감성분석 시스템 시작 (GPU 확인 중...)", flush=True)

    print(f"🚀 모드 설정 (Pure LLM: {args.pure_llm})")
    print("=" * 60)
    
    try:
        # 분석기 생성
        analyzer = SmartSentimentAnalyzer(force_gpu=force_gpu)
        
        device_name = "GPU" if analyzer.gpu_manager.gpu_available else "CPU"
        print(f"✅ 구동 환경: {device_name} 모드", flush=True)
        
        print(f"\n📦 {device_name}에 모델 로딩 중...", flush=True)
        loaded_models = analyzer.load_all_models()
        if not loaded_models:
            print(f"❌ {device_name} 모델 로딩 실패", flush=True)
            return
        
        print(f"✅ 로드된 모델: {loaded_models}", flush=True)
        print(f"✅ 분석 모드: {'Pure LLM' if args.pure_llm else '하이브리드 (키워드 매칭 포함)'}", flush=True)
        print(f"✅ 3단계 점수 시스템: 긍정(3), 중립(2), 부정(1)")

        # GPU 상태 확인
        analyzer.gpu_manager.monitor_gpu_memory()

        if args.test:
            # 테스트 케이스 실행
            test_cases = [
                {
                    "text": "신세계아이앤씨 052860이 국가정보원 주관 보안기능 확인서를 획득했다고 발표했습니다. 스파로스 CMP 플랫폼의 보안성이 국가 차원에서 인정받았으며, [기사 바로가기] http://example.com/news/123",
                    "company": "신세계아이앤씨",
                    "ticker": "052860"
                },
                {
                    "text": "휴맥스 115160의 신제품 셋톱박스가 글로벌 시장에서 큰 호응을 얻고 있습니다. 스마트홈 기술과 AI 기능이 탑재된 이번 제품은 매출 증가에 크게 기여할 것으로 전망됩니다. ⓒCopyright All rights reserved.",
                    "company": "휴맥스", 
                    "ticker": "115160"
                }
            ]

            print(f"\n📊 분석 테스트 (Pure LLM: {args.pure_llm})")
            print("-" * 40)
            
            for i, test_case in enumerate(test_cases, 1):
                start_time = datetime.now()
                result = analyzer.analyze_sentiment(
                    test_case['text'],
                    hint_company=test_case['company'],
                    hint_ticker=test_case['ticker'],
                    use_keywords=not args.pure_llm
                )
                processing_time = (datetime.now() - start_time).total_seconds()
                
                print(f"\n테스트 {i}: {result['sentiment']} (점수: {result['score']})")
                print(f"cleaned_text: {analyzer.normalize_text(test_case['text'])[:50]}...")
                print(f"⚡ {'GPU' if analyzer.gpu_manager.gpu_available else 'CPU'} 시간: {processing_time:.3f}초")
        else:
            # 24시간 감시 모드 실행
            db_path = args.db
            print(f"\n🚀 24시간 DB 감시 분석 시작 (DB: {db_path})", flush=True)
            
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # 결과 컬럼 추가 (없을 경우만)
            try:
                cursor.execute("ALTER TABLE articles ADD COLUMN imp_sentiment TEXT")
                cursor.execute("ALTER TABLE articles ADD COLUMN imp_score REAL")
                cursor.execute("ALTER TABLE articles ADD COLUMN imp_date TEXT")
                conn.commit()
                print("✅ DB 테이블 구조 업데이트 완료 (컬럼 추가)")
            except sqlite3.OperationalError:
                pass # 이미 존재함
                
            batch_count = 0
            while True:
                # 미분석 데이터 가져오기 (배치 단위)
                cursor.execute("""
                    SELECT link, title, full_content, ticker 
                    FROM articles 
                    WHERE imp_score IS NULL 
                    ORDER BY pub_date DESC 
                    LIMIT 20
                """)
                rows = cursor.fetchall()
                
                if not rows:
                    if batch_count % 10 == 0:
                        print(f"[{datetime.now()}] 새로운 기사가 없습니다. 대기 중...", flush=True)
                    batch_count += 1
                    time.sleep(60)
                    continue
                
                print(f"[{datetime.now()}] {len(rows)}개 기사 분석 수행 중...", flush=True)
                
                for link, title, content, ticker in rows:
                    try:
                        text = f"{title}\n{content or ''}"
                        result = analyzer.analyze_sentiment(text, hint_ticker=ticker, use_keywords=not args.pure_llm)
                        
                        cursor.execute("""
                            UPDATE articles 
                            SET imp_sentiment = ?, imp_score = ?, imp_date = ? 
                            WHERE link = ?
                        """, (result['sentiment'], result['score'], result['analysis_time'], link))
                        
                        # PM2 로그에 실시간 출력 (사장님이 진행 상황을 볼 수 있게 함)
                        print(f"  - [{result['sentiment']}] {title[:40]}... (Score: {result['score']})", flush=True)
                        
                    except Exception as e:
                        logger.error(f"건별 분석 오류: {e}")
                
                conn.commit()
                batch_count += 1
                # GPU 메모리 정기 청소
                analyzer.gpu_manager.clear_gpu_cache()

    except Exception as e:
        print(f"❌ 치명적 오류: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
