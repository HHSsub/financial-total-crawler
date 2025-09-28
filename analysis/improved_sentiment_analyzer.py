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
    
    def __init__(self):
        self.gpu_available = self._check_gpu_availability()
        self.device_count = 0
        self.devices = []
        
        if self.gpu_available:
            self._initialize_gpu()
    
    def _check_gpu_availability(self) -> bool:
        """GPU 사용 가능성 체크"""
        if not torch.cuda.is_available():
            raise RuntimeError("❌ CUDA가 사용 불가능합니다. GPU 드라이버와 CUDA를 확인하세요.")
        
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
        """최적의 GPU 디바이스 반환"""
        if not self.gpu_available:
            raise RuntimeError("GPU를 사용할 수 없습니다.")
        
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


class DynamicKeywordGenerator:
    """동적 키워드 생성기"""
    
    def __init__(self):
        self.cache = {}
        self.industry_cache = {}
        
    def get_company_info_from_ticker(self, ticker: str, company_name: str = None) -> Dict:
        """종목코드로부터 회사 정보 추출"""
        industry_patterns = {
            "IT/소프트웨어": ["IT", "소프트웨어", "플랫폼", "데이터", "AI", "클라우드", "시스템", "테크"],
            "통신": ["텔레콤", "통신", "모바일", "네트워크", "5G"],
            "금융": ["금융", "은행", "카드", "증권", "보험", "캐피탈"],
            "제조": ["제조", "생산", "공장", "산업", "머티리얼"],
            "바이오": ["바이오", "제약", "의료", "헬스케어", "의학"],
            "화학": ["화학", "케미칼", "소재"],
            "에너지": ["에너지", "전력", "가스", "석유"],
            "건설": ["건설", "건축", "부동산"],
            "유통": ["유통", "리테일", "쇼핑", "마트"],
            "전자": ["전자", "반도체", "디스플레이", "배터리"]
        }
        
        if company_name:
            name_lower = company_name.lower()
            for industry, keywords in industry_patterns.items():
                if any(keyword.lower() in name_lower for keyword in keywords):
                    return {
                        "industry": industry,
                        "business_type": self._infer_business_type(company_name),
                        "scale": self._infer_company_scale(ticker)
                    }
        
        return {
            "industry": "일반",
            "business_type": "일반기업", 
            "scale": "중견기업"
        }
    
    def _infer_business_type(self, company_name: str) -> str:
        """사업 유형 추론"""
        name_lower = company_name.lower()
        
        if any(word in name_lower for word in ["시스템", "솔루션", "테크", "데이터", "ai"]):
            return "IT서비스"
        elif any(word in name_lower for word in ["제조", "산업", "머티리얼"]):
            return "제조업"
        elif any(word in name_lower for word in ["금융", "캐피탈", "투자"]):
            return "금융업"
        else:
            return "일반기업"
    
    def _infer_company_scale(self, ticker: str) -> str:
        """기업 규모 추론"""
        if len(ticker) == 6:
            first_digit = int(ticker[0])
            if first_digit <= 1:
                return "대기업"
            elif first_digit <= 3:
                return "중견기업"
            else:
                return "중소기업"
        return "중견기업"

    def _generate_keywords_rule_based(self, company_info: Dict) -> Dict:
        """규칙 기반 키워드 생성"""
        industry = company_info.get('industry', '일반')
        
        # 산업별 키워드 매핑
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

    def get_company_keywords(self, ticker: str, company_name: str) -> Dict:
        """캐시된 키워드 반환 또는 새로 생성"""
        cache_key = f"{ticker}_{company_name}"
        
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        company_info = self.get_company_info_from_ticker(ticker, company_name)
        company_info['name'] = company_name
        company_info['ticker'] = ticker
        
        keywords = self._generate_keywords_rule_based(company_info)
        
        self.cache[cache_key] = {
            "company_info": company_info,
            "keywords": keywords,
            "generated_at": datetime.now().isoformat()
        }
        
        return self.cache[cache_key]


class SmartSentimentAnalyzer:
    """GPU 최적화된 스마트 감성분석기"""
    
    def __init__(self, force_gpu: bool = True):
        # GPU 관리자 초기화
        self.gpu_manager = GPUManager()
        self.force_gpu = force_gpu
        
        if force_gpu and not self.gpu_manager.gpu_available:
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
            "klue-roberta-large": {
                "name": "klue/roberta-large",
                "description": "KLUE RoBERTa Large - 한국어 특화",
                "type": "transformer",
                "weight": 0.4,
                "max_length": 512,
                "batch_size": 8
            },
            "korean-finbert": {
                "name": "snunlp/KR-FinBERT-SC",
                "description": "한국어 금융 특화 BERT",
                "type": "transformer", 
                "weight": 0.35,
                "max_length": 512,
                "batch_size": 8
            },
            "multilingual-roberta": {
                "name": "cardiffnlp/twitter-xlm-roberta-base-sentiment-multilingual",
                "description": "다국어 XLM-RoBERTa",
                "type": "pipeline",
                "weight": 0.25,
                "max_length": 512,
                "batch_size": 16
            }
        }

        # 설정
        self.config = {
            "keyword_weight": 0.4,
            "model_weight": 0.6,
            "normalization_strength": 0.7,
            "stability_factor": 0.1,
            "dynamic_keyword_enabled": True,
            "gpu_batch_size": 16,
            "use_mixed_precision": True,
            "gpu_memory_fraction": 0.8
        }

    def get_text_hash(self, text: str) -> str:
        """텍스트 해시 생성"""
        return hashlib.md5(text.encode('utf-8')).hexdigest()

    def normalize_text(self, text: str) -> str:
        """텍스트 정규화"""
        text = re.sub(r'출처\s*[:：]\s*.*$', '', text, flags=re.MULTILINE)
        text = re.sub(r'http[s]?://[^\s]+', '', text)
        text = re.sub(r'www\.[^\s]+', '', text)
        text = re.sub(r'기자\s*[:：]?\s*[가-힣]+', '', text)
        text = re.sub(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', '', text)
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

    def get_dynamic_keywords(self, company_name: str, ticker: str) -> Dict:
        """동적 키워드 생성"""
        if not self.config["dynamic_keyword_enabled"]:
            return self._get_base_keywords()
        
        if not company_name and not ticker:
            return self._get_base_keywords()
        
        company_data = self.keyword_generator.get_company_keywords(
            ticker or "000000", 
            company_name or "Unknown"
        )
        
        return company_data.get("keywords", self._get_base_keywords())

    def _get_base_keywords(self) -> Dict:
        """기본 키워드 세트"""
        return {
            "positive_keywords": [
                "상승", "증가", "성장", "발전", "확대", "개선", "호조", "긍정",
                "수익", "이익", "성과", "성공", "혁신", "달성", "향상", "활성화"
            ],
            "negative_keywords": [
                "하락", "감소", "부진", "악화", "우려", "타격", "충격", "위기",
                "손실", "적자", "문제", "어려움", "침체", "위험", "실패", "오류"
            ],
            "industry_specific_positive": [],
            "industry_specific_negative": [],
            "risk_patterns": []
        }

    def smart_keyword_analysis(self, text: str, company_name: str = None, ticker: str = None) -> Dict:
        """스마트 키워드 분석"""
        text_lower = text.lower()
        
        keyword_data = self.get_dynamic_keywords(company_name, ticker)
        
        positive_keywords = keyword_data.get("positive_keywords", [])
        negative_keywords = keyword_data.get("negative_keywords", [])
        risk_patterns = keyword_data.get("risk_patterns", [])
        
        # 키워드 매칭
        pos_matches = []
        neg_matches = []
        risk_matches = []
        
        for kw in positive_keywords:
            if re.search(r'\b' + re.escape(kw.lower()) + r'\b', text_lower):
                pos_matches.append(kw)
        
        for kw in negative_keywords:
            if re.search(r'\b' + re.escape(kw.lower()) + r'\b', text_lower):
                neg_matches.append(kw)
        
        for pattern in risk_patterns:
            if re.search(pattern, text_lower):
                risk_matches.append(pattern)
        
        # 가중치 계산
        total_pos = len(pos_matches)
        total_neg = len(neg_matches) + len(risk_matches) * 2
        
        total_signals = total_pos + total_neg
        if total_signals == 0:
            return {
                "positive": 0.5,
                "negative": 0.5,
                "details": {
                    "pos_matches": pos_matches,
                    "neg_matches": neg_matches,
                    "risk_matches": risk_matches,
                    "keyword_source": "dynamic" if self.config["dynamic_keyword_enabled"] else "base"
                }
            }

        positive_ratio = total_pos / total_signals
        negative_ratio = total_neg / total_signals

        return {
            "positive": positive_ratio,
            "negative": negative_ratio,
            "details": {
                "pos_matches": pos_matches,
                "neg_matches": neg_matches,
                "risk_matches": risk_matches,
                "keyword_source": "dynamic" if self.config["dynamic_keyword_enabled"] else "base",
                "company_info": keyword_data.get("company_info", {})
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
                    device=self.device.index,
                    torch_dtype=torch.float16 if self.config["use_mixed_precision"] else torch.float32,
                    return_all_scores=True
                )
                
            else:
                tokenizer = AutoTokenizer.from_pretrained(
                    model_info["name"],
                    use_fast=True
                )
                
                model = AutoModelForSequenceClassification.from_pretrained(
                    model_info["name"],
                    torch_dtype=torch.float16 if self.config["use_mixed_precision"] else torch.float32,
                    device_map="auto"
                )
                
                model = model.to(self.device)
                model.eval()
                
                if hasattr(model, 'half') and self.config["use_mixed_precision"]:
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
        """GPU 최적화된 단일 모델 분석"""
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
                    return {"positive": 0.5, "negative": 0.5}
                
                positive_scores = []
                negative_scores = []
                
                for result in all_results:
                    label = result.get("label", "").upper()
                    score = result.get("score", 0)
                    
                    if label in ["POSITIVE", "POS", "LABEL_2"]:
                        positive_scores.append(score)
                        negative_scores.append(1 - score)
                    elif label in ["NEGATIVE", "NEG", "LABEL_0"]:
                        negative_scores.append(score)
                        positive_scores.append(1 - score)
                
                if not positive_scores:
                    return {"positive": 0.5, "negative": 0.5}
                
                avg_positive = np.mean(positive_scores)
                avg_negative = np.mean(negative_scores)
                
                total = avg_positive + avg_negative
                if total == 0:
                    return {"positive": 0.5, "negative": 0.5}

                return {
                    "positive": avg_positive / total,
                    "negative": avg_negative / total
                }

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
                    return {"positive": 0.5, "negative": 0.5}

                avg_probs = np.mean(all_probs, axis=0)

                positive_prob = avg_probs[0] if len(avg_probs) > 0 else 0
                negative_prob = avg_probs[1] if len(avg_probs) > 1 else 0

                total = positive_prob + negative_prob
                if total == 0:
                    return {"positive": 0.5, "negative": 0.5}

                return {
                    "positive": positive_prob / total,
                    "negative": negative_prob / total
                }

        except Exception as e:
            logger.error(f"GPU 모델 분석 실패 ({model_key}): {e}")
            self.gpu_manager.clear_gpu_cache()
            return {"positive": 0.5, "negative": 0.5}

    def ensemble_analysis(self, text: str) -> Dict:
        """앙상블 분석"""
        model_results = {}

        for model_key, model_info in self.ensemble_models.items():
            if model_key in self.models:
                result = self.analyze_with_single_model(text, model_key)
                if result:
                    model_results[model_key] = {
                        "result": result,
                        "weight": model_info["weight"]
                    }

        if not model_results:
            return {"positive": 0.5, "negative": 0.5}

        total_weight = sum(data["weight"] for data in model_results.values())

        weighted_positive = sum(
            data["result"]["positive"] * data["weight"]
            for data in model_results.values()
        ) / total_weight

        weighted_negative = sum(
            data["result"]["negative"] * data["weight"]
            for data in model_results.values()
        ) / total_weight

        # 결과 안정화
        stability_factor = self.config["stability_factor"]
        weighted_positive = weighted_positive * (1 - stability_factor) + 0.5 * stability_factor
        weighted_negative = weighted_negative * (1 - stability_factor) + 0.5 * stability_factor

        return {
            "positive": weighted_positive,
            "negative": weighted_negative,
            "model_details": {model_key: data["result"] for model_key, data in model_results.items()}
        }

    def analyze_sentiment(self, text: str, hint_company: str = None, hint_ticker: str = None) -> Dict:
        """메인 감성분석 함수"""
        processed_text = self.normalize_text(text)
        
        if not processed_text:
            return {"error": "분석할 텍스트가 없습니다."}

        # 캐싱 확인
        text_hash = self.get_text_hash(processed_text)
        if text_hash in self.analysis_cache:
            return self.analysis_cache[text_hash]

        # 텍스트에서 기업 정보 추출
        extracted_company, extracted_ticker = self.extract_company_from_text(processed_text)
        
        target_company = hint_company or extracted_company
        target_ticker = hint_ticker or extracted_ticker

        # 앙상블 분석
        ensemble_result = self.ensemble_analysis(processed_text)
        
        # 키워드 분석
        keyword_result = self.smart_keyword_analysis(processed_text, target_company, target_ticker)

        # 최종 스코어 계산
        keyword_weight = self.config["keyword_weight"]
        model_weight = self.config["model_weight"]
        normalization_strength = self.config["normalization_strength"]

        final_positive = (ensemble_result["positive"] * model_weight +
                          keyword_result["positive"] * keyword_weight)
        final_negative = (ensemble_result["negative"] * model_weight +
                          keyword_result["negative"] * keyword_weight)

        # 정규화
        total = final_positive + final_negative
        if total > 0:
            final_positive /= total
            final_negative /= total

        # 결과 안정화
        final_positive = final_positive * normalization_strength + 0.5 * (1 - normalization_strength)
        final_negative = final_negative * normalization_strength + 0.5 * (1 - normalization_strength)

        # 최종 정규화
        total = final_positive + final_negative
        final_positive /= total
        final_negative /= total

        sentiment = "positive" if final_positive > final_negative else "negative"
        score = final_positive - final_negative
        confidence = max(final_positive, final_negative)

        result = {
            "sentiment": sentiment,
            "score": round(score, 4),
            "confidence": round(confidence, 4),
            "target_company": target_company,
            "target_ticker": target_ticker,
            "probabilities": {
                "positive": round(final_positive, 4),
                "negative": round(final_negative, 4)
            },
            "ensemble_raw": {
                "positive": round(ensemble_result["positive"], 4),
                "negative": round(ensemble_result["negative"], 4)
            },
            "keyword_analysis": {
                "probabilities": {
                    "positive": round(keyword_result["positive"], 4),
                    "negative": round(keyword_result["negative"], 4)
                },
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

    def batch_analyze_gpu(self, texts: List[str], companies: List[str] = None, tickers: List[str] = None) -> List[Dict]:
        """GPU 최적화된 배치 분석"""
        if not texts:
            return []
        
        logger.info(f"GPU 배치 분석 시작: {len(texts)}개 텍스트")
        
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
                    result = self.analyze_sentiment(text, company, ticker)
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
    """GPU 최적화된 테스트 함수"""
    print("🚀 GPU 기반 동적 키워드 생성 감성분석 시스템")
    print("=" * 60)
    
    try:
        # GPU 필수 모드로 분석기 생성
        analyzer = SmartSentimentAnalyzer(force_gpu=True)
        
        print("\n📦 GPU에 모델 로딩 중...")
        loaded_models = analyzer.load_all_models()
        if not loaded_models:
            print("❌ 모델 로딩 실패")
            return
        
        print(f"✅ GPU 로드된 모델: {loaded_models}")
        print(f"✅ 동적 키워드 생성: {analyzer.config['dynamic_keyword_enabled']}")
        print(f"✅ 혼합 정밀도: {analyzer.config['use_mixed_precision']}")

        # GPU 상태 확인
        print(f"\n🔥 GPU 상태:")
        analyzer.gpu_manager.monitor_gpu_memory()

        # 테스트 케이스
        test_cases = [
            {
                "text": "신세계아이앤씨 052860이 국가정보원 주관 보안기능 확인서를 획득했다고 발표했습니다. 스파로스 CMP 플랫폼의 보안성이 국가 차원에서 인정받았으며, 이는 회사의 기술력을 입증하는 중요한 성과입니다.",
                "company": "신세계아이앤씨",
                "ticker": "052860"
            },
            {
                "text": "휴맥스 115160의 신제품 셋톱박스가 글로벌 시장에서 큰 호응을 얻고 있습니다. 스마트홈 기술과 AI 기능이 탑재된 이번 제품은 매출 증가에 크게 기여할 것으로 전망됩니다.",
                "company": "휴맥스", 
                "ticker": "115160"
            },
            {
                "text": "대규모 데이터유출 사고가 발생해 수백만 명의 개인정보가 해킹되었습니다. 보안시스템의 취약점이 노출되면서 고객들의 피해가 심각한 상황입니다.",
                "company": None,
                "ticker": None
            }
        ]

        # 개별 테스트
        print(f"\n📊 개별 분석 테스트")
        print("-" * 40)
        
        for i, test_case in enumerate(test_cases, 1):
            print(f"\n테스트 {i}: {test_case['text'][:50]}...")
            
            start_time = datetime.now()
            result = analyzer.analyze_sentiment(
                test_case['text'],
                hint_company=test_case['company'],
                hint_ticker=test_case['ticker']
            )
            end_time = datetime.now()
            processing_time = (end_time - start_time).total_seconds()
            
            if "error" in result:
                print(f"❌ 오류: {result['error']}")
            else:
                print(f"✅ 감성: {result['sentiment']} (점수: {result['score']}, 신뢰도: {result['confidence']})")
                print(f"🎯 대상: {result.get('target_company', 'N/A')} ({result.get('target_ticker', 'N/A')})")
                print(f"📈 확률: 긍정 {result['probabilities']['positive']}, 부정 {result['probabilities']['negative']}")
                print(f"⚡ GPU 시간: {processing_time:.3f}초")
                
                keyword_details = result['keyword_analysis']['details']
                print(f"🔍 키워드: +{len(keyword_details.get('pos_matches', []))}, -{len(keyword_details.get('neg_matches', []))}")

        # 배치 테스트
        print(f"\n🚀 GPU 배치 분석 테스트")
        print("-" * 40)
        
        batch_texts = [case['text'] for case in test_cases]
        batch_companies = [case['company'] for case in test_cases]
        batch_tickers = [case['ticker'] for case in test_cases]
        
        start_time = datetime.now()
        batch_results = analyzer.batch_analyze_gpu(batch_texts, batch_companies, batch_tickers)
        end_time = datetime.now()
        batch_time = (end_time - start_time).total_seconds()
        
        print(f"✅ 배치 완료: {len(batch_results)}개 결과")
        print(f"⚡ 총 시간: {batch_time:.3f}초")
        print(f"🚀 처리속도: {len(batch_results)/batch_time:.2f}개/초")

        # 최종 GPU 상태
        print(f"\n🔥 최종 GPU 상태:")
        analyzer.gpu_manager.monitor_gpu_memory()

        print(f"\n✅ 테스트 완료!")

    except RuntimeError as e:
        print(f"❌ GPU 오류: {e}")
        print("CUDA 드라이버와 GPU 설정을 확인하세요.")
    except Exception as e:
        print(f"❌ 시스템 오류: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()