# sentiment_analyzer.py

import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification, pipeline
import warnings
import re
import numpy as np
from typing import Dict, List, Tuple

warnings.filterwarnings("ignore")


class ScalableSentimentAnalyzer:
    def __init__(self):
        self.models = {}
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        print(f"Device: {self.device}")

        # 앙상블용 모델 정의
        self.ensemble_models = {
            "kr-finbert": {
                "name": "snunlp/KR-FinBERT-SC",
                "description": "한국어 금융 특화 BERT",
                "type": "transformer",
                "weight": 0.5,
                "task": "classification"
            },
            "multilingual": {
                "name": "cardiffnlp/twitter-xlm-roberta-base-sentiment-multilingual",
                "description": "다국어 감성분석",
                "type": "pipeline",
                "weight": 0.5,
                "task": "sentiment-analysis"
            }
        }

        # 외부 설정 가능한 구조
        self.company_mapping = {}  # 외부에서 주입 가능
        self.industry_keywords = {}  # 외부에서 주입 가능
        self.target_company = None  # 분석 대상 기업

        # 기본 키워드와 패턴 설정
        self.setup_base_keywords()
        self.setup_base_patterns()

        # 앙상블 설정
        self.config = {
            "keyword_weight": 0.5,
            "model_weight": 0.5,
            "min_confidence_threshold": 0.1  # 최소 신뢰도
        }

    def setup_base_keywords(self):
        """기본 키워드 설정 (확장 가능)"""
        self.base_positive_keywords = [
            "상승", "급등", "증가", "성장", "발전", "확대", "개선", "호조", "긍정",
            "수익", "이익", "매출", "실적", "돌파", "달성", "향상", "활성화", "부상",
            "흑자", "성과", "성공", "혁신", "선점", "최고", "신기록", "호재",
            "수상", "대상", "영예", "우승", "1위", "최우수", "포상", "시상", "표창",
            "인정", "선정", "채택", "승인", "통과",
            "투자", "기대", "회복", "개발", "고도화", "확장", "런칭", "출시",
            "계약", "체결", "합의", "협력", "제휴", "파트너십"
        ]

        self.base_negative_keywords = [
            "하락", "급락", "감소", "부진", "악화", "우려", "타격", "충격", "위기",
            "손실", "적자", "부채", "마이너스", "하향", "폭락", "추락", "곤란", "악재",
            "최저", "최악", "파산", "도산", "부실", "한계", "위축", "둔화",
            "사고", "사건", "문제", "어려움", "침체", "불황", "실업", "구조조정",
            "위험", "경고", "주의", "실패", "오류", "결함", "장애", "마비",
            "중단", "지연", "제재", "처벌", "고발", "수사", "소송",
            "해킹", "유출", "침해", "공격", "노출", "피해", "불안", "탈취",
            "위조", "도용", "불법", "스팸", "피싱", "보안", "취약", "침투",
            "악용", "조작", "사기", "범죄", "바이러스", "악성코드"
        ]

    def setup_base_patterns(self):
        """기본 부정 패턴 설정 (확장 가능)"""
        self.base_negative_patterns = [
            r'데이터.*?유출', r'정보.*?유출', r'개인정보.*?유출',
            r'해킹.*?공격', r'사이버.*?공격', r'보안.*?침해',
            r'시스템.*?침투', r'서버.*?침입', r'내부망.*?침투',
            r'고객.*?피해', r'가입자.*?피해', r'이용자.*?피해',
            r'악성.*?코드', r'바이러스.*?감염', r'멀웨어',
            r'금융.*?사고', r'계좌.*?해킹', r'카드.*?도용',
            r'제품.*?결함', r'품질.*?문제', r'리콜',
            r'생산.*?중단', r'공장.*?가동.*?중단', r'서비스.*?중단'
        ]

    def reverse_sentence_order(self, text: str) -> str:
        """강건성 테스트용: 문장 순서 뒤바꾸기"""
        sentences = re.split(r'[.!?]\s+', text.strip())
        sentences = [s.strip() for s in sentences if s.strip()]
        
        if len(sentences) <= 1:
            return text
        
        # 마지막 문장이 구두점으로 끝나지 않는 경우 처리
        last_sentence = sentences[-1]
        if not re.search(r'[.!?]$', last_sentence):
            sentences[-1] = last_sentence
        
        # 문장 순서 뒤바꾸기
        reversed_sentences = sentences[::-1]
        
        # 원래 구두점 패턴 유지하며 재조립
        result = '. '.join(reversed_sentences)
        if not result.endswith('.'):
            result += '.'
        
        return result

    def load_company_mapping(self, mapping_data: Dict):
        """외부 기업 매핑 데이터 로드"""
        self.company_mapping = mapping_data
        print(f"기업 매핑 로드 완료: {len(mapping_data)}개 기업")

    def load_industry_keywords(self, industry_data: Dict):
        """외부 산업별 키워드 데이터 로드"""
        self.industry_keywords = industry_data
        print(f"산업별 키워드 로드 완료: {len(industry_data)}개 산업")

    def extract_target_company(self, text: str, hint_company: str = None) -> Tuple[str, str]:
        """기업 추출 (외부 힌트 지원)"""
        if hint_company:
            industry = self.company_mapping.get(hint_company, {}).get("industry", "일반")
            print(f"외부 힌트 사용 - 대상 기업: {hint_company} (산업: {industry})")
            return hint_company, industry

        text_lower = text.lower()
        company_mentions = {}

        for company, info in self.company_mapping.items():
            keywords = info.get("keywords", [company.lower()])
            count = 0
            for keyword in keywords:
                count += len(re.findall(r'\b' + re.escape(keyword) + r'\b', text_lower))
            if count > 0:
                company_mentions[company] = count

        if not company_mentions:
            return None, "일반"

        target_company = max(company_mentions.items(), key=lambda x: x[1])[0]
        industry = self.company_mapping.get(target_company, {}).get("industry", "일반")

        print(f"자동 추출 - 대상 기업: {target_company} (산업: {industry})")
        print(f"기업 언급 빈도: {company_mentions}")

        return target_company, industry

    def get_effective_keywords(self, target_company: str, industry: str) -> Tuple[List[str], List[str]]:
        positive_keywords = self.base_positive_keywords.copy()
        negative_keywords = self.base_negative_keywords.copy()

        if industry in self.industry_keywords:
            industry_data = self.industry_keywords[industry]
            positive_keywords.extend(industry_data.get("positive", []))
            negative_keywords.extend(industry_data.get("negative", []))

        if target_company and target_company in self.company_mapping:
            company_data = self.company_mapping[target_company]
            positive_keywords.extend(company_data.get("positive_keywords", []))
            negative_keywords.extend(company_data.get("negative_keywords", []))

        return positive_keywords, negative_keywords

    def get_effective_patterns(self, industry: str) -> List[str]:
        patterns = self.base_negative_patterns.copy()

        if industry in self.industry_keywords:
            industry_patterns = self.industry_keywords[industry].get("patterns", [])
            patterns.extend(industry_patterns)

        return patterns

    def smart_keyword_analysis(self, text: str, target_company: str = None, industry: str = "일반") -> Dict:
        """키워드 기반 1-3 점수 분석"""
        text_lower = text.lower()

        positive_keywords, negative_keywords = self.get_effective_keywords(target_company, industry)
        negative_patterns = self.get_effective_patterns(industry)

        pos_matches = [kw for kw in positive_keywords if re.search(r'\b' + re.escape(kw) + r'\b', text_lower)]
        neg_matches = [kw for kw in negative_keywords if re.search(r'\b' + re.escape(kw) + r'\b', text_lower)]
        pattern_matches = [pat for pat in negative_patterns if re.search(pat, text_lower)]

        total_pos = len(pos_matches)
        total_neg = len(neg_matches) + len(pattern_matches) * 2  # 패턴은 2배 가중치

        print(f"키워드 분석 ({industry}): 긍정 {len(pos_matches)}, 부정 {len(neg_matches)}, 패턴 {len(pattern_matches)}")

        # 1-3 점수 계산
        total_signals = total_pos + total_neg
        if total_signals == 0:
            score = 2.0  # 중립
        else:
            # 긍정 우세도 계산: -1 (완전부정) ~ +1 (완전긍정)
            dominance = (total_pos - total_neg) / total_signals
            # 1-3 점수로 변환: 2 + dominance (1~3 범위)
            score = 2.0 + dominance

        return {
            "score": max(1.0, min(3.0, score)),  # 1-3 범위 보장
            "details": {
                "pos_matches": pos_matches,
                "neg_matches": neg_matches,
                "pattern_matches": pattern_matches,
                "total_pos": total_pos,
                "total_neg": total_neg,
                "dominance": (total_pos - total_neg) / max(1, total_signals)
            }
        }

    def load_model(self, model_key: str) -> bool:
        if model_key not in self.ensemble_models:
            return False

        try:
            model_info = self.ensemble_models[model_key]
            print(f"'{model_info['description']}' 모델 로딩 중...")

            if model_info["type"] == "pipeline":
                self.models[model_key] = pipeline(
                    "sentiment-analysis",
                    model=model_info["name"],
                    device=0 if self.device.type == "cuda" else -1
                )
            else:
                tokenizer = AutoTokenizer.from_pretrained(model_info["name"])
                model = AutoModelForSequenceClassification.from_pretrained(model_info["name"])
                model.to(self.device)
                model.eval()

                self.models[model_key] = {
                    "tokenizer": tokenizer,
                    "model": model,
                    "info": model_info
                }

            print(f"모델 로딩 완료: {model_key}")
            return True

        except Exception as e:
            print(f"모델 로딩 실패 ({model_key}): {e}")
            return False

    def load_all_models(self) -> List[str]:
        loaded_models = []
        for model_key in self.ensemble_models.keys():
            if self.load_model(model_key):
                loaded_models.append(model_key)
        return loaded_models

    def analyze_with_single_model(self, text: str, model_key: str) -> Dict:
        """단일 모델 1-3 점수 분석"""
        if model_key not in self.models:
            return None

        model_data = self.models[model_key]

        # Pipeline 모델
        if callable(model_data):
            try:
                max_length = 500
                chunks = [text[i:i+max_length] for i in range(0, len(text), max_length)] if len(text) > max_length else [text]

                positive_count = 0
                negative_count = 0

                for chunk in chunks:
                    try:
                        result = model_data(chunk)[0]
                        label = result.get("label", "").upper()
                        if label in ["POSITIVE", "POS"]:
                            positive_count += 1
                        elif label in ["NEGATIVE", "NEG"]:
                            negative_count += 1
                    except:
                        continue

                total = positive_count + negative_count
                if total == 0:
                    return {"score": 2.0}  # 중립

                # 긍정 비율 계산 후 1-3 점수로 변환
                positive_ratio = positive_count / total
                # positive_ratio: 0~1 → score: 1~3
                score = 1.0 + 2.0 * positive_ratio

                return {"score": score}

            except Exception:
                return {"score": 2.0}  # 중립

        # Transformer 모델
        else:
            tokenizer = model_data["tokenizer"]
            model = model_data["model"]

            chunks = [text[i:i+400] for i in range(0, len(text), 400)] if len(text) > 400 else [text]
            chunk_results = []
            for chunk in chunks:
                inputs = tokenizer(chunk, return_tensors="pt", truncation=True, max_length=512, padding=True)
                inputs = {k: v.to(self.device) for k, v in inputs.items()}

                with torch.no_grad():
                    outputs = model(**inputs)
                    probs = torch.softmax(outputs.logits, dim=1).cpu().numpy()[0]

                chunk_results.append(probs)

            avg_probs = np.mean(chunk_results, axis=0)

            positive_prob = avg_probs[0] if len(avg_probs) > 0 else 0
            negative_prob = avg_probs[1] if len(avg_probs) > 1 else 0

            total = positive_prob + negative_prob
            if total == 0:
                return {"score": 2.0}  # 중립

            # 긍정 비율 계산 후 1-3 점수로 변환
            positive_ratio = positive_prob / total
            score = 1.0 + 2.0 * positive_ratio

            return {"score": score}

    def ensemble_analysis(self, text: str) -> Dict:
        """앙상블 1-3 점수 분석"""
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
            return {"score": 2.0}  # 중립

        # 가중평균 계산
        total_weight = sum(data["weight"] for data in model_results.values())
        weighted_score = sum(
            data["score"] * data["weight"]
            for data in model_results.values()
        ) / total_weight

        return {
            "score": weighted_score,
            "model_details": {model_key: data["score"] for model_key, data in model_results.items()}
        }

    def analyze_sentiment(self, text: str, hint_company: str = None, reverse_order: bool = False) -> Dict:
        """감성 분석 (1-3 점수 체계)"""
        processed_text = re.sub(r'출처\s*[:：]\s*.*$', '', text, flags=re.MULTILINE)
        processed_text = re.sub(r'http[s]?://[^\s]+', '', processed_text)
        processed_text = re.sub(r'\s+', ' ', processed_text).strip()

        if not processed_text:
            return {"error": "분석할 텍스트가 없습니다."}

        # 강건성 테스트: 문장 순서 뒤바꾸기
        if reverse_order:
            processed_text = self.reverse_sentence_order(processed_text)
            print(f"문장 순서 뒤바꿈 적용됨")

        target_company, industry = self.extract_target_company(processed_text, hint_company)
        self.target_company = target_company

        # 각각 1-3 점수로 분석
        ensemble_result = self.ensemble_analysis(processed_text)
        keyword_result = self.smart_keyword_analysis(processed_text, target_company, industry)

        # 가중평균으로 최종 점수 계산
        keyword_weight = self.config["keyword_weight"]
        model_weight = self.config["model_weight"]

        final_score = (ensemble_result["score"] * model_weight +
                      keyword_result["score"] * keyword_weight)

        # 감성 라벨 결정
        if final_score >= 2.5:
            sentiment = "positive"
        elif final_score <= 1.5:
            sentiment = "negative"
        else:
            sentiment = "neutral"

        # 신뢰도 계산 (중립점 2에서의 거리)
        confidence = abs(final_score - 2.0) / 1.0  # 0~1 범위

        return {
            "sentiment": sentiment,
            "score": round(final_score, 4),
            "confidence": round(confidence, 4),
            "target_company": target_company,
            "industry": industry,
            "keyword_score": round(keyword_result["score"], 4),
            "model_score": round(ensemble_result["score"], 4),
            "keyword_analysis": keyword_result["details"],
            "model_details": ensemble_result.get("model_details", {}),
            "config": self.config,
            "reversed_order": reverse_order
        }


# 기본 설정 데이터 예시
DEFAULT_COMPANY_MAPPING = {
    "SK텔레콤": {
        "keywords": ["skt", "sk텔레콤", "sk telecom"],
        "industry": "통신",
        "positive_keywords": [],
        "negative_keywords": []
    },
    "신한금융": {
        "keywords": ["신한금융", "신한은행", "신한"],
        "industry": "금융",
        "positive_keywords": [],
        "negative_keywords": []
    },
    "삼성전자": {
        "keywords": ["삼성전자", "삼성", "galaxy", "갤럭시"],
        "industry": "전자",
        "positive_keywords": [],
        "negative_keywords": []
    },
    "데이터스트림즈": {
        "keywords": ["데이터스트림즈", "datastreams"],
        "industry": "IT",
        "positive_keywords": [],
        "negative_keywords": []
    }
}

DEFAULT_INDUSTRY_KEYWORDS = {
    "통신": {
        "negative": ["해킹", "유출", "침해", "보안사고", "개인정보유출"],
        "patterns": [r'통신.*?장애', r'네트워크.*?마비', r'기지국.*?문제']
    },
    "금융": {
        "negative": ["금융사고", "횡령", "부실", "연체", "손실"],
        "patterns": [r'금융.*?사고', r'예금.*?손실', r'대출.*?부실']
    },
    "전자": {
        "negative": ["리콜", "결함", "폭발", "화재"],
        "patterns": [r'제품.*?결함', r'배터리.*?폭발', r'리콜.*?결정']
    }
}


def print_system_info(analyzer: ScalableSentimentAnalyzer):
    """시스템 정보 출력"""
    print(f"\n=== 확장 가능한 감성분석 시스템 (1-3 점수) ===")
    print(f"로드된 모델: {list(analyzer.models.keys())}")
    print(f"지원 기업 수: {len(analyzer.company_mapping)}")
    print(f"지원 산업 수: {len(analyzer.industry_keywords)}")
    print(f"설정: {analyzer.config}")


def main():
    analyzer = ScalableSentimentAnalyzer()

    # 기본 데이터 로드
    analyzer.load_company_mapping(DEFAULT_COMPANY_MAPPING)
    analyzer.load_industry_keywords(DEFAULT_INDUSTRY_KEYWORDS)

    # 모델 로드
    loaded_models = analyzer.load_all_models()
    if not loaded_models:
        print("모델 로딩 실패")
        return

    print_system_info(analyzer)

    while True:
        print(f"\n1. 텍스트 분석")
        print(f"2. 기업 지정 분석")
        print(f"3. 강건성 테스트 (문장 순서 뒤바꿈)")
        print(f"4. 샘플 테스트")
        print(f"5. 종료")

        choice = input("\n선택하세요 (1-5): ").strip()

        if choice == "1":
            text = input("\n분석할 텍스트: ").strip()
            if text:
                result = analyzer.analyze_sentiment(text)
                if "error" in result:
                    print(f"오류: {result['error']}")
                else:
                    print(f"\n=== 분석 결과 ===")
                    print(f"대상: {result.get('target_company', 'N/A')} ({result.get('industry', 'N/A')})")
                    print(f"감성: {result['sentiment']} (점수: {result['score']}/3)")
                    print(f"신뢰도: {result['confidence']}")
                    print(f"키워드 점수: {result['keyword_score']}/3")
                    print(f"모델 점수: {result['model_score']}/3")
                    print(f"키워드 매칭: +{len(result['keyword_analysis']['pos_matches'])}, -{len(result['keyword_analysis']['neg_matches'])}, 패턴 {len(result['keyword_analysis']['pattern_matches'])}")

        elif choice == "2":
            company = input("대상 기업: ").strip()
            text = input("분석할 텍스트: ").strip()
            if text:
                result = analyzer.analyze_sentiment(text, company)
                print(f"\n'{company}' 관점 분석: {result['sentiment']} ({result['score']}/3)")

        elif choice == "3":
            text = input("\n분석할 텍스트: ").strip()
            if text:
                print("\n=== 일반 분석 ===")
                result1 = analyzer.analyze_sentiment(text, reverse_order=False)
                print(f"감성: {result1['sentiment']} (점수: {result1['score']}/3)")
                
                print("\n=== 문장순서 뒤바꿈 분석 ===")
                result2 = analyzer.analyze_sentiment(text, reverse_order=True)
                print(f"감성: {result2['sentiment']} (점수: {result2['score']}/3)")
                
                score_diff = abs(result1['score'] - result2['score'])
                print(f"\n점수 차이: {score_diff:.4f} (강건성: {'좋음' if score_diff < 0.5 else '보통' if score_diff < 1.0 else '약함'})")

        elif choice == "4":
            samples = [
                "SK텔레콤 해킹으로 개인정보 유출 피해 발생",
                "데이터스트림즈 대상 수상으로 주가 상승",
                "신한은행 금융사고로 고객 피해 확산"
            ]
            for sample in samples:
                result = analyzer.analyze_sentiment(sample)
                print(f"\n'{sample[:30]}...' → {result['sentiment']} ({result['score']}/3)")

        elif choice == "5":
            break


if __name__ == "__main__":
    main()