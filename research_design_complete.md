# 데이터 자산과 기업 가치에 관한 통합 연구 설계
## Integrated Research Design for Data Asset and Corporate Value Analysis

---

## 1. 개요 (Overview)

본 연구는 한국 상장기업의 데이터 자산 활동이 기업 가치에 미치는 영향을 분석하는 것을 목적으로 합니다. 연구는 DART(금융감독원 전자공시시스템) 및 기타 시장/뉴스 데이터 소스를 활용하여 실증 분석을 수행합니다.

---

## 2. 데이터 소스 및 수집 방법론 (Data Sources and Collection Methodology)

### 2.1 재무 데이터 (Financial Data) - DART API

**데이터 소스**: 금융감독원 전자공시시스템(DART)  
**수집 방법**: DART Open API 활용 (simple_api_dart_crawler.py)  
**수집 기간**: 2014년부터 현재까지 (과거 10년치)  

#### 주요 수집 변수

| 변수명 | XBRL 태그 | 한글 계정명 | 정의 |
|--------|-----------|-------------|------|
| $Assets_{i,t}$ | $\mathrm{ifrs\text{-}full\_Assets}$ | 자산총계, 총자산 | 기업 $i$의 시점 $t$에서의 총 자산 |
| $Liabilities_{i,t}$ | $\mathrm{ifrs\text{-}full\_Liabilities}$ | 부채총계, 총부채 | 기업 $i$의 시점 $t$에서의 총 부채 |
| $Equity_{i,t}$ | $\mathrm{ifrs\text{-}full\_Equity}$ | 자본총계, 총자본 | 기업 $i$의 시점 $t$에서의 총 자본 |
| $Revenue_{i,t}$ | $\mathrm{ifrs\text{-}full\_Revenue}$ | 매출액, 영업수익 | 기업 $i$의 시점 $t$에서의 총 매출액 |
| $ProfitLoss_{i,t}$ | $\mathrm{ifrs\text{-}full\_ProfitLoss}$ | 당기순이익, 순이익 | 기업 $i$의 시점 $t$에서의 당기순이익 |

#### 보고서 우선순위 및 수집 제약사항

| 우선순위 | reprt_code | 보고서명 | 기준일 |
|----------|------------|----------|--------|
| 1 | `11013` | 1분기보고서 | 03-31 |
| 2 | `11012` | 반기보고서 | 06-30 |
| 3 | `11014` | 3분기보고서 | 09-30 |
| 4 | `11011` | 사업보고서 | 12-31 |

#### 숫자 값 전처리 (safe_parse_num 함수)

$$
\mathrm{ParsedValue} =
\begin{cases}
\text{None} & \text{if } x \text{ is None or in } \{\text{nan},\ \text{none},\ \text{null},\ \text{-}\} \\
-|x| & \text{if } x \text{ is in format } (\text{value}) \\
\mathrm{float}(x) & \text{otherwise}
\end{cases}
$$

### 2.2 시장 데이터 (Market Data) - KRX

**데이터 소스**: 한국거래소(KRX) via pykrx 라이브러리  
**수집 방법**: krx_quarterly_fetch.py 스크립트  
**수집 주기**: 분기말 (3월, 6월, 9월, 12월 말일)  

#### 주요 수집 변수

| 변수명 | 원본 컬럼명 | 정의 | 단위 |
|--------|-------------|------|------|
| $MarketCap_{i,t}$ | 시가총액 | 종가 $\times$ 상장주식수 | 원(KRW) |
| $Shares_{i,t}$ | 상장주식수 | 유통 주식 총량 | 주 |
| $Price_{i,t}$ | 종가 | 분기말 최종 거래가격 | 원(KRW) |
| $Volume_{i,t}$ | 거래량 | 일일 거래 주식량 | 주 |
| $Value_{i,t}$ | 거래대금 | 일일 거래 총액 | 원(KRW) |

#### 종목코드 표준화

$$
\text{Standardized Stock Code} = \mathrm{zero\text{-}fill}\bigl(\mathrm{extract\_digits}(S),\ 6\bigr)
$$

여기서:
- $\mathrm{extract\_digits}(S)$: 문자열 $S$에서 모든 숫자를 추출하여 연결하는 함수
- $\mathrm{zero\text{-}fill}(C, L)$: 문자열 $C$의 길이가 $L$보다 작을 경우, 왼쪽에 '0'을 채워 $L$ 길이로 만드는 함수

**예시**:
- 'A005930' $\rightarrow$ '005930'
- '5930' $\rightarrow$ '005930'

### 2.3 뉴스 감성 데이터 (News Sentiment Data) - 네이버 API

**데이터 소스**: 네이버 뉴스 검색 API  
**수집 방법**: improved_api_naver_crawler.py 스크립트  
**수집 기간**: 2015-01-01 ~ 2024-12-31 (10년간)  

#### 데이터 자산 키워드 분류체계

| 카테고리 | 키워드 개수 | 주요 키워드 예시 |
|----------|-------------|------------------|
| 기본 | 11개 | 데이터, 빅데이터, AI, 인공지능, 머신러닝 |
| 인프라 | 11개 | 클라우드, 데이터센터, API, GPU, NPU |
| 활용분야 | 13개 | 자연언어처리, 컴퓨터비전, IoT 데이터 |
| 무형연계자산 | 4개 | 소프트웨어 자산, 지식재산권, 디지털 트윈 |
| 신흥영역 | 9개 | 생성형AI, LLM, 데이터 거버넌스 |

#### 텍스트 전처리 과정

| 처리 단계 | 적용 공식/방법 | 목적 |
|-----------|----------------|------|
| HTML 태그 제거 | `re.sub('<.*?>', '', text)` | 순수 텍스트 추출 |
| HTML 엔티티 변환 | `text.replace(entity, char)` | 특수문자 정규화 |
| 공백 정규화 | `re.sub(r'\s+', ' ', text).strip()` | 구조 표준화 |
| 본문 길이 제한 | `get_text(...)[:5000]` | 처리 효율성 확보 |

---

## 3. 변수 정의 및 측정

### 3.1 종속변수

#### 3.1.1 시장가치 대 장부가치 비율 (MBV)
기업의 시장 가치 대 장부 가치 비율로 기업 가치의 대리변수로 활용:

$$
MBV = \frac{\mathrm{market\_cap\_krw}}{\mathrm{자본총계}}
$$

로그 변환을 통한 정규성 확보:

$$
log\_MBV = \ln(MBV)
$$

### 3.2 독립변수

#### 3.2.1 뉴스 감성 변수
연간 뉴스 기사를 기반으로 다음과 같은 감성 지표를 계산:

- 긍정 확률 평균 (positive\_mean): 연간 뉴스 기사의 긍정 확률 평균  
- 감성 점수 (sentiment\_score):  
  $$
  SentimentScore = PositiveMean - NegativeMean
  $$
- 감성 변동성 (sentiment\_volatility):  
  $$
  SentimentVolatility = \operatorname{std}(PositiveProbability)
  $$
- 감성 범위 (sentiment\_range):  
  $$
  SentimentRange = PositiveMax - PositiveMin
  $$

#### 3.2.2 회계 정보량 그룹 (InfoGroup)
기업의 회계 정보량에 따른 더미 변수:

- 하위권 기업: 1  
- 상위권 기업: 0

### 3.3 통제변수

#### 3.3.1 재무 변수
- 레버리지 (Leverage)  
  $$
  Leverage = \frac{\mathrm{부채총계}}{\mathrm{자산총계}}
  $$

- 총자산순이익률 (ROA)  
  $$
  ROA = \frac{\mathrm{당기순이익}}{\mathrm{자산총계}}
  $$

- 자기자본순이익률 (ROE)  
  $$
  ROE = \frac{\mathrm{당기순이익}}{\mathrm{자본총계}}
  $$

- 총자산회전율 (Asset Turnover)  
  $$
  Asset\_Turnover =
  \begin{cases}
  \dfrac{\mathrm{매출액}}{\mathrm{자산총계}} & \text{if 매출액} > 0 \\
  0 & \text{otherwise}
  \end{cases}
  $$

- 매출액순이익률 (Profit Margin)  
  $$
  Profit\_Margin =
  \begin{cases}
  \dfrac{\mathrm{당기순이익}}{\mathrm{매출액}} & \text{if 매출액} > 0 \\
  \mathrm{NaN} & \text{otherwise}
  \end{cases}
  $$

#### 3.3.2 기업 규모 변수
로그 총자산 (log\_assets)  
$$
log\_assets = \ln(\mathrm{total\_assets})
$$

#### 3.3.3 뉴스 관련 통제 변수
- 로그 뉴스 개수 (log\_news\_count)  
  $$
  log\_news\_count = \ln(\mathrm{news\_count})
  $$

- 뉴스 강도 (news\_intensity)  
  $$
  NewsIntensity = \frac{\mathrm{news\_count}}{\max(\mathrm{news\_count})}
  $$

- 뉴스 품질 더미 (news\_quality)  
  $$
  NewsQuality = I\bigl(\mathrm{news\_count} \ge 5\bigr)
  $$

### 3.4 데이터 자산 관련 변수
데이터 자산 관련성은 텍스트 내 키워드 출현 여부를 통해 측정되며,  
5개 하위 카테고리로 분류된 총 40개의 구체적인 키워드 집합을 정의:

1. 사전 필터링: 기사 제목과 요약 텍스트에 키워드 포함 여부 확인  
2. 본문 포함 최종 매칭: 제목 + 요약 + 본문 전체 텍스트에서 키워드 재검색  
3. 매칭 결과 저장: 최종 키워드 매칭된 기사에 대해 matched\_categories 변수 생성

---

## 4. 표본 선정 및 필터링 기준 (Sample Selection and Filtering Criteria)

### 4.1 기업 표본 선정

- **대상 시장**: KOSPI, KOSDAQ, KONEX 전체 상장기업
- **수집 방법**: PyKRX 라이브러리의 `get_all_listed_companies()` 함수
- **식별자**: ticker(종목코드)를 기본 키로 사용
- **필터링**: 추가 산업별/규모별 필터링 없이 전체 상장기업 포함

### 4.2 시계열 표본 기준

- **재무 데이터**: 현재 연도 기준 과거 10년치
- **시장 데이터**: 2014년부터 현재까지, 분기말 기준
- **뉴스 데이터**: 2015-01-01 ~ 2024-12-31

### 4.3 데이터 품질 필터링

- **상장폐지 기업**: 현재 시점 기준 2년 이내 상장폐지 기업 제외
- **API 호출 실패**: 연속 3회 실패 시 해당 기업 데이터 수집 중단
- **결측치 처리**: 핵심 재무변수 결측 시 해당 관측치 제외

---

## 5. 데이터 전처리 및 통계적 가공 (Data Preprocessing and Statistical Treatment)

### 5.1 이상치 제거

극단치 처리를 위한 1%, 99% 분위수 기준 적용:

$$
X_{\mathrm{winsorized}} =
\begin{cases}
Q_{0.01} & \text{if } X < Q_{0.01} \\
X & \text{if } Q_{0.01} \le X \le Q_{0.99} \\
Q_{0.99} & \text{if } X > Q_{0.99}
\end{cases}
$$

### 5.2 표준화 (Standardization)

연속변수에 대한 Z-score 표준화:

$$
X_{\mathrm{std}} = \frac{X - \mu_X}{\sigma_X}
$$

### 5.3 시계열 정렬 및 매칭

- **재무-시장 데이터 매칭**: firm\_id와 period\_yyyyq 기준
- **뉴스 데이터 매칭**: ticker와 분기별 집계 기준
- **시차 구조**: 뉴스 감성은 해당 분기, 재무/시장 데이터는 분기말 기준

---

## 6. 계량경제학적 모형 (Econometric Models)

### 6.1 기본 OLS 모형

$$
\mathrm{Tobin's\ Q}_{i,t} =
\beta_0
+ \beta_1 Sentiment_{i,t}^{\mathrm{std}}
+ \beta_2 InfoGroup_i
+ \beta_3 \bigl(Sentiment_{i,t}^{\mathrm{std}} \times InfoGroup_i\bigr)
+ \beta_4 Size_{i,t}^{\mathrm{std}}
+ \beta_5 Leverage_{i,t}^{\mathrm{std}}
+ \sum D_t
+ \varepsilon_{i,t}
$$

여기서:  
- $InfoGroup_i \in \{0,1\}$: 정보집약적 기업 더미  
- $D_t$: 연도 더미 변수  
- $\varepsilon_{i,t}$: 오차항  

### 6.2 고정효과 모형 (Fixed Effects Model)

$$
\bigl(
    \mathrm{Tobin's\ Q}_{i,t} - \overline{\mathrm{Tobin's\ Q}_i}
\bigr)
=
\beta_1 \bigl(
    \mathrm{Sentiment}_{i,t}^{\mathrm{std}} - \overline{\mathrm{Sentiment}_i^{\mathrm{std}}}
\bigr)
+
\beta_2 \bigl(
    \mathrm{Sentiment}_{i,t}^{\mathrm{std}} \times \mathrm{InfoGroup}_i
    - \overline{\mathrm{Sentiment}_i^{\mathrm{std}} \times \mathrm{InfoGroup}_i}
\bigr)
+
\sum_k \bigl(
    X_{k,i,t} - \overline{X_{k,i}}
\bigr)
+
\bigl(
    D_t - \overline{D_t}
\bigr)
+
\bigl(
    \varepsilon_{i,t} - \overline{\varepsilon_{i}}
\bigr)
$$

개체별 고정효과를 제거한 집단 내(within) 추정량 사용

### 6.3 혼합효과 모형 (Mixed Effects Model)

$$
\mathrm{Tobin's\ Q}_{i,t} =
\beta_0
+ \beta_1 Sentiment_{i,t}^{\mathrm{std}}
+ \beta_2 InfoGroup_i
+ \beta_3 \bigl(Sentiment_{i,t}^{\mathrm{std}} \times InfoGroup_i\bigr)
+ \sum_{k=4}^{6} \beta_k X_{k,i,t}
+ u_i
+ \varepsilon_{i,t}
$$

여기서 $u_i \sim N(0,\, \sigma^2)$는 개체별 임의효과

---

## 7. 통계적 진단 및 검증 (Statistical Diagnostics and Validation)

### 7.1 모형 진단 검정

- **정규성 검정**: Shapiro-Wilk test, Jarque-Bera test
- **등분산성 검정**: Breusch-Pagan test, White test  
- **자기상관 검정**: Durbin-Watson test, Wooldridge test for panel data
- **다중공선성 진단**: VIF(Variance Inflation Factor) < 10.0

### 7.2 견고성 검증 (Robustness Tests)

- **군집 강건 표준오차 (Cluster-Robust Standard Errors)** 적용
- **대안적 종속변수**: ROA, ROE 등 회계기반 성과지표 활용
- **하위표본 분석**: 산업별, 규모별 분할 분석

### 7.3 통계적 기준값

| 통계량 | 기준값 | 적용 목적 |
|--------|--------|-----------|
| 유의수준 ($\alpha$) | 0.01, 0.05, 0.10 | 통계적 유의성 검정 |
| VIF 임계값 | 10.0 | 다중공선성 진단 |
| DW 통계량 기준 | $1.5 < DW < 2.5$ | 자기상관 진단 |
| 윈저라이징 분위수 | 1%, 99% | 극값 처리 |

---

## 8. 시스템 파라미터 및 제약사항 (System Parameters and Constraints)

### 8.1 API 및 수집 제약사항

| 구분 | 파라미터 | 설정값 | 설명 |
|------|----------|--------|------|
| DART API | 일일 요청 한도 | 20,000건 | API 호출 제한 |
| DART API | 검색 기간 제한 | 3개월 | corp_code 없이 검색 시 |
| 네이버 API | 기업당 최대 기사수 | 500개 | 데이터 균형성 확보 |
| 네이버 API | 요청 지연시간 | 0.1초 | 서버 부하 방지 |
| KRX 수집 | 호출 간 대기시간 | 0.6초 | 안정적 데이터 수집 |
| KRX 수집 | 최대 재시도 횟수 | 3회 | 네트워크 오류 대응 |

### 8.2 데이터 처리 제약사항

| 구분     | 제약사항    | 설정값    | 목적       |
| ------ | ------- | ------ | -------- |
| 본문 길이  | 최대 길이   | 5,000자 | 처리 효율성   |
| 동시 요청수 | 비동기 처리  | 20개    | 서버 부하 조절 |
| 타임아웃   | 요청 대기시간 | 15초    | 안정성 확보   |
| 검색어 변형 | 최대 개수   | 3개     | 검색 포괄성   |

---

## 9. 뉴스 데이터 수집 세부 기준 (Detailed News Data Collection Criteria)

### 9.1 검색어 생성 방식

| 구분     | 기준/공식 | 설명 |      |     |                          |                      |
| ------ | -------------------------------------- | ------------------- | ---- | --- | ------------------------ | -------------------- |
| 검색어 생성 | `company_name` 및 `re.sub(r'주식회사$|㈜|주식회사|（주）|(주)', '', company_name)` | 정식명칭 + 약칭으로 검색 범위 확대 |
| 중복 제거  | `link in self.collected_links_session` | 기사 링크(URL) 기준 중복 방지 |      |     |                          |                      |

### 9.2 키워드 매칭 프로세스

1. **사전 필터링**: 기사 제목과 요약 텍스트에 키워드 포함 여부 확인
2. **본문 포함 최종 매칭**: 제목 + 요약 + 본문 전체에서 키워드 재검색
3. **매칭 결과 저장**: `matched_categories` 변수 생성 및 `keyword_matches` 테이블 저장

---

## 10. 연구의 한계 및 고려사항 (Limitations and Considerations)

### 10.1 데이터 소스 한계

* **pykrx 라이브러리**: 웹 스크래핑 기반으로 KRX 웹사이트 구조 변경에 민감
* **네이버 뉴스 API**: 검색 결과 상위 1,000개 기사로 제한
* **DART API**: 비정형 텍스트 데이터의 정형화 한계

### 10.2 방법론적 한계

* **키워드 기반 감성분석**: 맥락적 의미 파악 제한
* **인과관계 추론**: 횡단면적 상관관계 분석의 한계
* **표본 선택 편의**: 상장기업으로 한정된 분석 범위

### 10.3 통계적 고려사항

* **패널 데이터 불균형**: 기업별 관측 기간 차이
* **생존 편의**: 상장폐지 기업 데이터 부족
* **시점 효과**: 거시경제 환경 변화의 영향

---

## 11. 결론 (Conclusion)

이상의 연구 설계는 데이터 자산과 기업 가치 간의 관계를 규명하기 위한 체계적이고 포괄적인 분석 틀을 제공합니다. 다차원적 데이터 소스의 결합과 엄격한 통계적 전처리, 그리고 견고한 모형 설계를 통해 실증적 분석의 신뢰성을 확보합니다.

본 연구 설계의 주요 강점은 다음과 같습니다:

1. **다차원적 데이터 통합**: 재무, 시장, 뉴스 감성 데이터의 체계적 결합
2. **엄격한 전처리 기준**: 윈저라이징, 표준화 등 통계적 엄밀성 확보
3. **포괄적 모형 설계**: OLS, 고정효과, 혼합효과 모형을 통한 견고성 검증
4. **명확한 변수 정의**: 재현 가능한 연구를 위한 상세한 변수 구성 방법 제시

본 연구 설계를 통해 한국 상장기업의 데이터 자산 활동이 기업 가치에 미치는 영향에 대한 실증적 증거를 제공하고, 나아가 데이터 경제 시대의 기업 전략 수립에 유의미한 시사점을 제시할 수 있을 것으로 기대합니다.

---

**참고문헌 (References)**

* 금융감독원 전자공시시스템 (DART): https://dart.fss.or.kr/
* 금융감독원 전자공시시스템 OpenDART 개발가이드: https://opendart.fss.or.kr/guide/main.do?apiGrpCd=DS001
* pykrx GitHub Repository: https://github.com/sharebook-kr/pykrx
* 한국거래소(KRX) 시장 데이터 시스템: https://data.krx.co.kr/
