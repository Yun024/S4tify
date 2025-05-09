# 음악 트렌드 분석을 위한 데이터 파이프라인 구축
프로그래머스 데브코스 데이터 엔지니어링[5기] **Final Project** <br>
<b>**Team: S4tify** | 2025.02.24 ~ 2025.03.20</b>

# 목차
1. 프로젝트 개요
2. 프로젝트 아키텍처
3. Infra 구성
4. 데이터 파이프라인
5. 결과물(대시보드)
6. 프로젝트 회고

# 1. 프로젝트 개요
## 프로젝트 소개
#### 주제
- 음악 트렌드 분석을 위한 데이터 파이프라인 구축
#### 목표
- 실시간 음악 스트리밍 데이터와 국내 및 국외 차트 데이터를 수집
- 수집한 데이터를 처리하여 스트리밍 데이터와 주간 차트 그리고 음악 메타 정보를 적재하는 데이터 파이프라인 구축
#### 선정배경
- **실시간 음악 소비 패턴 분석의 필요성**
    - 현재 음악 산업은 **실시간 데이터 분석이 중요한 요소**로 작용하고 있음
    - 국내외 음악 플랫폼 사이트의 데이터가 빠르게 변동되며, 음악 산업의 의사결정에도 영향을 미침
- **대용량 데이터의 효과적인 처리 및 분석 요구 증가**
    - 일일 스트리밍 이벤트가 대용량 발생하며, 이를 분석하기 위한 **대용량 데이터 처리가 가능한 분산 처리 시스템** 이 필수적
    - 기존의 정적 데이터 분석 방식이 아닌 **실시간 스트리밍 데이터 처리 및 적재가 가능한 시스템을** 요구
- **비즈니스 실무에서 활용 가능한 데이터 파이프라인 구축**
    - 실제 기업 환경에서 **실시간 데이터 분석을 활용하여 트렌드 예측 및 마케팅 전략을 최적화**하는 것이 중요
    - **클라우드 및 빅데이터 기술을 결합한 ETL/ELT 파이프라인**을 구축
    - 실무에서도 적용할 수 있는 데이터 처리 및 분석 구조로 설계

## 팀 구성 및 역할
  + `윤여준`: 시스템 아키텍처 설계, AWS 인프라 구축, EC2 서버 개발, 개발 환경 통합
  + `부상혁`: 스트리밍 데이터 생성과 수집 및 처리, 인프라 구성, 데이터 시각화
  + `신예린`: spotify 웹 크롤링 및 api를 활용한 ETL 파이프라인 구축 및 ELT 파이프라인 구축, 데이터 시각화
  + `윤병훈`: 데이터 플로우 아키텍처 설계, 국내 차트 데이터 ETL 파이프라인 구축 및 ELT 파이프라인 구축, 데이터 시각화

## 프로젝트 수행 절차 및 방법
![image](https://github.com/user-attachments/assets/33b6950c-2d28-4f16-b473-bf8b70438d5e)

## 활용 기술 및 프레임 워크
![image](https://github.com/user-attachments/assets/ef139844-e0b2-4c90-98e0-fef49b88cb7f)

# 2. 프로젝트 아키텍처
## 시스템 아키텍처
![image](https://github.com/user-attachments/assets/b257c95f-1ff3-466e-95b9-893b96472d08)

## 데이터 파이프라인 아키텍처(데이터 플로우)
![image](https://github.com/user-attachments/assets/137992e9-9a83-4c68-851e-3013a3aae34d)

# 3. Infra 구성
## Docker 구성
### Apache Airflow
![image](https://github.com/user-attachments/assets/82f90576-afcd-44e5-8e4d-32ccc987ec16)
※ apache-airflow-2.9.1 버전 사용
- curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.9.1/docker-compose.yaml'
<br>

### Apache Kafka & Eventsim
![image](https://github.com/user-attachments/assets/8a0309f6-411c-4ead-b818-f23b1824e104)

## AWS 구성
### EC2
![image](https://github.com/user-attachments/assets/3993c8c1-b4d0-4d8e-81c4-0627344f32fa)

### VPC
![image](https://github.com/user-attachments/assets/3d11a8e6-b08a-47fa-8504-5075c927b5d9)

### S3
![image](https://github.com/user-attachments/assets/b67d3295-1a56-4200-9f86-33807a3593a1)

## Github Actions 구성
![image](https://github.com/user-attachments/assets/c1a42d12-cd84-43f7-bb73-3ff841047e09)
#### 주요기능
- **PR 트리거** : PR이 main 브랜치로 병합될 때 자동실행
- **Python 환경 설정** :Python 3.10 환경을 자동으로 설정
- **코드 포맷팅 수행** : black, isort, autopop8을 실행하여 코드스타일 정리
- **자동 커밋&푸시** : 변경 사항이 있을 경우 PR 브랜치에 자동 커밋 & 푸시

# 4. 데이터 파이프라인
## Bronze Medallion
<details>
    <summary> 국외 차트 데이터 </summary>  
  
    데이터 수집
    - Spotify Website
    - Spotify Artist API
    - Spotify Artist Track API
    
    데이터 변환
    - 수집된 데이터를 모두 CSV 형식으로 변환하여 저장
      + Spotify Global Top 50 chart의 경우 1위부터 50위까지 순차적 저장 → 랭킹 컬럼 추가
      + potify Global Top 50 Chart의 artist 컬럼과 artist info의 genre 컬럼은 리스트 형태로 저장
        
    데이터 적재
    - CSV로 변환된 데이터를 AWS S3 Bucket에 저장
       + 기존 파일이 있을 경우 덮어쓰기 수행
</details>

<details>
    <summary>국내 차트 데이터</summary>
    
    데이터 수집
    - 곡 제목, 아티스트, 순위 등 핵심 데이터 확보
    - Spotify API를 활용하여 아티스트의 장르 정보를 검색 및 추가
    - DATE 컬럼을 추가하여 데이터 수집 날짜 기록
  
    데이터 변환
    - JSON 형식으로 저장된 데이터를 CSV 형식으로 변환
    - csv.writer를 활용하여 모든 데이터를 자동으로 따옴표 처리하여 저장
    - 장르 데이터는 리스트 형태로 저장
    - DAG 실행 날짜를 컬럼에 추가
  
    데이터 적재
    - S3Hook을 활용하여 변환된 CSV 파일을 지정된 S3 버킷에 업로드
    - 기존 파일이 존재할 경우, 덮어쓰기 수행
</details>

<details>
    <summary>스트리밍 데이터</summary>
    
    데이터 수집
    - Eventsim을 사용하여 음악 스트리밍 데이터 생성 
      + 음악 스트리밍 웹 사이트(Spotify)의 사용자 데이터 시뮬레이션
      + 실제 데이터와 유사한 이벤트 로그를 지정된 기간만큼 생성 가능
    
    데이터 변환
    - Kafka Topic 생성: Kafka Admin Client를 사용해 지정된 토픽이 없으면 새로 생성
    - Eventsim 실행 대기: Eventsim이 동작하는 컨테이너가 실행될 때 까지 확인하고, 실행되면 해당 컨테이너 ID 추적
    - Docker 컨테이너 로그 스트리밍: Eventsim이 실시간으로 생성한 로그 데이터 중 JSON 데이터만 필터링
    - Kafka Broker로 전송: 필터링된 JSON 데이터를 Kafka Producer를 통해 Kafka Broker로 전송
  
    데이터 적재
    - S3 Sink Connector 설정 파일 생성 및 Kafka Connect API를 통해 S3 Sink Connector 등록
    - Kafka에 적재된 데이터를 S3 버킷으로 자동 저장
</details>

## Silver Medallion
<details>
    <summary>국외 차트 데이터</summary>
  
    데이터 수집
    - S3 Bucket에 저장 되어 있는 CSV 파일을 Spark Dataframe으로 read 
      + 일관된 형식으로 읽어올 수 있도록 각 CSV 파일에 형식에 맞춘 데이터 스키마 정의 → 데이터 형식 명시
      + Array로 저장된 컬럼의 경우 문자열로 변환하여 읽어온 뒤 다시 Array로 변경
  
    데이터 변환
    - 중복 데이터 제거, 테이블 join, 날짜 데이터 컬럼 추가 
    - 파이썬 스크립트를 통해 join된 Dataframe 정보를 기반, 노래 장르 API 요청 및 컬럼 추가 
  
    데이터 적재
    - 테이블을 적재하기 전 해당 테이블이 있는지 확인, 만약 없다면 테이블 생성 진행
    - write_pandas함수를 사용하여 snwoflake 테이블에 적재 
</details>

<details>
    <summary>국내 차트 데이터</summary>
  
    데이터 수집
    - S3 버킷에서 데이터를 읽어 Spark DataFrame API로 변환 및 처리
    - 여러 차트 소스를 병합, genre, date 등의 추가 컬럼 포함
  
    데이터 변환
    - Spark의 DataFrame API를 활용하여 CSV 파일로 저장된 차트 데이터를 읽고, 필요한 변환 작업을 수행
    - 테이블 존재 여부 확인 후 필요 시 생성
    - 컬럼별 적절한 데이터 유형 지정 및 SQL 쿼리 작성
  
    데이터 적재
    - 변환된 데이터를 Snowflake의 RAW_DATA 스키마에 삽입
    - 문자열 및 NULL 값 처리로 데이터 정합성 유지
</details>

<details>
    <summary>스트리밍 데이터</summary>
  
    데이터 수집
    - Airflow의 SparkSubmitOperator를 활용해 S3에서 지정된 날짜의 이벤트 데이터 동적 추출
  
    데이터 변환
    - 필요한 컬럼(song, artist, location 등)만 유지하고, 불필요한 이벤트 제거
    - 결측값(NULL) 처리 및 데이터 정제
  
    데이터 적재
    - Snowflake 임시 테이블에 데이터 저장 후 MERGE INTO를 통해 UPSERT 수행
    - 기존 데이터가 존재하면 업데이트, 없으면 신규 삽입

    리소스 정리
    - MERGE 완료 후 임시 테이블 제거하여 불필요한 리소스 정리
</details>

## Gold Medallion
<details>
    <summary>국외 차트 데이터</summary>
  
    데이터 수집
    - Snowflake에 저장되어 있는 테이블을 Spark Dataframe으로 read 
      + format을 snowflak로 지정하여 spark로 테이블을 load
      
    데이터 변환
    - filter함수를 사용하여 오늘 날짜의 데이터만 read
    - explode 함수를 사용, array 형식이었던 데이터를 펼치고 group by 및 count 진행 
    - date_time 컬럼 추가
 
    데이터 적재
    - 테이블을 적재하기 전 해당 테이블이 있는지 확인, 만약 없다면 테이블 생성 진행
    - spark-snowflake connector를 사용하여 생성한 spark dataframe을 snowflake 테이블에 적재
</details>

<details>
    <summary>국내 차트 데이터</summary>
  
    데이터 수집
    - RAW_DATA.music_charts 테이블에서 최신 데이터 필터링
    - 1분 대기 시간 설정으로 안정적인 데이터 적재 보장
    - RAW_DATA 스키마에서 데이터를 가져와 ANALYTICS 스키마로 변환 및 저장

    데이터 변환
    - 최신 차트 데이터를 정제하고 장르·아티스트·신규 진입 곡 트렌드 분석
    - 순위 변동·1위 곡 유지 기간·상위 10위 곡 안정성 평가 등 차트 흐름 분석
    - 분석 결과를 ANALYTICS 스키마에 저장하여 비즈니스 인사이트 제공
  
    데이터 적재
    - 분석 결과를 ANALYTICS 스키마에 저장
    - Preset.io 등 대시보드 툴을 활용한 시각화 및 보고서 작성
</details>

<details>
    <summary>스트리밍 데이터</summary>
  
    데이터 수집
    - TriggerDagRunOperator를 사용하여 eventsim_ETL DAG 실행
    - Snowflake EVENTSIM_LOG 테이블에서 데이터를 읽어 Spark DataFrame으로 변환
  
    데이터 변환
    - 노래별·아티스트별 재생 횟수 집계 (groupBy(), count(), orderBy())
    - 컬럼명 변경 및 정렬하여 분석용 데이터 생성 
  
    데이터 적재
    - 변환된 데이터를 Snowflake EVENTSIM_SONG_COUNTS 테이블에 덮어쓰기
</details>

# 5. 결과물(대시보드)
![image](https://github.com/user-attachments/assets/b047c2b7-178d-4fa1-8d2b-af9fa568f666)

# 6. 프로젝트 회고
## 프로젝트 결과 및 성과
#### 효율적인 데이터 파이프라인 구축
+ Kafka Connect를 사용하여 **Kafka Consumer를 개발하는 부담을 줄이고, 보다 안정적인 데이터 적재 환경을 조성**
+ Airflow를 활용하여 **스트리밍 데이터와 차트 데이터를 자동으로 수집, 변환, 적재하는 ETL/ELT 파이프라인**을 구축
#### 확장성을 고려한 설계
+ 기존 Consumer 기반 적재 방식에서 Kafka Connect 기반의 적재방식으로 변경하여 Snowflake로 손쉽게 확장 가능하도록 개선
+ S3 → Snowflake → Preset까지 이어지는 **단계적인 데이터 적재 구조를 설계하여 다양한 데이터 활용 가능성 확보**
#### 협업과 DevOps 경험
+ **GitHub Actions 및 Docker를 활용하여 CI/CD 파이프라인을 구축**, 개발된 DAG 및 Spark Job이 자동으로 실행되도록 설정
+ 팀 내 코드 리뷰 및 역할 분배를 통해 **체계적인 협업 진행**
#### 실무에 가까운 프로젝트 경험
+ Kafka, Spark, Snowflake 등의 빅데이터 기술을 직접 사용하며 **대규모 데이터 처리 및 운영 경험을 쌓을 수 있었음**
+ 데이터 엔지니어링 및 데이터 아키텍처 설계에 대한 **이해도를 높이는 계기가 됨**

## 활용 방안 및 기대효과
#### **음악 스트리밍 서비스의 실시간 트렌드 분석**
- 실시간으로 수집된 스트리밍 데이터를 기반으로 **어떤 아티스트와 곡이 가장 많이 재생되는지** 등의 데이터 분석
- **사용자의 청취 패턴을 기반으로 개인 맞춤형 음악 추천 모델 개발 가능**
- **예측 분석을 활용하여 다음 주의 음악 차트를 미리 예측**하는 AI 기반 서비스로 확장 가능

#### **음원 차트 및 마케팅 전략 수립 지원**
- 국내외 음악 차트 데이터를 종합하여 **현재 가장 핫한 아티스트 및 트랙을 분석**
- 특정 아티스트의 **시간대 및 플랫폼 별 인기 트렌드** 등을 분석하여 마케팅 전략 수립 가능
- 신곡이 발매된 후의 **초기 반응을 실시간으로 분석하여 마케팅 효과를 최적화**

#### **스트리밍 데이터 기반 음악 산업 인사이트 제공**
- 특정 아티스트의 곡이 **어떤 국가, 지역에서 가장 많이 재생되고 있는지 시각화**하여 글로벌 시장에서의 반응 분석
- 각 플랫폼의 차트 데이터를 비교하여, **어떤 서비스에서 특정 장르가 더 인기 있는지 데이터 기반의 평가** 가능
- 데이터 분석 통해 **음악 제작사 및 음원 유통사가 시장 전략을 수립할 수 있도록 인사이트 제공**

#### **실무에서 활용 가능한 데이터 파이프라인 및 클라우드 아키텍처 제공**
- 최신 클라우드 및 빅데이터 기술을 활용하여 **대규모 실시간 데이터 처리 아키텍처를 설계**
- **데이터 수집(ETL), 적재, 분석(ELT)까지 자동화된 파이프라인을 구축하여 기업에서도 바로 활용할 수 있도록 구성**
- 실시간 대시보드를 통해 **비즈니스 의사결정을 위한 데이터 시각화 가능**

## 향후 개선할 점 및 확장 방향성
#### 개인화 추천 시스템 구현 필요
+ 초기 기획 단계에서 Spark ML을 활용한 개인화 음악 추천 시스템 구축이 예정되어 있었으나, 일정 및 리소스 부족으로 구현실패
+ 향후 Spark ML 또는 Snowflake ML을 활용하여 사용자의 음악 감상 패턴을 분석하고
+ 협업 필터링(Collaborative Filtering) 또는 콘텐츠 기반 필터링(Content-Based Filtering) 기법을 적용한 추천 시스템 구현

#### 데이터 활용성 및 서비스 연계 확대
+ 현재는 Snowflake에 적재된 데이터를 SQL 기반으로만 분석 가능
+ 이후에는 **BI 도구(Superset, Power BI 등)** 를 연계하여 대시보드를 구축
+ 외부 연동을 위한 API 엔드포인트 제공을 통해 다른 서비스와의 연계 가능성을 확대할 필요가 있음

#### 대용량 데이터 환경에 대한 테스트 및 최적화
+ Eventsim을 통한 현재 테스트 데이터는 규모가 제한적
+ 실제 운영 환경에서 발생할 수 있는 수백만 건 이상의 데이터를 다룰 준비가 부족
+ Kafka의 파티셔닝 전략, S3 적재 효율성, Snowflake의 쿼리 성능 및 파티셔닝 전략을 포함한 성능 테스트 및 최적화 작업이 필요

#### 운영 자동화 및 실시간 모니터링 시스템 강화
+ Airflow DAG 실패, Kafka/Snowflake 성능 이상 등의 운영 오류 발생 시 자동 재시도 및 Slack 알림 연동 필요
+ Kafka, Airflow 등의 로그와 상태를 모니터링하기 위해 Prometheus + Grafana 기반의 실시간 모니터링 시스템 도입이 요구됨
