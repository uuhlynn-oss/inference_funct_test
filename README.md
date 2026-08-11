# Real-time Inference Pipeline

Azure 기반 실시간 대중교통 혼잡도 예측 시스템의 **ML 추론 파이프라인**입니다.

Stream Analytics에서 5분 단위로 집계된 실시간 대중교통 데이터를 입력으로 받아 추론에 필요한 Feature를 구성하고, Azure Machine Learning Endpoint를 호출하여 예측 결과를 PostgreSQL에 저장합니다.

---

## Architecture

```text id="9v7m0q"
                 Real-time Data Pipeline
                         │
                         ▼
                ┌─────────────────┐
                │   Event Hub     │
                │ Real-time Data  │
                └────────┬────────┘
                         │
                         ▼
                ┌─────────────────┐
                │ Stream Analytics │
                │                 │
                │ Event-time      │
                │ 5-min Window    │
                │ Aggregation     │
                └────────┬────────┘
                         │
                         │ Aggregated Features
                         ▼
                ┌─────────────────────┐
                │  Inference Function │
                │                     │
                │ Input Validation    │
                │ Feature Engineering │
                └─────────┬───────────┘
                          │
                          │ Model Input
                          ▼
                ┌─────────────────────┐
                │ Azure ML Endpoint   │
                │                     │
                │   ML Inference      │
                └─────────┬───────────┘
                          │
                          │ Prediction
                          ▼
                ┌─────────────────────┐
                │    PostgreSQL       │
                │ prediction_result   │
                └─────────────────────┘
```

---

## Role

본 Repository는 전체 실시간 데이터 파이프라인에서 **ML 추론 직전부터 예측 결과 저장까지**를 담당합니다.

### 주요 책임

* Stream Analytics 집계 데이터 수신
* 입력 데이터 검증
* ML Feature Engineering
* Azure Machine Learning Endpoint 호출
* Prediction Result 처리
* PostgreSQL 저장

---

## End-to-End Flow

```text id="4r3w4n"
External Data
      │
      ▼
Data Collection
      │
      ▼
Event Hub
      │
      ▼
Stream Analytics
      │
      │ 5-minute Aggregation
      ▼
Inference Function
      │
      ├── Input Validation
      ├── Feature Engineering
      │
      ▼
Azure ML Endpoint
      │
      │ Prediction
      ▼
PostgreSQL
      │
      ▼
prediction_result
```

---

# Stream Analytics Processing

Stream Analytics에서는 실시간 BUS/SUBWAY 데이터를 **event time 기준 5분 Tumbling Window**로 집계합니다.

### 주요 처리

* `event_time` 기반 Event-time Processing
* BUS / SUBWAY 데이터 필터링
* `boarding`, `alighting` 데이터 품질 검증
* 5분 Tumbling Window 집계
* 위치 및 데이터 유형별 집계 결과 생성

### Aggregation

```text id="ndj57f"
5-minute Tumbling Window
          │
          ├── sample_count
          ├── traffic_sum_5m
          ├── boarding_sum_5m
          └── alighting_sum_5m
```

실제 Stream Analytics Query에서는 `TIMESTAMP BY event_time`을 사용하여 수집 시각이 아닌 **이벤트 발생 시각을 기준으로 Window를 구성**합니다.

또한 `TRY_CAST`를 활용하여 승차 및 하차 데이터가 유효한 경우만 집계하도록 구성했습니다.

예시:

```sql id="y9j2b8"
SELECT
    source_id,
    location_id,
    data_type,
    DATEADD(minute, -5, System.Timestamp()) AS window_start,
    System.Timestamp() AS window_end,
    CAST(COUNT(*) AS bigint) AS sample_count,

    CAST(
        SUM(
            TRY_CAST(boarding AS bigint)
            + TRY_CAST(alighting AS bigint)
        ) AS bigint
    ) AS traffic_sum_5m,

    CAST(
        SUM(TRY_CAST(boarding AS bigint))
        AS bigint
    ) AS boarding_sum_5m,

    CAST(
        SUM(TRY_CAST(alighting AS bigint))
        AS bigint
    ) AS alighting_sum_5m

INTO [observation-5m-postgres]

FROM [eh_raw]

TIMESTAMP BY event_time

WHERE
    data_type IN ('BUS', 'SUBWAY')
    AND TRY_CAST(boarding AS bigint) IS NOT NULL
    AND TRY_CAST(alighting AS bigint) IS NOT NULL

GROUP BY
    source_id,
    location_id,
    data_type,
    TumblingWindow(minute, 5);
```

---

# Inference Function

Stream Analytics에서 생성된 집계 데이터를 기반으로 ML 추론에 필요한 입력 Feature를 구성합니다.

```text id="8ep6cq"
Aggregated Data
      │
      ▼
Input Validation
      │
      ▼
Feature Engineering
      │
      ▼
Model Input
      │
      ▼
Azure ML Endpoint
```

Inference Function은 모델 자체를 실행하는 것이 아니라 **실시간 데이터와 ML 모델 사이의 추론 오케스트레이션 역할**을 담당합니다.

---

# Key Technical Decision

## Feature Engineering을 Inference Function에 통합

실시간 추론을 위해 별도의 Feature Engineering 서비스를 추가하는 대신, **Inference Function에서 ML 호출 직전에 필요한 Feature를 생성**하도록 설계했습니다.

### 고려했던 구조

```text id="e1t0yc"
Stream Analytics
       ↓
Feature Engineering Service
       ↓
Inference Function
       ↓
Azure ML
```

### 최종 구조

```text id="9u0z8k"
Stream Analytics
       ↓
Inference Function
       │
       ├── Validation
       ├── Feature Engineering
       └── AML Request
               ↓
        Azure ML Endpoint
```

### 설계 목적

* 추론 경로 단순화
* 별도 전처리 서비스 운영 복잡도 감소
* Feature 생성과 추론 요청의 일관성 확보
* ML Endpoint 연동 단계 최소화

---

# Prediction Result

Azure ML Endpoint에서 반환된 예측 결과는 PostgreSQL의 `prediction_result` 테이블에 저장합니다.

```text id="4qv9uy"
Azure ML Endpoint
       │
       ▼
Prediction Result
       │
       ▼
PostgreSQL
       │
       ▼
prediction_result
```

예측 결과는 이후 대시보드 및 의사결정 지원 시스템에서 조회할 수 있도록 관리합니다.

### 주요 데이터

| Field                | Description |
| -------------------- | ----------- |
| `location_id`        | 예측 대상 위치    |
| `prediction_time`    | 예측 기준 시각    |
| `prediction_horizon` | 예측 시점       |
| `prediction_value`   | 모델 예측 결과    |
| `created_at`         | 결과 생성 시각    |

---

# Tech Stack

| Technology                    | Role                                      |
| ----------------------------- | ----------------------------------------- |
| Python                        | Application / Data Processing             |
| Azure Functions               | Inference Orchestration                   |
| Azure Stream Analytics        | Event-time Processing & 5-min Aggregation |
| Azure Machine Learning        | ML Model Inference                        |
| Azure Database for PostgreSQL | Prediction Result Storage                 |

---

# Project Structure

```text id="a3n0ds"
.
├── function_app.py       # Inference Function
├── host.json             # Azure Functions Host configuration
├── requirements.txt      # Python dependencies
├── .funcignore           # Azure Functions deployment exclusions
└── .gitignore            # Git exclusions
```

---

# Local Development

## 1. Create Virtual Environment

```bash id="1k2t7a"
python -m venv .venv
```

Windows:

```bash id="7k8f4d"
.venv\Scripts\activate
```

## 2. Install Dependencies

```bash id="v5g3z2"
pip install -r requirements.txt
```

## 3. Configure Environment

Azure resource connection information and secrets should be configured through `local.settings.json` or environment variables.

`local.settings.json` is excluded from Git to prevent credentials and connection information from being committed.

## 4. Run Azure Function

```bash id="f7q1xm"
func start
```

---

# E2E Validation

Mock 기반으로 다음 전체 추론 경로를 검증했습니다.

```text id="z1x7cw"
Stream Analytics
       ↓
Inference Function
       ↓
Feature Engineering
       ↓
Azure ML Inference
       ↓
PostgreSQL
```

### Validation

* Stream Analytics 집계 데이터 전달 검증
* Inference Function 입력 처리 검증
* Feature Engineering 검증
* ML Inference 요청/응답 흐름 검증
* Prediction Result PostgreSQL 적재 확인

현재 Mock 기반 E2E 검증을 완료했으며, 실제 Azure ML Endpoint 배포 후 Production 환경에서 최종 검증을 진행합니다.

---

# Related Repository

본 Repository는 전체 실시간 데이터 파이프라인 중 **Inference 단계**를 담당합니다.

```text id="p0w3mz"
[Data Collection]
       ↓
[Event Hub]
       ↓
[Stream Analytics]
       ↓
┌────────────────────────┐
│  Real-time Inference   │
│     This Repository    │
└───────────┬────────────┘
            ↓
       [Azure ML]
            ↓
       [PostgreSQL]
```

전체 데이터 수집 및 Streaming Pipeline은 별도의 Repository에서 관리합니다.
