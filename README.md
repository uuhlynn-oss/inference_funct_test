# Real-time Inference Pipeline

Azure 기반 실시간 대중교통 혼잡도 예측 시스템의 **ML 추론 파이프라인**입니다.

Stream Analytics에서 전달된 실시간 집계 데이터를 입력으로 받아 Feature Engineering을 수행하고, Azure Machine Learning Endpoint를 통해 혼잡도를 예측한 후 PostgreSQL에 결과를 저장합니다.

---

## Architecture

```text
                    Real-time Data Pipeline
                            │
                            ▼
                  ┌───────────────────┐
                  │ Stream Analytics  │
                  │                   │
                  │ 5-min Aggregation │
                  └─────────┬─────────┘
                            │
                            │ Aggregated Features
                            ▼
                  ┌───────────────────┐
                  │ Inference Function│
                  │                   │
                  │ Input Validation  │
                  │ Feature Engineering│
                  └─────────┬─────────┘
                            │
                            │ Feature Vector
                            ▼
                  ┌───────────────────┐
                  │ Azure ML Endpoint │
                  │                   │
                  │   ML Inference    │
                  └─────────┬─────────┘
                            │
                            │ Prediction
                            ▼
                  ┌───────────────────┐
                  │    PostgreSQL     │
                  │ prediction_result │
                  └───────────────────┘
```

---

## Role

본 Repository는 실시간 데이터 파이프라인에서 **ML 추론 직전부터 예측 결과 저장까지**를 담당합니다.

### 주요 책임

* Stream Analytics 집계 결과 수신
* Input Validation
* ML Feature Engineering
* Azure ML Endpoint 호출
* Prediction Result 처리
* PostgreSQL 저장

---

## Processing Flow

```text
Stream Analytics
      │
      ▼
Aggregated Data
      │
      ▼
┌─────────────────────┐
│ Input Validation    │
└─────────┬───────────┘
          ▼
┌─────────────────────┐
│ Feature Engineering │
└─────────┬───────────┘
          ▼
┌─────────────────────┐
│ Azure ML Endpoint   │
│      Inference      │
└─────────┬───────────┘
          ▼
┌─────────────────────┐
│ Prediction Result   │
│ Validation          │
└─────────┬───────────┘
          ▼
┌─────────────────────┐
│ PostgreSQL          │
│ prediction_result   │
└─────────────────────┘
```

---

## Key Technical Decision

### Feature Engineering을 Inference Function에 통합

실시간 추론 과정에서 별도의 Feature Engineering 서비스를 추가하지 않고, **Inference Function에서 추론 직전에 필요한 Feature를 생성**하도록 설계했습니다.

```text
기존 고려 구조

Stream Analytics
       ↓
Feature Engineering Service
       ↓
Inference Function
       ↓
Azure ML
```

```text
최종 구조

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

이를 통해:

* 추론 경로 단순화
* 서비스 간 연동 단계 감소
* 전처리와 추론 요청의 일관성 확보
* 추가 인프라 운영 복잡도 감소

를 달성했습니다.

---

## Input

Stream Analytics에서 5분 단위로 집계된 실시간 Feature를 입력으로 사용합니다.

예시:

```json
{
  "source_id": "SRC_OA21285_CITYDATA",
  "location_id": "JAMSIL",
  "window_start": "2026-08-08T13:00:00Z",
  "window_end": "2026-08-08T13:05:00Z",
  "features": {
    "subway_current": 0.72,
    "subway_1h": 0.64,
    "subway_2h": 0.58,
    "bus_current": 0.51,
    "bus_1h": 0.47,
    "bus_2h": 0.43
  }
}
```

실제 Feature 구성은 학습된 ML 모델의 입력 스키마에 맞춰 처리합니다.

---

## ML Inference

Feature Engineering이 완료된 데이터는 Azure Machine Learning Endpoint로 전달됩니다.

```text
Aggregated Features
        ↓
Feature Engineering
        ↓
Model Input
        ↓
Azure ML Endpoint
        ↓
Prediction
```

Inference Function은 모델 자체를 실행하는 역할이 아니라 **실시간 데이터와 ML Endpoint 사이의 추론 오케스트레이션 역할**을 담당합니다.

---

## Prediction Result

ML Endpoint에서 반환된 예측 결과는 PostgreSQL의 `prediction_result` 테이블에 저장합니다.

예시 구조:

| Column               | Description |
| -------------------- | ----------- |
| `location_id`        | 예측 대상 위치    |
| `prediction_time`    | 예측 기준 시각    |
| `prediction_horizon` | 예측 시점       |
| `prediction_value`   | 예측 결과       |
| `created_at`         | 결과 생성 시각    |

이를 통해 이후 Dashboard 및 의사결정 지원 시스템에서 예측 결과를 조회할 수 있습니다.

---

## Tech Stack

| Technology                    | Role                               |
| ----------------------------- | ---------------------------------- |
| Python                        | Application / Data Processing      |
| Azure Functions               | Serverless Inference Orchestration |
| Azure Stream Analytics        | Real-time Aggregation              |
| Azure Machine Learning        | ML Model Inference                 |
| Azure Database for PostgreSQL | Prediction Result Storage          |

---

## Project Structure

```text
.
├── function_app.py       # Inference Function
├── host.json             # Azure Functions Host 설정
├── requirements.txt      # Python dependencies
├── .funcignore           # Azure Functions 배포 제외 파일
└── .gitignore            # Git 제외 파일
```

---

## Local Development

### 1. Virtual Environment

```bash
python -m venv .venv
```

Windows:

```bash
.venv\Scripts\activate
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure Environment

Azure 리소스 연결 정보와 Secret은 `local.settings.json` 또는 환경변수로 관리합니다.

`local.settings.json`은 보안상 Git Repository에 포함하지 않습니다.

### 4. Run Azure Function

```bash
func start
```

---

## E2E Validation

Mock 기반으로 다음 추론 경로를 검증했습니다.

```text
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

### Validation Result

* Input → Inference → DB 저장 흐름 검증
* Prediction Result PostgreSQL 적재 확인
* 실시간 추론 경로 E2E 검증 완료

> 실제 Azure ML Endpoint 배포 후 Production Endpoint 기준의 최종 검증을 진행합니다.

---

## Related Pipeline

본 Repository는 전체 실시간 데이터 파이프라인의 **Inference 단계**를 담당합니다.

```text
External Data
      ↓
Data Collection
      ↓
Event Hub
      ↓
Stream Analytics
      ↓
┌───────────────────────┐
│  This Repository      │
│  Real-time Inference  │
└───────────┬───────────┘
            ↓
       Azure ML
            ↓
       PostgreSQL
```
