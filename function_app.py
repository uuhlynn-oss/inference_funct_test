import azure.functions as func

import json
import logging
import os
import requests

import psycopg2
import psycopg2.extras

from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal


app = func.FunctionApp(
    http_auth_level=func.AuthLevel.FUNCTION
)


# =========================================================
# PostgreSQL Connection
# =========================================================

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        port=os.getenv("DB_PORT", "5432"),
        sslmode=os.getenv("POSTGRES_SSLMODE", "require")
    )


# =========================================================
# DB Lookup
# =========================================================

def get_latest_weather(conn, location_id):

    cur = conn.cursor(
        cursor_factory=psycopg2.extras.DictCursor
    )

    cur.execute(
        """
        SELECT
            temperature,
            rain,
            snow
        FROM transit_mvp.weather_history
        WHERE location_id = %s
        ORDER BY event_time DESC
        LIMIT 1;
        """,
        (location_id,)
    )

    row = cur.fetchone()
    cur.close()

    if row is None:
        return {
            "temperature": 20.0,
            "rain": 0,
            "snow": 0
        }

    return dict(row)


def get_event(conn, location_id, observation_time):

    cur = conn.cursor(
        cursor_factory=psycopg2.extras.DictCursor
    )

    cur.execute(
        """
        SELECT
            COUNT(*) AS event_count,
            COALESCE(MAX(event_scale), 0) AS event_scale
        FROM transit_mvp.event_master
        WHERE location_id = %s
          AND status = 'SCHEDULED'
          AND start_at <= %s
          AND end_at >= %s;
        """,
        (
            location_id,
            observation_time,
            observation_time
        )
    )

    row = cur.fetchone()
    cur.close()

    if row is None:
        return {
            "event": 0,
            "event_scale": 0
        }

    return {
        "event": int(row["event_count"] > 0),
        "event_scale": int(row["event_scale"])
    }


def get_holiday(conn, target_date):

    cur = conn.cursor(
        cursor_factory=psycopg2.extras.DictCursor
    )

    cur.execute(
        """
        SELECT
            is_holiday
        FROM transit_mvp.holiday_master
        WHERE holiday_date = %s;
        """,
        (target_date,)
    )

    row = cur.fetchone()
    cur.close()

    if row is None:
        return False

    return row["is_holiday"]


# =========================================================
# Feature Engineering
# =========================================================

def make_features(
    traffic_sum_5m,
    window_start,
    window_end,
    weather,
    event_info,
    holiday
):

    feature_hour = window_end.hour
    feature_weekday = window_start.isoweekday()

    feature_holiday = (
        feature_weekday >= 6
        or holiday
    )

    return {
        "hour": feature_hour,
        "weekday": feature_weekday,
        "holiday": int(feature_holiday),
        "traffic_volume": traffic_sum_5m,
        "rain": weather["rain"],
        "snow": weather["snow"],
        "temperature": float(weather["temperature"]),
        "event": int(event_info["event"]),
        "event_scale": int(event_info["event_scale"])
    }


# =========================================================
# AML Endpoint Configuration
# =========================================================

AML_ENDPOINTS = {
    "BUS": {
        0: {
            "url_env": "AML_BUS_CURRENT_URL",
            "key_env": "AML_BUS_CURRENT_KEY",
            "model_version": "bus-congestion-realtime"
        },
        60: {
            "url_env": "AML_BUS_1H_URL",
            "key_env": "AML_BUS_1H_KEY",
            "model_version": "bus-congestion-1h"
        },
        120: {
            "url_env": "AML_BUS_2H_URL",
            "key_env": "AML_BUS_2H_KEY",
            "model_version": "bus-congestion-2h"
        }
    },

    "SUBWAY": {
        0: {
            "url_env": "AML_SUBWAY_CURRENT_URL",
            "key_env": "AML_SUBWAY_CURRENT_KEY",
            "model_version": "subway-congestion-realtime"
        },
        60: {
            "url_env": "AML_SUBWAY_1H_URL",
            "key_env": "AML_SUBWAY_1H_KEY",
            "model_version": "subway-congestion-1h"
        },
        120: {
            "url_env": "AML_SUBWAY_2H_URL",
            "key_env": "AML_SUBWAY_2H_KEY",
            "model_version": "subway-congestion-2h"
        }
    }
}


FEATURE_COLUMNS = [
    "hour",
    "weekday",
    "holiday",
    "traffic_volume",
    "rain",
    "snow",
    "temperature",
    "event",
    "event_scale"
]


# =========================================================
# AML Payload
# =========================================================

def build_aml_payload(features):
    return {
        "Inputs": {
            "input1": [
                {
                    "hour": features["hour"],
                    "weekday": features["weekday"],
                    "holiday": features["holiday"],
                    "traffic_volume": features["traffic_volume"],
                    "rain": features["rain"],
                    "snow": features["snow"],
                    "temperature": features["temperature"],
                    "event": features["event"],
                    "event_scale": features["event_scale"]
                }
            ]
        },
        "GlobalParameters": {}
    }


# =========================================================
# AML Response Parsing
# =========================================================

def extract_congestion_score(result):
    try:
        results = result["Results"]

        # Designer Endpoint마다 WebServiceOutput0 / WebServiceOutput1
        # 이름이 다를 수 있으므로 첫 번째 출력 사용
        for output_name, output_rows in results.items():
            if (
                output_name.startswith("WebServiceOutput")
                and isinstance(output_rows, list)
                and len(output_rows) > 0
                and "Scored Labels" in output_rows[0]
            ):
                return float(
                    output_rows[0]["Scored Labels"]
                )

        raise ValueError(
            f"Scored Labels가 포함된 WebServiceOutput을 찾지 못했습니다: {result}"
        )

    except (KeyError, IndexError, TypeError, ValueError) as error:
        raise ValueError(
            f"AML 응답에서 Scored Labels 추출 실패: {result}"
        ) from error


# =========================================================
# AML Request
# =========================================================

def call_aml_endpoint(
    data_type,
    horizon_min,
    features
):

    config = AML_ENDPOINTS[
        data_type
    ][horizon_min]

    endpoint_url = os.getenv(
        config["url_env"]
    )

    endpoint_key = os.getenv(
        config["key_env"]
    )

    if not endpoint_url:
        raise ValueError(
            f"{config['url_env']} 환경변수가 없습니다."
        )

    if not endpoint_key:
        raise ValueError(
            f"{config['key_env']} 환경변수가 없습니다."
        )

    payload = build_aml_payload(
        features
    )

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {endpoint_key}"
    }

    logging.info(
        "[AML REQUEST] data_type=%s horizon=%s",
        data_type,
        horizon_min
    )

    response = requests.post(
        endpoint_url,
        headers=headers,
        json=payload,
        timeout=30
    )

    response.raise_for_status()

    try:
        raw_result = response.json()

    except ValueError:
        raw_result = response.text

    congestion_score = (
        extract_congestion_score(
            raw_result
        )
    )

    if not (
        0 <= congestion_score <= 100
    ):
        raise ValueError(
            "congestion_score 범위 오류: "
            f"{congestion_score}"
        )

    logging.info(
        "[AML RESPONSE] "
        "data_type=%s horizon=%s score=%s",
        data_type,
        horizon_min,
        congestion_score
    )

    return {
        "data_type": data_type,
        "horizon_min": horizon_min,
        "congestion_score": congestion_score,
        "model_version": config["model_version"],
        "raw_result": raw_result
    }


# =========================================================
# AML Multi-thread
# =========================================================

def call_models_parallel(
    prediction_jobs
):

    results = []

    if not prediction_jobs:
        return results

    max_workers = min(
        6,
        len(prediction_jobs)
    )

    with ThreadPoolExecutor(
        max_workers=max_workers
    ) as executor:

        future_map = {}

        for job in prediction_jobs:

            future = executor.submit(
                call_aml_endpoint,
                job["data_type"],
                job["horizon_min"],
                job["features"]
            )

            future_map[future] = job

        for future in as_completed(
            future_map
        ):

            job = future_map[future]

            try:

                result = future.result()

                result["location_id"] = (
                    job["location_id"]
                )

                result["generated_at"] = (
                    job["generated_at"]
                )

                result["target_at"] = (
                    job["target_at"]
                )

                results.append(
                    result
                )

            except Exception as error:

                logging.exception(
                    "[AML ERROR] "
                    "data_type=%s horizon=%s",
                    job["data_type"],
                    job["horizon_min"]
                )

                results.append(
                    {
                        "data_type": job["data_type"],
                        "horizon_min": job["horizon_min"],
                        "location_id": job["location_id"],
                        "generated_at": job["generated_at"],
                        "target_at": job["target_at"],
                        "error": str(error)
                    }
                )

    results.sort(
        key=lambda item: (
            item["data_type"],
            item["horizon_min"]
        )
    )

    return results


# =========================================================
# prediction_result INSERT
# =========================================================

def save_prediction_results(
    conn,
    predictions
):

    successful_predictions = [
        prediction
        for prediction in predictions
        if "error" not in prediction
    ]

    if not successful_predictions:

        logging.warning(
            "저장 가능한 AML 예측결과가 없습니다."
        )
        return 0

    sql = """
        INSERT INTO transit_mvp.prediction_result (
            data_type,
            location_id,
            generated_at,
            target_at,
            horizon_min,
            congestion_score,
            model_version
        )
        VALUES (
            %(data_type)s,
            %(location_id)s,
            %(generated_at)s,
            %(target_at)s,
            %(horizon_min)s,
            %(congestion_score)s,
            %(model_version)s
        );
    """

    cur = conn.cursor()

    try:

        for prediction in successful_predictions:

            cur.execute(
                sql,
                {
                    "data_type": (
                        prediction["data_type"]
                    ),
                    "location_id": (
                        prediction["location_id"]
                    ),
                    "generated_at": (
                        prediction["generated_at"]
                    ),
                    "target_at": (
                        prediction["target_at"]
                    ),
                    "horizon_min": (
                        prediction["horizon_min"]
                    ),
                    "congestion_score": round(
                        float(
                            prediction[
                                "congestion_score"
                            ]
                        ),
                        2
                    ),
                    "model_version": (
                        prediction["model_version"]
                    )
                }
            )

        conn.commit()

    except Exception:

        conn.rollback()
        raise

    finally:

        cur.close()

    logging.info(
        "[DB] prediction_result %s건 저장 완료",
        len(successful_predictions)
    )

    return len(successful_predictions)


# =========================================================
# HTTP Trigger
# ASA → Function → AML → PostgreSQL
# =========================================================

@app.route(
    route="predict_traffic",
    methods=["POST"]
)
def predict_traffic(
    req: func.HttpRequest
) -> func.HttpResponse:

    logging.info(
        "========================================"
    )

    logging.info(
        "ASA → Function App Prediction START"
    )

    logging.info(
        "========================================"
    )

    conn = None

    try:

        req_body = req.get_json()

        logging.info(
            "[1] Received payload from ASA: %s",
            req_body
        )

        items = (
            req_body
            if isinstance(req_body, list)
            else [req_body]
        )

        logging.info(
            "[2] Received item count: %s",
            len(items)
        )

        conn = get_db_connection()

        prediction_jobs = []
        feature_results = []

        # =================================================
        # Feature 생성
        # =================================================

        for item in items:

            location_id = (
                item["location_id"]
            )

            data_type = (
                item["data_type"]
            )

            traffic_sum_5m = (
                item["traffic_sum_5m"]
            )

            if data_type not in (
                "BUS",
                "SUBWAY"
            ):
                raise ValueError(
                    f"Unsupported data_type: "
                    f"{data_type}"
                )

            window_start_str = (
                item["window_start"]
                .replace(
                    "Z",
                    "+00:00"
                )
            )

            window_end_str = (
                item["window_end"]
                .replace(
                    "Z",
                    "+00:00"
                )
            )

            window_start = (
                datetime.fromisoformat(
                    window_start_str
                )
            )

            window_end = (
                datetime.fromisoformat(
                    window_end_str
                )
            )

            weather = get_latest_weather(
                conn,
                location_id
            )

            event_info = get_event(
                conn,
                location_id,
                window_end
            )

            holiday = get_holiday(
                conn,
                window_start.date()
            )

            features = make_features(
                traffic_sum_5m=traffic_sum_5m,
                window_start=window_start,
                window_end=window_end,
                weather=weather,
                event_info=event_info,
                holiday=holiday
            )

            feature_results.append(
                {
                    "data_type": data_type,
                    "location_id": location_id,
                    "features": features
                }
            )

            # 현재 집계 종료시각을 예측 생성시각 기준으로 사용
            generated_at = window_end

            for horizon_min in (
                0,
                60,
                120
            ):

                target_at = (
                    generated_at
                    + timedelta(
                        minutes=horizon_min
                    )
                )

                prediction_jobs.append(
                    {
                        "data_type": data_type,
                        "horizon_min": horizon_min,
                        "location_id": location_id,
                        "generated_at": generated_at,
                        "target_at": target_at,
                        "features": features
                    }
                )

        # =================================================
        # AML 최대 6개 병렬 호출
        # =================================================

        logging.info(
            "[3] AML parallel jobs: %s",
            len(prediction_jobs)
        )

        predictions = (
            call_models_parallel(
                prediction_jobs
            )
        )

        # =================================================
        # prediction_result 저장
        # =================================================

        saved_count = (
            save_prediction_results(
                conn,
                predictions
            )
        )

        logging.info(
            "[4] prediction_result saved: %s",
            saved_count
        )

        # =================================================
        # Response
        # =================================================

        return func.HttpResponse(
            json.dumps(
                {
                    "status": "success",
                    "processed_count": len(items),
                    "prediction_job_count": (
                        len(prediction_jobs)
                    ),
                    "saved_count": saved_count,
                    "features": feature_results,
                    "predictions": predictions
                },
                ensure_ascii=False,
                default=str
            ),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as e:

        logging.exception(
            "Error executing prediction pipeline"
        )

        return func.HttpResponse(
            json.dumps(
                {
                    "status": "error",
                    "message": str(e)
                },
                ensure_ascii=False
            ),
            status_code=500,
            mimetype="application/json"
        )

    finally:

        if conn:
            conn.close()

        logging.info(
            "========================================"
        )

        logging.info(
            "ASA → Function App Prediction END"
        )

        logging.info(
            "========================================"
        )