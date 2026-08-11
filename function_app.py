import azure.functions as func

import json
import logging
import os
import psycopg2
import psycopg2.extras

from datetime import datetime


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
        port=os.getenv("DB_PORT", "5432")
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
            COALESCE(SUM(expected_people), 0)
            AS total_expected_people
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

    return row["total_expected_people"] if row else 0


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
# BUS / SUBWAY 공통
# =========================================================

def make_features(
    traffic_sum_5m,
    window_start,
    window_end,
    weather,
    event_people,
    holiday
):

    # -----------------------------------------------------
    # 시간 Feature
    # -----------------------------------------------------

    feature_hour = window_end.hour
    feature_weekday = window_start.isoweekday()

    # -----------------------------------------------------
    # 주말 또는 공휴일
    # -----------------------------------------------------

    feature_holiday = (
        feature_weekday >= 6
        or holiday
    )

    # -----------------------------------------------------
    # Event 규모 Feature
    # -----------------------------------------------------

    event_scale = 0

    if event_people == 0:
        event_scale = 0

    elif event_people < 1000:
        event_scale = 1

    elif event_people < 5000:
        event_scale = 2

    elif event_people < 10000:
        event_scale = 3

    elif event_people < 20000:
        event_scale = 4

    else:
        event_scale = 5

    # -----------------------------------------------------
    # 최종 Feature
    # -----------------------------------------------------

    return {
        "hour": feature_hour,
        "weekday": feature_weekday,
        "holiday": int(feature_holiday),

        "traffic_volume": traffic_sum_5m,

        # SA에서 이미 0/1로 변환됨
        "rain": weather["rain"],
        "snow": weather["snow"],

        "temperature": float(
            weather["temperature"]
        ),

        "event": int(event_people > 0),
        "event_scale": event_scale
    }


# =========================================================
# HTTP Trigger
# ASA → Function App
# =========================================================

@app.route(
    route="predict_traffic",
    methods=["POST"]
)
def predict_traffic(req: func.HttpRequest) -> func.HttpResponse:

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

        # -------------------------------------------------
        # 1. ASA Payload
        # -------------------------------------------------

        req_body = req.get_json()

        logging.info(
            f"[1] Received payload from ASA: {req_body}"
        )

        # ASA Output은 단건 또는 배열 가능
        items = (
            req_body
            if isinstance(req_body, list)
            else [req_body]
        )

        logging.info(
            f"[2] Received item count: {len(items)}"
        )

        conn = get_db_connection()

        # -------------------------------------------------
        # 2. 각 집계 데이터 처리
        # BUS / SUBWAY 각각 독립적으로 처리
        # -------------------------------------------------

        for item in items:

            logging.info(
                "----------------------------------------"
            )

            logging.info(
                f"Processing item: {item}"
            )

            # -------------------------------------------------
            # 기본 데이터
            # -------------------------------------------------

            location_id = item["location_id"]

            data_type = item["data_type"]

            traffic_sum_5m = item["traffic_sum_5m"]

            # -------------------------------------------------
            # 데이터 타입 검증
            # -------------------------------------------------

            if data_type not in ("BUS", "SUBWAY"):

                raise ValueError(
                    f"Unsupported data_type: {data_type}"
                )

            logging.info(
                f"[DATA TYPE] {data_type}"
            )

            logging.info(
                f"[TRAFFIC] traffic_sum_5m = "
                f"{traffic_sum_5m}"
            )

            # -------------------------------------------------
            # 시간 파싱
            # -------------------------------------------------

            window_start_str = (
                item["window_start"]
                .replace("Z", "+00:00")
            )

            window_end_str = (
                item["window_end"]
                .replace("Z", "+00:00")
            )

            window_start = datetime.fromisoformat(
                window_start_str
            )

            window_end = datetime.fromisoformat(
                window_end_str
            )

            logging.info(
                f"[3] Window: "
                f"{window_start} ~ {window_end}"
            )

            # -------------------------------------------------
            # 3. PostgreSQL 부가 데이터 조회
            # -------------------------------------------------

            weather = get_latest_weather(
                conn,
                location_id
            )

            logging.info(
                f"[4] Weather: {weather}"
            )

            event_people = get_event(
                conn,
                location_id,
                window_end
            )

            logging.info(
                f"[5] Event people: {event_people}"
            )

            holiday = get_holiday(
                conn,
                window_start.date()
            )

            logging.info(
                f"[6] Holiday: {holiday}"
            )

            # -------------------------------------------------
            # 4. Feature 생성
            # BUS / SUBWAY 모두 동일한 함수 사용
            # 단, 각각의 traffic_sum_5m으로 별도 생성
            # -------------------------------------------------

            features = make_features(
                traffic_sum_5m=traffic_sum_5m,
                window_start=window_start,
                window_end=window_end,
                weather=weather,
                event_people=event_people,
                holiday=holiday
            )

            # -------------------------------------------------
            # 5. BUS Feature
            # -------------------------------------------------

            if data_type == "BUS":

                bus_features = features

                logging.info(
                    "========================================"
                )

                logging.info(
                    "[BUS FEATURES]"
                )

                logging.info(
                    json.dumps(
                        bus_features,
                        ensure_ascii=False,
                        default=str
                    )
                )

                logging.info(
                    "========================================"
                )

                # ---------------------------------------------
                # 향후 AML Endpoint
                # ---------------------------------------------
                #
                # bus_prediction_1 = call_bus_model_1(
                #     bus_features
                # )
                #
                # bus_prediction_2 = call_bus_model_2(
                #     bus_features
                # )
                #
                # bus_prediction_3 = call_bus_model_3(
                #     bus_features
                # )

            # -------------------------------------------------
            # 6. SUBWAY Feature
            # -------------------------------------------------

            elif data_type == "SUBWAY":

                subway_features = features

                logging.info(
                    "========================================"
                )

                logging.info(
                    "[SUBWAY FEATURES]"
                )

                logging.info(
                    json.dumps(
                        subway_features,
                        ensure_ascii=False,
                        default=str
                    )
                )

                logging.info(
                    "========================================"
                )

                # ---------------------------------------------
                # 향후 AML Endpoint
                # ---------------------------------------------
                #
                # subway_prediction_1 = call_subway_model_1(
                #     subway_features
                # )
                #
                # subway_prediction_2 = call_subway_model_2(
                #     subway_features
                # )
                #
                # subway_prediction_3 = call_subway_model_3(
                #     subway_features
                # )

        # -------------------------------------------------
        # 7. 테스트 응답
        # -------------------------------------------------

        return func.HttpResponse(
            json.dumps(
                {
                    "status": "success",
                    "message": (
                        "ASA → Function App → "
                        "BUS/SUBWAY Feature generation successful"
                    ),
                    "processed_count": len(items)
                },
                ensure_ascii=False
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