import os
import requests
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()


# ======================fun===
# PostgreSQL Connection
# =========================

def get_db_connection():

    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        port=os.getenv("DB_PORT")
    )


# =========================
# observation_5m 조회
# =========================

def get_observation_5m(conn, aggregate_id):

    cur = conn.cursor(
        cursor_factory=psycopg2.extras.DictCursor
    )

    cur.execute(
        """
        SELECT
            location_id,
            window_start,
            window_end,
            traffic_sum_5m,
            temperature,
            rain,
            snow,
            data_type
        FROM transit_mvp.observation_5m
        WHERE aggregate_id = %s;
        """,
        (aggregate_id,)
    )

    row = cur.fetchone()

    cur.close()

    return row



# =========================
# event 조회
# =========================

def get_event(conn, location_id, observation_time):

    cur = conn.cursor(
        cursor_factory=psycopg2.extras.DictCursor
    )

    cur.execute(
        """
        SELECT
            COALESCE(SUM(expected_people),0)
            AS total_expected_people
        FROM transit_mvp.event_master
        WHERE venue_id = %s
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

    return row["total_expected_people"]
# =========================
# holiday 조회
# =========================


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

# =========================
# bus 예측을 위한 feature engineerign
# =========================
def make_bus_features(observation, event_people, holiday):

    feature_hour = (observation["window_end"].hour)
    feature_weekday = (observation["window_start"].isoweekday())

    # 주말 + 법정공휴일
    feature_holiday = (feature_weekday >= 6 or holiday)

    feature_traffic_volume = (observation["traffic_sum_5m"])
    feature_rain = (observation["rain"])
    feature_snow = (observation["snow"])
    feature_temperature = (observation["temperature"])


   # =====================
    # Event Feature
    # =====================

    feature_event = (event_people > 0    )
    feature_event_scale = 0

    if event_people == 0:
        feature_event_scale = 0

    elif event_people < 1000:
        feature_event_scale = 1

    elif event_people < 5000:
        feature_event_scale = 2

    elif event_people < 10000:
        feature_event_scale = 3

    elif event_people < 20000:
        feature_event_scale = 4


    else:
        feature_event_scale = 5



    return {

        "hour": feature_hour,
        "weekday": feature_weekday,
        "holiday": int(feature_holiday),
        "traffic_volume": feature_traffic_volume,
        "rain": int(feature_rain),
        "snow": int(feature_snow),
        "temperature": float(feature_temperature),
        "event": int(feature_event),
        "event_scale": feature_event_scale
    }



# =========================
# AML Endpoint 호출 // local 테스트로 진행해주세요!
# =========================



# =========================
# Test
# =========================

if __name__ == "__main__":

    conn = get_db_connection()

    print("DB 연결 성공")


    # 1. observation 조회
    observation = get_observation_5m(
        conn,
        4
    )


    # 2. event 조회
    event_people = get_event(
        conn,
        observation["location_id"],
        observation["window_start"]
    )


    # 3. holiday 조회
    holiday = get_holiday(
        conn,
        observation["window_start"].date()
    )


    # 4. feature 생성
    features = make_bus_features(
        observation,
        event_people,
        holiday
    )


    print("Features")
    print(features)



    # 5. ML 호출 및 결과 찍어보세요!
   



    conn.close()