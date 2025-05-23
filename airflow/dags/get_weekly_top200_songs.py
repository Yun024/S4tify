import os
import time
from datetime import datetime, timedelta

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


@task
def extract(countries, **kwargs):

    user_id = os.getenv("AIRFLOW_VAR_SPOTIFY_ID")
    user_pass = os.getenv("AIRFLOW_VAR_SPOTIFY_PASS")

    execution_date = kwargs["ds"]
    exec_date = datetime.strptime(execution_date, "%Y-%m-%d")
    current_weekday = exec_date.weekday()

    if current_weekday >= 5:  # 주말(토, 일)인 경우 → 이번 주 목요일 가져오기
        days_until_thursday = (3 - current_weekday) % 7
        target_date = exec_date + timedelta(days=days_until_thursday)
    else:  # 평일(월~금)인 경우 → 지난주 목요일 가져오기
        days_since_thursday = (current_weekday - 3) % 7
        target_date = exec_date - timedelta(days=days_since_thursday)

    date = target_date.strftime("%Y-%m-%d")

    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )

    with webdriver.Chrome(service=Service(), options=options) as driver:
        driver.get(
            "https://charts.spotify.com/charts/view/regional-global-weekly/latest"
        )
        time.sleep(2)

        login_button = driver.find_element(
            By.XPATH, '//*[@id="__next"]/div/div/header/div/div[2]/a/span[1]'
        )
        login_button.click()

        id_input = driver.find_element(
            By.XPATH, '//*[@id="login-username"]').send_keys(user_id)

        try:
            login_button = driver.find_element(
                By.XPATH, '//*[@id="login-button"]/span[1]/span'
            )
            login_button.click()

            time.sleep(2)

            continue_button = driver.find_element(
                By.XPATH,
                '//*[@id="encore-web-main-content"]/div/div/div/div/form/div[2]/section/button',
            )
            continue_button.click()
            time.sleep(2)
        except BaseException:
            pass

        password_input = driver.find_element(
            By.XPATH, '//*[@id="login-password"]'
        ).send_keys(user_pass)

        login_button = driver.find_element(By.XPATH, '//*[@id="login-button"]')
        login_button.click()
        time.sleep(3)

        for country in countries:
            driver.get(
                f"https://charts.spotify.com/charts/view/regional-{country}-weekly/{date}"
            )

            try:
                WebDriverWait(
                    driver, 5).until(
                    EC.presence_of_element_located(
                        (By.ID, "onetrust-group-container")))
                driver.execute_script(
                    "document.getElementById('onetrust-group-container').style.display='none';"
                )
                print("✅ 'onetrust-group-container' 배너 숨김 완료")
            except BaseException:
                print("⚠️ 배너를 찾을 수 없음")

            csv_download_button = driver.find_element(
                By.XPATH, '//*[@id="__next"]/div/div[3]/div/div/div[2]/span'
            )
            csv_download_button.click()

            time.sleep(2)


@task
def upload_to_s3():
    s3_hook = S3Hook(aws_conn_id="AWS_S3")
    s3_bucket = "de5-s4tify"
    local_dir = "/home/airflow/Downloads"

    files = [f for f in os.listdir(local_dir) if f.endswith(".csv")]

    for file_name in files:
        s3_prefix = file_name.split("-")[1]
        s3_key = f"raw_data/weekly_top200_songs/{s3_prefix}/{file_name}"

        local_file_path = os.path.join(local_dir, file_name)

        s3_hook.load_file(
            filename=local_file_path,
            key=s3_key,
            bucket_name=s3_bucket,
            replace=True)

        try:
            os.remove(local_file_path)
        except BaseException:
            pass


with DAG(
    dag_id="get_weekly_top200_songs",
    start_date=datetime(2025, 1, 1),
    schedule="0 0 * * 6",
    catchup=False,
    tags=["Web_Crawling"],
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
    },
) as dag:

    countries = ["kr", "global", "us"]
    extract(countries) >> upload_to_s3()
