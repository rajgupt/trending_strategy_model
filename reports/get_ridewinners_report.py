import requests
import os
import datetime
import logging
from dotenv import load_dotenv

load_dotenv()
API_TOKEN = os.getenv('API_TOKEN')

def download_report(screen_id, date='latest'):
    # Construct URL with screen_id
    if date != 'latest':
        url = f'https://ridewinners.com/api/v1/screens/5/details?date={date}&format=csv'
    else:
        url = f'https://ridewinners.com/api/v1/screens/{screen_id}/latest/details?format=csv'
        date = 'latest'


    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-US,en;q=0.9,hi;q=0.8',
        'authorization': API_TOKEN,
        'content-type': 'application/json',
        'origin': 'https://ridewinners.com',
        'referer': f'https://ridewinners.com/screen/{screen_id}/{date}/details',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
    }

    payload = {
        "filters": {},
        "sortBy": "sector",
        "isAscOrder": True
    }
    print("url:", url)
    print("Screen ID:", screen_id)
    print("Date:", date)
    print("Headers:", headers)
    print("Payload:", payload)
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()  # Raise exception for bad status codes

    output_dir = f"data/{screen_id}"
    os.makedirs(output_dir, exist_ok=True)
    if date == 'latest':
        date = datetime.datetime.now().strftime("%Y-%m-%d")
    output_file = f"{output_dir}/detail_{date}.csv"

    if response.ok:
        with open(output_file, "wb") as f:
            f.write(response.content)
        print(f"CSV downloaded successfully to {output_file}.")
    else:
        print(f"Failed to download: {response.status_code}\n{response.text}")


def download_trend_report(screen_id, date, must_trend_on_date=False):
    """
    Download trend report from ridewinners.com
    
    Args:
        screen_id (int): ID of the screen to download
        date (str): Date in YYYY-MM-DD format
        must_trend_on_date (bool): Filter for trending on specific date
    """
    # Construct URL with parameters
    url = f'https://ridewinners.com/api/v1/screens/{screen_id}/trend?format=csv&date={date}&mustTrendOnDate={str(must_trend_on_date).lower()}'

    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-US,en;q=0.9,hi;q=0.8',
        'authorization': API_TOKEN,
        'content-type': 'application/json',
        'origin': 'https://ridewinners.com',
        'referer': f'https://ridewinners.com/screen/{screen_id}/{date}/trend',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
    }

    payload = {
        "filters": {},
        "sortBy": "trending_days",
        "isAscOrder": False
    }

    print("url:", url)
    print("Screen ID:", screen_id)
    print("Date:", date)
    print("Must Trend On Date:", must_trend_on_date)
    print("Headers:", headers)
    print("Payload:", payload)

    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()

        output_dir = f"data/{screen_id}"
        os.makedirs(output_dir, exist_ok=True)
        output_file = f"{output_dir}/trend_{date}.csv"

        if response.ok:
            with open(output_file, "wb") as f:
                f.write(response.content)
            print(f"Trend CSV downloaded successfully to {output_file}.")
            return output_file
        else:
            print(f"Failed to download: {response.status_code}\n{response.text}")
            return None

    except Exception as e:
        logging.error(f"Error downloading trend report for screen_id={screen_id}, date={date}: {e}")
        print(f"Error downloading trend report: {str(e)}")
        return None

def get_fridays_until_today(start_date):
    """
    Returns a list of Fridays (YYYY-MM-DD) from start_date until today.

    Args:
        start_date (str or datetime.date): Start date in 'YYYY-MM-DD' format or as a date object.

    Returns:
        list: List of Friday dates as strings.
    """
    today = datetime.date.today()
    if isinstance(start_date, str):
        d = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
    else:
        d = start_date
    # Find first Friday on or after start_date
    while d.weekday() != 4:
        d += datetime.timedelta(days=1)
    fridays = []
    while d <= today:
        fridays.append(d.strftime("%Y-%m-%d"))
        d += datetime.timedelta(days=7)
    return fridays

# screen_id = 15
# for date_str in get_fridays_until_today(2025):
#     print(f"Processing report for screen_id={screen_id}, date={date_str}")
#     try:
#         download_report(screen_id, date_str)
#     except Exception as e:
#         logging.basicConfig(filename='download_reports.log', level=logging.ERROR,
#                             format='%(asctime)s %(levelname)s:%(message)s')
#         logging.error(f"Error downloading report for screen_id={screen_id}, date={date_str}: {e}")
#         print(f"Error downloading report for screen_id={screen_id}, date={date_str}: {e}")

screen_id = 11
for date_str in get_fridays_until_today(2025):
    print(f"Processing report for screen_id={screen_id}, date={date_str}")
    try:
        download_trend_report(screen_id, date_str)
    except Exception as e:
        logging.basicConfig(filename='download_reports.log', level=logging.ERROR,
                            format='%(asctime)s %(levelname)s:%(message)s')
        logging.error(f"Error downloading report for screen_id={screen_id}, date={date_str}: {e}")
        print(f"Error downloading report for screen_id={screen_id}, date={date_str}: {e}")