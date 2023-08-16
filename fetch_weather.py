import httpx
from prefect import task, flow, get_run_logger
from prefect_email import EmailServerCredentials, email_send_message


@task(retries=2, retry_delay_seconds=.5)
def fetch_weather(lat: float, long: float):
    base_url = "https://api.open-meteo.com/v1/forecast"
    weather = httpx.get(
        base_url, 
        params=dict(latitude=lat, longitude=long, hourly="temperature_2m"), 
    )
    most_recent_temp = float(weather.json()["hourly"]["temperature_2m"][0])    
    # most_recent_temp = weather.json
    if weather.status_code >= 400:
        raise Exception()
    print(f"Most recent temp C: {most_recent_temp} degrees")
    return most_recent_temp

# @task(retries=2, retry_delay_seconds=2)
# def get_windspeed(lat: float, lon: float):
#     base_url = "https://api.open-meteo.com/v1/forecast/"
#     print("Working")
#     # wind = httpx.get(
#     #     base_url,
#     #     params = dict(latitude=lat, longitude=lon, hourly="temperature_2m"),
#     # )
#     wind_speed = float(wind.json()["hourly"]["windspeed_10m"][0])
#     print(f"Current wind speed C: {wind_speed}")
#     return wind_speed

@task
def save_weather(temp: float):
    with open("weather.csv", "w+") as w:
        w.write(str(temp))
    return "Successfully wrote temp"


@flow(retries=2, retry_delay_seconds=.5)
def pipeline(lat: float, lon: float):
    logger = get_run_logger()
    temp = fetch_weather(lat, lon)
    # ws = get_windspeed(lat, lon)
    logger.info(f"INFO {temp}")
    result = save_weather(temp)
    logger.debug("You only see this message if the logging level is set to DEBUG. 🙂")
    return result

@flow
def example_email_send_message_flow(email_addresses: list[str]):
    email_server_credentials = EmailServerCredentials.load("stridehealth-email")
    for email_address in email_addresses:
        subject = email_send_message.with_options(name=f"email {email_address}").submit(
            email_server_credentials=email_server_credentials,
            subject="Example Flow Notification using Gmail",
            msg="This proves email_send_message works!",
            email_to=email_address,
        )


if __name__ == "__main__":
    # example_email_send_message_flow(["libby.theoharis@gmail.com"]) 
    pipeline(38.9, -77.0)