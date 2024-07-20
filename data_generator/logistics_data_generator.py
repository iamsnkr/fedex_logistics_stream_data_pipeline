import random
import string
from datetime import datetime, timedelta, timezone


def generate_from_to_locations():
    cities = [
        "New York, NY", "Los Angeles, CA", "Chicago, IL", "Houston, TX", "Phoenix, AZ",
        "Philadelphia, PA", "San Antonio, TX", "San Diego, CA", "Dallas, TX", "San Jose, CA",
        "Austin, TX", "Jacksonville, FL", "San Francisco, CA", "Indianapolis, IN", "Columbus, OH",
        "Fort Worth, TX", "Charlotte, NC", "Seattle, WA", "Denver, CO", "Washington, DC",
        "Boston, MA", "El Paso, TX", "Nashville, TN", "Detroit, MI", "Oklahoma City, OK",
        "Portland, OR", "Las Vegas, NV", "Memphis, TN", "Louisville, KY", "Milwaukee, WI"
    ]
    # Choose a random from_location
    from_location = random.choice(cities)

    # Remove from_location from the list of cities
    to_locations = [city for city in cities if city != from_location]

    # Choose a random to_location from the remaining cities
    to_location = random.choice(to_locations)

    # print(f"From Location: {from_location}")
    # print(f"To Location: {to_location}")
    return {'from': from_location, 'to': to_location}


def generate_timestamp():
    start_date = datetime(2023, month=1, day=1, hour=0, minute=0, second=0, tzinfo=timezone.utc)
    end_date = datetime(2025, month=12, day=31, hour=23, minute=59, second=59, tzinfo=timezone.utc)

    random_diff = random.randint(0, int((end_date - start_date).total_seconds()))
    time = start_date + timedelta(seconds=random_diff)
    # print(time.strftime("%Y-%m-%dT%H:%M:%SZ"))
    return time.strftime("%Y-%m-%dT%H:%M:%SZ")


def generate_status():
    statuses = [
        "picked up",
        "in-transit",
        "arrived at hub",
        "out for delivery",
        "delivered",
        "returned to sender"
    ]
    return random.choice(statuses)


def generate_logistics_data():
    random_id = "".join(random.choices(string.ascii_uppercase + string.digits, k=5))
    locations = generate_from_to_locations()
    logistics_data = {
        "shipment_id": random_id,
        "origin": locations['from'],
        "destination": locations['to'],
        "status": generate_status(),
        "timestamp": generate_timestamp()
    }
    return logistics_data

# __all__ = ['generate_logistics_data'] # Only func will be exported when using "from module import *"
