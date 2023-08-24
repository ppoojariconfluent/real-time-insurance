import time
import random
from confluent_kafka import Producer
import json

conf = {
    'bootstrap.servers': '',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': '',
    'sasl.password': ''
}

def calculate_idv(showroom_price, age_in_years):
    # Depreciation percentages based on car age
    depreciation = {
        0: 0.05,
        1: 0.15,
        2: 0.20,
        3: 0.30,
        4: 0.40,
        5: 0.50
        # Add more age and depreciation percentage pairs as needed
    }

    if age_in_years < 0 or age_in_years > 5:
        raise ValueError("Age should be between 0 and 5 years")

    if age_in_years not in depreciation:
        raise ValueError("Depreciation percentage not defined for the given age")

    depreciation_percentage = depreciation[age_in_years]
    idv = showroom_price - (depreciation_percentage * showroom_price)
    return idv

def produce_car_event(p, owner_id, owner_name, car_model):
    claim_amount = random.randint(0, 10000)
    num_claims = random.randint(1, 5)  # Random number of claims
    traffic_violations = random.randint(1, 5)  # Random traffic violations
    showroom_price = random.randint(500000, 1500000)
    age_in_years = random.randint(0, 5)
    idv = calculate_idv(showroom_price, age_in_years)
    
    event_data = {
        "owner_id": owner_id,
        "owner_name": owner_name,
        "car_model": car_model,
        "claim_amount": claim_amount,
        "num_claims": num_claims,
        "traffic_violations": traffic_violations,
        "showroom_price": showroom_price,
        "age_in_years": age_in_years,
        "idv": idv,
        "event_timestamp": int(time.time() * 1000)
    }
    p.produce("quote_requests", key=car_model, value=json.dumps(event_data))
    p.flush()

def main():
    p = Producer(conf)

    owners = [
        {"id": "1", "name": "John Doe", "car_model": "Toyota Corolla"},
        {"id": "2", "name": "Alice Smith", "car_model": "Honda Civic"},
        {"id": "3", "name": "Bob Johnson", "car_model": "Ford Mustang"},
        {"id": "4", "name": "Alex Smith", "car_model": "Audi A4"},
        {"id": "5", "name": "Grace Wellington", "car_model": "BMW X1"},
        # Add more owners and car models as needed
    ]

    num_events = 10  # Set the number of events to produce
    for _ in range(num_events):
        owner = random.choice(owners)
        produce_car_event(p, owner["id"], owner["name"], owner["car_model"])

    print(f"Produced {num_events} events.")

if __name__ == "__main__":
    main()
