"""
generate_synthetic_data.py
Generates realistic synthetic shipment data for BloomDirect claims pipeline.
Usage:
    python data/generate_synthetic_data.py --rows 100 --late-pct 0.40 --output data/sample_shipments.csv
"""

import argparse
import random
import pandas as pd
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()
random.seed(42)

SHIP_METHODS = [
    ("UPS_Ground", "UPS", 1),
    ("FEDEX_Ground", "FedEx", 1),
    ("Standard_Overnight", "FedEx", 1),
    ("Priority_Overnight", "FedEx", 1),
    ("FEDEX International", "FedEx", 2),
]

GIFT_MESSAGES = [
    ("Happy Birthday! Wishing you all the love today!", "Birthday"),
    ("Feliz Cumpleaños! Te queremos mucho!", "Birthday"),
    ("Happy Birthday Mom! Love you always!", "Birthday"),
    ("Thinking of you during this difficult time. With love.", "Funeral"),
    ("We are so sorry for your loss. Our hearts are with you.", "Funeral"),
    ("In loving memory. You are in our thoughts.", "Funeral"),
    ("Happy Valentine's Day! You mean the world to me.", "Valentine"),
    ("Happy Anniversary! Here's to many more years together.", "Anniversary"),
    ("Congratulations on your graduation! So proud of you!", "Graduation"),
    ("Just because I love you!", "General"),
    ("Thank you for everything you do.", "General"),
    ("Get well soon! Sending you healing thoughts.", "General"),
]

UPS_LATE_STATUSES = [
    "A mechanical failure has caused a delay. We will update the delivery date as soon as possible.",
    "A late UPS trailer arrival has delayed delivery. We're adjusting plans to deliver your package as quickly as possible.",
    "A late flight has caused a delay. We will update the delivery date as soon as possible.",
    "Severe weather conditions have delayed delivery.",
    "Due to operating conditions, your package may be delayed.",
]

FEDEX_LATE_STATUSES = [
    "Delay",
    "Operational Delay",
    "Local Delay",
    "Weather Delay",
    "Shipment exception",
    "Delivery exception",
]

DAMAGE_STATUSES = [
    "A damage has been reported and we will notify the sender.",
    "Your package was damaged in transit. We will notify the sender with details.",
    "The package has been damaged and the sender will be notified.",
]


def random_ship_date(start=None, end=None):
    from datetime import date, timedelta
    if start is None:
        start = (date.today() - timedelta(days=14)).strftime("%Y-%m-%d")
    if end is None:
        end = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
        
    """Generate a random ship date (Mon-Sat only)."""
    start_dt = datetime.strptime(start, "%Y-%m-%d")
    end_dt = datetime.strptime(end, "%Y-%m-%d")
    while True:
        delta = random.randint(0, (end_dt - start_dt).days)
        d = start_dt + timedelta(days=delta)
        if d.weekday() != 6:  # skip Sunday
            return d


def add_working_days(start_date, days):
    """Add N working days (Mon-Sat) to a date."""
    current = start_date
    added = 0
    while added < days:
        current += timedelta(days=1)
        if current.weekday() != 6:  # skip Sunday
            added += 1
    return current


def generate_tracking_id(carrier):
    """Generate realistic tracking ID."""
    if carrier == "UPS":
        return "1Z" + "".join([random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789") for _ in range(16)])
    else:
        return "".join([str(random.randint(0, 9)) for _ in range(12)])


def generate_order_id(carrier):
    """Generate realistic order ID."""
    if random.random() < 0.4:
        # Amazon-style order ID
        return f"{random.randint(100,999)}-{random.randint(1000000,9999999)}-{random.randint(1000000,9999999)}"
    else:
        return str(random.randint(1000000, 9999999))


def generate_row(row_type, ship_method_info):
    """Generate a single shipment row."""
    ship_method, carrier, sla_days = ship_method_info
    ship_date = random_ship_date()
    pickup_date = ship_date  # assume pickup on ship date
    promised_date = add_working_days(pickup_date, sla_days)
    tracking_id = generate_tracking_id(carrier)
    gift_msg, _ = random.choice(GIFT_MESSAGES)

    if row_type == "on_time":
        last_status = "Delivered"
        last_status_date = promised_date.strftime("%Y-%m-%d 23:59")
        first_status = "Picked up"
        first_status_date = ship_date.strftime("%Y-%m-%d %H:%M").replace(
            "%H:%M", f"{random.randint(10,19):02d}:{random.choice(['00','15','30','45'])}"
        )

    elif row_type == "late":
        last_status = "Delivered"
        late_days = random.randint(1, 3)
        delivery_date = add_working_days(promised_date, late_days)
        last_status_date = delivery_date.strftime("%Y-%m-%d 23:59")
        first_status = "Picked up"
        first_status_date = ship_date.strftime("%Y-%m-%d") + f" {random.randint(10,19):02d}:00"

    elif row_type == "damage":
        last_status = random.choice(DAMAGE_STATUSES)
        last_status_date = add_working_days(ship_date, 2).strftime("%Y-%m-%d %H:%M").replace(
            "%H:%M", f"{random.randint(8,17):02d}:00"
        )
        first_status = "Picked up"
        first_status_date = ship_date.strftime("%Y-%m-%d") + f" {random.randint(10,19):02d}:00"

    elif row_type == "lost":
        last_status = "In transit" if carrier == "FedEx" else "Destination Scan"
        last_status_date = add_working_days(ship_date, 2).strftime("%Y-%m-%d %H:%M").replace(
            "%H:%M", f"{random.randint(8,17):02d}:00"
        )
        first_status = "Picked up"
        first_status_date = ship_date.strftime("%Y-%m-%d") + f" {random.randint(10,19):02d}:00"

    elif row_type == "not_picked_up":
        last_status = "Shipment information sent to FedEx" if carrier == "FedEx" else "Order Processed: Ready for UPS"
        last_status_date = ship_date.strftime("%Y-%m-%d") + " 08:00"
        first_status = last_status
        first_status_date = last_status_date

    return {
        "partner_order_id": generate_order_id(carrier),
        "ship_method": ship_method,
        "ship_date": ship_date.strftime("%Y-%m-%d"),
        "track_id": tracking_id,
        "last_track_status": last_status,
        "last_track_status_date": last_status_date,
        "first_track_status": first_status,
        "first_track_status_date": first_status_date,
        "gift_message": gift_msg if random.random() < 0.6 else "",
    }


def generate_dataset(rows=100, late_pct=0.40, output="data/sample_shipments.csv"):
    """Generate balanced synthetic shipment dataset."""
    # Distribution
    n_late = int(rows * late_pct * 0.5)
    n_damage = int(rows * late_pct * 0.3)
    n_lost = int(rows * late_pct * 0.15)
    n_not_picked = int(rows * late_pct * 0.05)
    n_on_time = rows - n_late - n_damage - n_lost - n_not_picked

    records = []

    for _ in range(n_on_time):
        sm = random.choice(SHIP_METHODS)
        records.append(generate_row("on_time", sm))

    for _ in range(n_late):
        sm = random.choice(SHIP_METHODS)
        records.append(generate_row("late", sm))

    for _ in range(n_damage):
        sm = random.choice(SHIP_METHODS)
        records.append(generate_row("damage", sm))

    for _ in range(n_lost):
        sm = random.choice(SHIP_METHODS)
        records.append(generate_row("lost", sm))

    for _ in range(n_not_picked):
        sm = random.choice(SHIP_METHODS)
        records.append(generate_row("not_picked_up", sm))

    random.shuffle(records)
    df = pd.DataFrame(records)
    df.to_csv(output, index=False)

    print(f"Generated {len(df)} rows → {output}")
    print(f"  On-time:     {n_on_time}")
    print(f"  Late:        {n_late}")
    print(f"  Damaged:     {n_damage}")
    print(f"  Lost:        {n_lost}")
    print(f"  Not picked:  {n_not_picked}")
    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic BloomDirect shipment data")
    parser.add_argument("--rows", type=int, default=100, help="Number of rows to generate")
    parser.add_argument("--late-pct", type=float, default=0.40, help="Failure rate (0.0-1.0)")
    parser.add_argument("--output", type=str, default="data/sample_shipments.csv", help="Output CSV path")
    args = parser.parse_args()
    generate_dataset(rows=args.rows, late_pct=args.late_pct, output=args.output)
