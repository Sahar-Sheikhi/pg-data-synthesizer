from faker import Faker
import random

class DataGenerator:
    def __init__(self):
        self.fake = Faker()
        # The Registry: Key to maintaining Referential Integrity
        self.registry = {
            "customer_id": [],
            "address_id": [],
            "staff_id": [1, 2],
            "store_id": [1, 2]
        }

    def generate_customer(self, count):
        customers = []
        for i in range(1, count + 1):
            cust_id = i
            self.registry["customer_id"].append(cust_id)
            
            customers.append({
                "customer_id": cust_id,
                "store_id": random.choice(self.registry["store_id"]),
                "first_name": self.fake.first_name().upper(),
                "last_name": self.fake.last_name().upper(),
                "email": self.fake.email(),
                "address_id": random.randint(1, 600), # Standard dvdrental range
                "activebool": True,
                "active": 1
            })
        return customers

    def generate_rental(self, count):
        rentals = []
        if not self.registry["customer_id"]:
            print("Warning: No customers generated yet. Rentals will have empty customer links.")
            
        for i in range(1, count + 1):
            rentals.append({
                "rental_id": i,
                "rental_date": self.fake.date_time_this_year(),
                "inventory_id": random.randint(1, 4500),
                "customer_id": random.choice(self.registry["customer_id"]) if self.registry["customer_id"] else None,
                "return_date": self.fake.date_time_this_year(),
                "staff_id": random.choice(self.registry["staff_id"]),
                "last_update": self.fake.date_time_this_month()
            })
        return rentals