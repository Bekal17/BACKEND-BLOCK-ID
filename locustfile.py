from locust import HttpUser, task, between
import csv
import random

# Load wallets dari CSV
wallets = []
with open("wallets.csv") as f:
    for row in csv.DictReader(f):
        wallets.append(row["wallet"])

class BlockIDUser(HttpUser):
    wait_time = between(1, 2)

    @task
    def trust_score_list(self):
        # ambil random 5 wallet tiap request
        sample = random.sample(wallets, min(5, len(wallets)))

        self.client.post(
            "/api/trust-score/list",
            json=sample
        )

