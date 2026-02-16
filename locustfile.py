from locust import HttpUser, task, between

# Pakai wallet yang sudah kamu publish di BlockID
wallets = [
    "EgVMwBG3hLodAMPHbHgEuAXfYcxoWCwrsLAXwXGkWM7H",
    "4syyc9yo5NKW2RVEU4YSVxjxtDfDBT6R9MFsKSVU8Bts",
]

class BlockIDUser(HttpUser):
    wait_time = between(1, 2)

    @task
    def get_scores(self):
        self.client.post(
            "/api/trust-score/list",
            json={"wallets": wallets}
        )
