import httpx

resp = httpx.post(
    "https://shield.openfort.io/project/encryption-session",
    headers={
        "x-api-key": "2ec5c69f-7880-44b6-9e77-98aa7addec06",
        "x-api-secret": "7b7797daff8078b1925a9a3a0b07fd2dd8bee7228b8551a6e3fae2ada7d86977",
        "x-encryption-part": "ApbvHEdwGOpqnqp59zBS0BBQM6ou4rHBwJ7Fixc96ZPD",
        "Content-Type": "application/json",
    },
    json={},
)

print("Status:", resp.status_code)
print("Body:", resp.text)