import httpx

with open("test_data.parquet", "rb") as f:
    b = f.read()

result = httpx.post(
    "",
    data={
        "parquet": bytes(b),
        "column": "Close",
        "max_imfs": 10,
    },
    timeout=600,
)

print(result)
