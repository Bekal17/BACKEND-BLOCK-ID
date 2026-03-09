import re

with open("schema.sql", "r", encoding="utf-8") as f:
    schema = f.read()

schema = schema.replace(
    "INTEGER PRIMARY KEY AUTOINCREMENT",
    "SERIAL PRIMARY KEY"
)

schema = schema.replace("REAL", "DOUBLE PRECISION")

schema = re.sub(r"INTEGER,", "BIGINT,", schema)

with open("schema_postgres.sql", "w", encoding="utf-8") as f:
    f.write(schema)

print("Converted schema saved to schema_postgres.sql")