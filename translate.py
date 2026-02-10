import json

queries = []
with open("queries.json", "r") as f:
    queries = json.load(f)

for q in queries:
    qn = int(q["query_nr"])
    with open(f"q{qn}.sql", "w") as f:
        f.write(q["query"])
