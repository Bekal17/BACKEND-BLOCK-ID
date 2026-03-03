from collections import defaultdict, deque
import time

from backend_blockid.database.connection import get_connection


conn = get_connection()
cur = conn.cursor()

cur.execute("SELECT sender_wallet, receiver_wallet FROM wallet_graph_edges")
edges = cur.fetchall()

graph: dict[str, set[str]] = defaultdict(set)
for r in edges:
    graph[r["sender_wallet"]].add(r["receiver_wallet"])
    graph[r["receiver_wallet"]].add(r["sender_wallet"])

visited: set[str] = set()
cluster_id = 0

for wallet in graph:
    if wallet in visited:
        continue

    cluster_id += 1

    queue: deque[str] = deque([wallet])

    while queue:
        w = queue.popleft()
        if w in visited:
            continue
        visited.add(w)

        cur.execute(
            """
            INSERT OR IGNORE INTO wallet_cluster_members (cluster_id, wallet, added_at)
            VALUES (?, ?, ?)
            """,
            (cluster_id, w, int(time.time())),
        )

        for n in graph[w]:
            if n not in visited:
                queue.append(n)

conn.commit()
conn.close()

print("Clusters created:", cluster_id)

