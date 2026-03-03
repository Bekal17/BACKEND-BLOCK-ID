from collections import deque


def compute_graph_distance(graph, scam_wallets, max_distance=4):
    """
    Compute BFS-based graph distance from known scam wallets.

    Args:
        graph: dict[str, list[str]] mapping wallet -> neighbors.
        scam_wallets: iterable of scam wallet addresses (seed nodes).
        max_distance: maximum hop distance to propagate.

    Returns:
        dict[str, int]: wallet -> distance (0 for scam wallets, 1 for direct neighbors, etc.).
    """

    distance: dict[str, int] = {}
    queue: deque[str] = deque()

    for w in scam_wallets:
        if w in distance:
            continue
        distance[w] = 0
        queue.append(w)

    while queue:
        current = queue.popleft()
        d = distance[current]

        if d >= max_distance:
            continue

        for neighbor in graph.get(current, []):
            if neighbor not in distance:
                distance[neighbor] = d + 1
                queue.append(neighbor)

    print("[GRAPH DISTANCE SAMPLE]")
    for w in list(distance.keys())[:10]:
        print(w, distance[w])

    return distance

