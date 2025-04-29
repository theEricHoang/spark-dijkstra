from pyspark import SparkContext, SparkConf
import sys
import heapq

def parse_edge(line):
    """source, destination, weight"""
    parts = line.strip().split()
    return int(parts[0]), int(parts[1]), float(parts[2])

def load_graph(path, sc):
    lines = sc.textFile(path)

    # parse # of nodes and edges
    header = lines.first()
    data_lines = lines.filter(lambda line: line != header) # all the other lines excluding the header

    header_parts = header.split()
    num_nodes = int(header_parts[0])
    edges = data_lines.map(parse_edge)

    # adjacency list representation
    graph = edges.map(lambda edge: (edge[0], [(edge[1], edge[2])])) \
                 .reduceByKey(lambda a, b: a + b) \
                 .collectAsMap()
    
    return graph, num_nodes

def dijkstra(graph, num_nodes, src):
    distances = {node: float('inf') for node in range(num_nodes)}
    distances[src] = 0

    # priority queue
    pq = [(0, src)]

    while pq:
        current_dist, current_node = heapq.heappop(pq)
        
        # skip longer paths
        if current_dist > distances[current_node]:
            continue

        if current_node in graph:
            for neighbor, weight in graph[current_node]:
                distance = current_dist + weight

                # update shortest path
                if distance < distances[neighbor]:
                    distances[neighbor] = distance
                    heapq.heappush(pq, (distance, neighbor))

    return distances

def main():
    conf = SparkConf().setAppName("Dijkstra")
    sc = SparkContext(conf=conf)

    if len(sys.argv) < 2:
        print("Usage: spark-submit main.py <input_file> [source_node]")
        sys.exit(1)

    input_file = sys.argv[1]
    source_node = 0

    if len(sys.argv) >= 3:
        source_node = int(sys.argv[2])

    graph, num_nodes = load_graph(input_file, sc)
    distances = dijkstra(graph, num_nodes, source_node)

    print(f"Shortest distances from node {source_node}:")
    for node in range(num_nodes):
        if distances[node] == float('inf'):
            print(f"Node {node}: INF")
        else:
            print(f"Node {node}: {int(distances[node])}")

    sc.stop()

if __name__ == "__main__":
    main()