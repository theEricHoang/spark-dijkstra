# spark-dijkstra by eric hoang
for Cloud Computing spring 2025

finding the shortest paths using Dijkstra's and Apache Spark

## prerequisites
- Apache Spark 3.3.2
- Python 3.10.6
- text file in this format: (example included in this repo)
```
num_nodes   num_edges
source      destination     weight
source      destination     weight
...
```

## running
clone the repository and enter directory:
```bash
$ git clone https://github.com/theEricHoang/spark-dijkstra
$ cd spark-dijkstra
```

initialize virtual environment and install dependencies:
```bash
$ python3 -m venv .venv
$ source .venv/bin/activate
$ pip install -r requirements.txt
```

run the program:
```bash
$ spark-submit main.py <input_file> [source_node]
$ spark-submit main.py weighted_graph.txt 0 # example
```