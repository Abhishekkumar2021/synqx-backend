from collections import defaultdict, deque
from typing import Dict, List, Set, Tuple, Optional
from app.core.errors import AppError

class DagCycleError(AppError):
    """Raised when a cycle is detected in the DAG."""
    pass

class DAG:
    """
    Represents a Directed Acyclic Graph (DAG) for pipeline execution.
    Nodes are identified by unique string IDs.
    """
    def __init__(self):
        self._graph: Dict[str, Set[str]] = defaultdict(set)  # Adjacency list: node -> set of neighbors
        self._reverse_graph: Dict[str, Set[str]] = defaultdict(set) # Reverse adjacency list: node -> set of predecessors
        self._nodes: Set[str] = set() # All unique nodes in the graph

    def add_node(self, node_id: str) -> None:
        """Adds a node to the DAG."""
        self._nodes.add(node_id)
        # Ensure node exists in graph even if it has no edges
        if node_id not in self._graph:
            self._graph[node_id] = set()
        if node_id not in self._reverse_graph:
            self._reverse_graph[node_id] = set()

    def add_edge(self, from_node: str, to_node: str) -> None:
        """
        Adds a directed edge from 'from_node' to 'to_node'.
        Both nodes are automatically added if they don't exist.
        """
        if from_node == to_node:
            raise ValueError(f"Self-loops are not allowed in a DAG: {from_node} -> {to_node}")

        self.add_node(from_node)
        self.add_node(to_node)

        self._graph[from_node].add(to_node)
        self._reverse_graph[to_node].add(from_node)
        
        # Optional: check for cycles immediately after adding an edge
        # self.check_for_cycles() # This can be expensive for large DAGs

    def get_nodes(self) -> Set[str]:
        """Returns all nodes in the DAG."""
        return self._nodes

    def get_edges(self) -> List[Tuple[str, str]]:
        """Returns all edges in the DAG as a list of (from_node, to_node) tuples."""
        edges = []
        for from_node, to_nodes in self._graph.items():
            for to_node in to_nodes:
                edges.append((from_node, to_node))
        return edges

    def get_downstream_nodes(self, node_id: str) -> Set[str]:
        """Returns all direct downstream neighbors of a node."""
        return self._graph.get(node_id, set())

    def get_upstream_nodes(self, node_id: str) -> Set[str]:
        """Returns all direct upstream predecessors of a node."""
        return self._reverse_graph.get(node_id, set())
        
    def _calculate_in_degrees(self) -> Dict[str, int]:
        """Calculates the in-degree of each node."""
        in_degrees = {node: 0 for node in self._nodes}
        for node in self._nodes:
            # The length of _reverse_graph[node] gives the in-degree
            in_degrees[node] = len(self._reverse_graph[node])
        return in_degrees

    def topological_sort(self) -> List[str]:
        """
        Performs a topological sort of the DAG using Kahn's algorithm.
        Raises DagCycleError if a cycle is detected.
        """
        in_degrees = self._calculate_in_degrees()
        queue = deque([node for node, degree in in_degrees.items() if degree == 0])
        
        sorted_nodes = []
        while queue:
            node = queue.popleft()
            sorted_nodes.append(node)

            for neighbor in self._graph[node]:
                in_degrees[neighbor] -= 1
                if in_degrees[neighbor] == 0:
                    queue.append(neighbor)
        
        if len(sorted_nodes) != len(self._nodes):
            raise DagCycleError("Cycle detected in the DAG. Topological sort not possible.")
            
        return sorted_nodes
    
    def check_for_cycles(self) -> bool:
        """
        Checks if the DAG contains any cycles.
        Returns True if a cycle is found, False otherwise.
        """
        try:
            self.topological_sort()
            return False
        except DagCycleError:
            return True

    def __str__(self):
        nodes_str = ", ".join(sorted(list(self._nodes)))
        edges_str = ", ".join([f"{u}->{v}" for u, v in self.get_edges()])
        return f"DAG(Nodes: {{{nodes_str}}}, Edges: {{{edges_str}}})"

    def __repr__(self):
        return self.__str__()