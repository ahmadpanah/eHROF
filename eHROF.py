import time
import random
import heapq
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple

SIMULATION_DURATION_SECONDS = 120
CONTROL_INTERVAL_SECONDS = 1
NODE_CPU_CAPACITY = 1000
NODE_MEMORY_CAPACITY_MB = 4096
NODE_NETWORK_BW_MBPS = 1000
DEFAULT_CONTAINER_CPU_PERIOD = 100.0
DEFAULT_CONTAINER_CPU_BUDGET = 20.0
MIN_CONTAINER_CPU_BUDGET = 5.0

PROACTIVE_CONTROL_TYPE = "placeholder"
INTERFERENCE_MONITOR_TYPE = "simulated"
OFFLINE_PLANNER_TYPE = "heuristic_placeholder"

@dataclass
class Task:
    id: int
    wcet: float
    period: float
    deadline: float

@dataclass
class ContainerMetrics:
    last_response_time: float = 0.0
    deadline_misses_consecutive: int = 0
    total_deadline_misses: int = 0
    current_laxity: float = 0.0
    avg_cpu_usage_millicores: float = 0.0
    avg_network_usage_mbps: float = 0.0
    llc_miss_rate: float = 0.0
    memory_bw_usage_gbps: float = 0.0

@dataclass
class Container:
    id: str
    tasks: List[Task]
    req_memory_mb: int
    req_network_bw_mbps: float
    is_real_time: bool = True
    is_sensitive_to_interference: bool = True
    cpu_period: float = DEFAULT_CONTAINER_CPU_PERIOD
    cpu_budget: float = DEFAULT_CONTAINER_CPU_BUDGET
    cpu_budget_min: float = MIN_CONTAINER_CPU_BUDGET
    target_deadline_or_response_time: float = 0.0
    assigned_node_id: Optional[str] = None
    metrics: ContainerMetrics = field(default_factory=ContainerMetrics)
    control_state: Dict = field(default_factory=dict)
    is_potential_bully: bool = False
    migration_pending: bool = False
    next_job_release_time: float = 0.0
    last_job_finish_time: float = 0.0

@dataclass
class Node:
    id: str
    cpu_capacity: int
    memory_capacity_mb: int
    network_bw_mbps: int
    node_type: str = "generic"
    containers: Dict[str, Container] = field(default_factory=dict)
    available_cpu: int = field(init=False)
    available_memory_mb: int = field(init=False)
    available_network_bw_mbps: int = field(init=False)
    total_cpu_utilization_millicores: float = 0.0
    total_memory_usage_mb: int = 0
    total_network_usage_mbps: float = 0.0
    is_overloaded: bool = False
    interference_monitor: Optional['InterferenceMonitor'] = None
    node_controller: Optional['NodeLevelController'] = None

    def __post_init__(self):
        self.available_cpu = self.cpu_capacity
        self.available_memory_mb = self.memory_capacity_mb
        self.available_network_bw_mbps = self.network_bw_mbps

@dataclass
class Cluster:
    nodes: Dict[str, Node] = field(default_factory=dict)
    containers: Dict[str, Container] = field(default_factory=dict)
    cluster_controller: Optional['ClusterLevelController'] = None
    deployment_queue: List[Container] = field(default_factory=list)
    migration_queue: List[Tuple[str, str, str]] = field(default_factory=list)