import simpy
import random
import time
import statistics
import math

# --- Configuration Constants ---
SIMULATION_TIME = 10000  # Simulation time units (e.g., ms)
NUM_NODES = 5           # Number of nodes in the cluster
CONTAINERS_PER_NODE_INITIAL = 4 # Avg containers per node initially
NODE_TYPES = {
    'TypeA': {'cpu_cores': 4, 'cpu_speed': 3.0, 'memory': 16, 'network_bw': 1000, 'llc_contention_factor': 1.0}, # Standard node
    'TypeB': {'cpu_cores': 8, 'cpu_speed': 2.5, 'memory': 32, 'network_bw': 1000, 'llc_contention_factor': 0.8}  # More cores, slightly slower, less LLC contention sensitive
}
RT_CONTAINER_PROB = 0.6 # Probability a new container is real-time
BULLY_CONTAINER_PROB = 0.15 # Probability a container is an interference generator ("bully")

# Resource Requirements (Example Ranges)
WCET_RANGE = (5, 20)      # Worst-Case Execution Time (ms)
PERIOD_RANGE = (50, 200)   # Task Period (ms)
DEADLINE_FACTOR = 0.9     # Deadline relative to Period
CPU_UTIL_RANGE = (0.1, 0.5) # Desired CPU utilization per container
MEM_REQ_RANGE = (100, 500) # Memory requirement (MB)
NET_BW_REQ_RANGE = (10, 100) # Network BW requirement (Mbps) for RT tasks

# Interference Simulation
BASE_LLC_MISS_RATE = 0.01
BULLY_LLC_IMPACT = 0.15  # Additional LLC miss rate caused by a bully on others
BASE_MEM_BW_USAGE = 50
BULLY_MEM_BW_IMPACT = 200 # Additional Mem BW used by bully

# Control & Migration Parameters
PROACTIVE_CONTROL_INTERVAL = 100 # How often proactive controllers run
INTERFERENCE_MONITOR_INTERVAL = 50
INTERFERENCE_THRESHOLD_LLC = 0.10 # LLC miss rate threshold to flag potential bully
INTERFERENCE_THRESHOLD_MEM = 150 # Mem BW usage threshold
MIGRATION_COST_BASE = 500 # Base time units for migration
MIGRATION_COST_PER_MEM = 0.5 # Additional cost per MB of memory state

# Planner Configuration
HEURISTIC_PLANNING_TIME = 0.1 # Fast heuristic time (seconds, simulated)
SMT_CHECK_TIME_PER_CRITICAL = 0.5 # Time per critical node/container (seconds, simulated)
CRITICAL_UTIL_THRESHOLD = 0.85 # Node utilization threshold for SMT check
CRITICAL_LAXITY_THRESHOLD = 10 # Minimum laxity (ms) before SMT check

# --- Helper Functions ---
def calculate_initial_q(wcet, period, cpu_util_target):
    """Calculate initial CPU budget (Q) based on target utilization."""
    # Simple model: Q/P = U => Q = U * P
    # More realistically, WCET provides a lower bound, but util target drives budget
    q = cpu_util_target * period
    # Ensure Q is at least WCET (with some buffer)
    return max(q, wcet * 1.1)

def get_node_cpu_capacity(node_config):
    """Total processing capacity (abstract units)."""
    # Simplified: cores * speed factor
    return node_config['cpu_cores'] * node_config['cpu_speed']

# --- Core Classes ---

class Container:
    """Represents a containerized application."""
    _id_counter = 0
    def __init__(self, env, container_id, is_rt, is_bully):
        self.env = env
        self.id = container_id # f"C{Container._id_counter}"
        # Container._id_counter += 1
        self.is_rt = is_rt
        self.is_bully = is_bully
        self.node = None # Assigned node object

        # Real-time properties (if applicable)
        self.wcet = random.uniform(*WCET_RANGE) if self.is_rt else 0
        self.period = random.uniform(*PERIOD_RANGE) if self.is_rt else 0
        self.deadline = self.period * DEADLINE_FACTOR if self.is_rt else float('inf')

        # Resource Requirements
        self.cpu_util_req = random.uniform(*CPU_UTIL_RANGE)
        self.mem_req = random.uniform(*MEM_REQ_RANGE)
        self.network_bw_req = random.uniform(*NET_BW_REQ_RANGE) if self.is_rt else random.uniform(1, 20) # Lower for BE

        # Assigned Resources (by orchestrator/controller)
        self.cpu_budget_q = 0 # Qk in HCBS/eHROF terms
        self.cpu_period_p = self.period if self.is_rt else 100 # Pk in HCBS/eHROF terms

        # State & Metrics
        self.running_process = None
        self.last_activation_time = -1
        self.last_completion_time = -1
        self.response_times = []
        self.deadline_misses = 0
        self.total_activations = 0
        self.current_latency = 0
        self.migration_in_progress = False

        # Interference Metrics (collected by Node's monitor)
        self.llc_miss_rate = BASE_LLC_MISS_RATE
        self.mem_bw_usage = BASE_MEM_BW_USAGE

    def get_current_cpu_demand_fraction(self):
        """Fraction of a single core's capacity this container would use if unthrottled."""
        if self.cpu_util_req < 0:
            raise ValueError("CPU utilization requirement cannot be negative.")
        return self.cpu_util_req

    def get_effective_cpu_share(self):
        """ Calculates the guaranteed CPU share based on Q and P """
        if self.cpu_period_p > 0:
            return self.cpu_budget_q / self.cpu_period_p
        return 0

    def assign_to_node(self, node, q, p):
        self.node = node
        self.cpu_budget_q = q
        self.cpu_period_p = p
        print(f"{self.env.now:.2f}: Container {self.id} assigned to Node {node.id} with Q={q:.2f}, P={p:.2f}")
        if self.is_rt:
            self.running_process = self.env.process(self.run_rt_task())

    def unassign_from_node(self):
        if self.running_process and not self.running_process.triggered:
            try:
                self.running_process.interrupt("unassigned")
            except RuntimeError:
                pass # Process already finished or interrupted
        print(f"{self.env.now:.2f}: Container {self.id} unassigned from Node {self.node.id}")
        self.node = None
        self.cpu_budget_q = 0


    def run_rt_task(self):
        """Simulates periodic real-time task execution."""
        while self.node is not None:
            try:
                # Wait for the next period
                yield self.env.timeout(self.period)
                if not self.node: break # Check if unassigned during timeout

                self.last_activation_time = self.env.now
                self.total_activations += 1

                # --- Simulate Execution & Contention ---
                # Calculate actual execution time based on budget, node load, and interference
                base_exec_time = self.wcet # Start with WCET (can be randomized)

                # Interference impact (simplified)
                contention_factor = self.node.get_current_contention_factor(self)
                actual_exec_time = base_exec_time * contention_factor

                # HCBS/Budgeting impact (Simplified: time needed / share available)
                # A proper HCBS sim is complex. This is a placeholder.
                # Assume budget Q must be consumed within period P.
                # Effective speed = allocated_share * node_speed_factor
                # time_needed = actual_exec_time / effective_speed
                # This needs a resource contention model within the Node

                # --- SimPy Resource Request (Simplified Node CPU) ---
                # This is a *very* simplified model of CPU scheduling/budgeting.
                # A real sim would need a custom SimPy resource modeling HCBS.
                # We'll use a simple delay proportional to exec time and node load.
                processing_time = self.node.calculate_processing_delay(self, actual_exec_time)

                yield self.env.timeout(processing_time)
                # --- End Simulation ---

                self.last_completion_time = self.env.now
                response_time = self.last_completion_time - self.last_activation_time
                self.response_times.append(response_time)
                self.current_latency = response_time

                if response_time > self.deadline:
                    self.deadline_misses += 1
                    # print(f"{self.env.now:.2f}: WARNING - Container {self.id} MISSED DEADLINE on Node {self.node.id} (RT={response_time:.2f}, D={self.deadline:.2f})")

            except simpy.Interrupt as i:
                 # print(f"{self.env.now:.2f}: Container {self.id} interrupted ({i.cause})")
                 if i.cause == "unassigned" or i.cause == "migration":
                     return # Exit cleanly
                 # Handle other interrupts if needed


class InterferenceMonitor:
    """Simulates monitoring shared resources (LLC, Mem B/W) per container."""
    def __init__(self, env, node):
        self.env = env
        self.node = node
        self.action = env.process(self.run())

    def run(self):
        while True:
            yield self.env.timeout(INTERFERENCE_MONITOR_INTERVAL)
            self.collect_metrics()

    def collect_metrics(self):
        bullies_on_node = [c for c in self.node.containers if c.is_bully]
        num_bullies = len(bullies_on_node)

        for container in self.node.containers:
            # Simulate LLC miss rate
            container.llc_miss_rate = BASE_LLC_MISS_RATE
            if container.is_bully:
                 container.llc_miss_rate += BULLY_LLC_IMPACT # Bully's own high rate
            else:
                 # Victim's rate increases based on number of bullies
                 container.llc_miss_rate += num_bullies * BULLY_LLC_IMPACT * random.uniform(0.8, 1.2)

            # Simulate Memory Bandwidth Usage
            container.mem_bw_usage = BASE_MEM_BW_USAGE
            if container.is_bully:
                 container.mem_bw_usage += BULLY_MEM_BW_IMPACT * random.uniform(0.8, 1.2)
            # Victims don't necessarily use more BW, but might stall more (captured elsewhere)

            # Add noise/variation
            container.llc_miss_rate *= random.uniform(0.9, 1.1)
            container.mem_bw_usage *= random.uniform(0.9, 1.1)

            # Clamp values
            container.llc_miss_rate = max(0, container.llc_miss_rate)
            container.mem_bw_usage = max(0, container.mem_bw_usage)

            # Report to Node Controller (implicitly available via container object)
            # print(f"{self.env.now:.2f}: Monitor Node {self.node.id} - Cont {container.id}: LLC Miss={container.llc_miss_rate:.4f}, Mem BW={container.mem_bw_usage:.1f}")


class PredictiveController:
    """Placeholder for MPC/RL controller logic."""
    def __init__(self, env, node):
        self.env = env
        self.node = node
        self.action = env.process(self.run())

    def run(self):
        while True:
            yield self.env.timeout(PROACTIVE_CONTROL_INTERVAL)
            self.predict_and_adjust()

    def predict_and_adjust(self):
        # --- This is where MPC/RL logic would go ---
        # 1. Gather State: Current metrics (latency, misses), interference metrics, potentially short-term workload predictions.
        # 2. Predict Future State (MPC): Use a system model (e.g., ResponseTime = f(Q, Load, Interference)).
        # 3. Optimize Control Action (MPC): Solve optimization problem to find new Qk minimizing predicted misses/latency + resource cost.
        # 4. Select Action (RL): Use the learned policy (State -> Action) to choose adjustments to Qk.
        # --- Placeholder Logic ---
        for container in self.node.containers:
            if not container.is_rt: continue

            # Simple Proactive Rule: If recent latency is high but deadline not missed, slightly increase budget.
            recent_avg_latency = statistics.mean(container.response_times[-5:]) if len(container.response_times) >= 5 else container.current_latency
            target_latency = container.deadline * 0.8 # Target some headroom

            if recent_avg_latency > target_latency and recent_avg_latency < container.deadline:
                 new_q = container.cpu_budget_q * 1.05 # Increase budget slightly
                 # Check against node capacity constraints before applying
                 if self.node.can_afford_budget_increase(container, new_q - container.cpu_budget_q):
                     # print(f"{self.env.now:.2f}: ProactiveCTRL Node {self.node.id}: Increasing Cont {container.id} Q to {new_q:.2f} (Latency={recent_avg_latency:.2f})")
                     container.cpu_budget_q = new_q
                 # else:
                     # print(f"{self.env.now:.2f}: ProactiveCTRL Node {self.node.id}: Cannot increase Cont {container.id} budget, node near capacity.")

            # Simple Reactive Rule (complementary): If deadline missed, increase more significantly (this overlaps HROF's original reaction)
            # if container.current_latency > container.deadline:
            #      new_q = container.cpu_budget_q * 1.2
            #      # Check capacity
            #      container.cpu_budget_q = new_q


class Node:
    """Represents a physical node in the cluster."""
    _id_counter = 0
    def __init__(self, env, cluster, node_type):
        self.env = env
        self.cluster = cluster
        self.id = f"N{Node._id_counter}"
        Node._id_counter += 1
        self.config = NODE_TYPES[node_type]
        self.node_type = node_type
        self.containers = []

        # Resources
        self.cpu_capacity_total = get_node_cpu_capacity(self.config) # Abstract units
        self.mem_capacity = self.config['memory']
        self.network_bw_capacity = self.config['network_bw']
        # self.cpu_resource = simpy.Resource(env, capacity=self.config['cpu_cores']) # More complex resource model needed for HCBS

        # eHROF Components
        self.interference_monitor = InterferenceMonitor(env, self)
        self.predictive_controller = PredictiveController(env, self) # Can choose MPC/RL variant

        # State for Cluster Reporting
        self.potential_bully_containers = set()
        self.victim_containers = set()
        self.is_overloaded = False

        self.action = env.process(self.monitor_and_report()) # Periodic check for cluster


    def add_container(self, container, q, p):
        if self.can_accommodate(container, q, p):
            self.containers.append(container)
            container.assign_to_node(self, q, p)
            return True
        return False

    def remove_container(self, container):
        if container in self.containers:
            container.unassign_from_node()
            self.containers.remove(container)
            return True
        return False

    def get_current_cpu_utilization(self):
        """Calculates the total allocated CPU share."""
        total_share = sum(c.get_effective_cpu_share() for c in self.containers)
        # Normalize by total capacity (can exceed 1.0 if oversubscribed)
        # return total_share / self.config['cpu_cores']
        # Let's return the sum of shares, Cluster can compare to capacity
        return total_share

    def get_current_mem_usage(self):
        return sum(c.mem_req for c in self.containers)

    def get_current_network_usage(self):
        return sum(c.network_bw_req for c in self.containers)

    def can_accommodate(self, container, q, p):
        """Check if node has enough resources for a new container."""
        # Check CPU
        prospective_share = (q / p) if p > 0 else 0
        current_share = sum(c.get_effective_cpu_share() for c in self.containers)
        # Use a threshold < 1.0 to leave buffer
        if (current_share + prospective_share) > self.config['cpu_cores'] * 0.95:
             # print(f"Node {self.id}: CPU check failed for {container.id}. Current: {current_share:.2f}, Req: {prospective_share:.2f}, Capacity: {self.config['cpu_cores']}")
             return False

        # Check Memory
        if (self.get_current_mem_usage() + container.mem_req) > self.mem_capacity:
            # print(f"Node {self.id}: Memory check failed for {container.id}")
            return False

        # Check Network BW
        if (self.get_current_network_usage() + container.network_bw_req) > self.network_bw_capacity:
            # print(f"Node {self.id}: Network check failed for {container.id}")
            return False

        return True

    def can_afford_budget_increase(self, container, delta_q):
        """Check if increasing a container's budget Q is feasible."""
        current_share = sum(c.get_effective_cpu_share() for c in self.containers if c != container)
        new_container_share = (container.cpu_budget_q + delta_q) / container.cpu_period_p
        return (current_share + new_container_share) <= self.config['cpu_cores'] * 0.98 # Allow slightly tighter packing for adjustments

    def get_current_contention_factor(self, target_container):
        """Simplified model of performance degradation due to shared resource contention."""
        factor = 1.0
        # LLC Contention Impact
        num_bullies = len([c for c in self.containers if c.is_bully and c != target_container])
        # Node sensitivity and number of bullies increase contention
        factor += num_bullies * 0.1 * self.config['llc_contention_factor']

        # Overall Load Impact (simple)
        # load = self.get_current_cpu_utilization() / self.config['cpu_cores']
        # factor += load * 0.1 # High load slightly increases contention

        return max(1.0, factor * random.uniform(0.95, 1.05)) # Add some noise

    def calculate_processing_delay(self, container, base_exec_time):
        """ VERY simplified placeholder for HCBS/scheduling delay """
        # Needs proper resource modeling
        # Simple version: delay = base_time / allocated_share + factor*load
        allocated_share = container.get_effective_cpu_share()
        if allocated_share <= 0.001: return base_exec_time * 10 # Penalize if no budget

        # Simulated delay based on budget ratio
        sim_delay = base_exec_time / allocated_share

        # Add small delay based on overall node load
        node_load_factor = sum(c.get_effective_cpu_share() for c in self.containers) / self.config['cpu_cores']
        sim_delay *= (1 + node_load_factor * 0.2) # Increase delay slightly with higher load

        return max(base_exec_time, sim_delay) # Ensure at least base time

    def monitor_and_report(self):
         """Periodically checks status and reports triggers to Cluster."""
         while True:
            yield self.env.timeout(self.cluster.orchestration_interval) # Align with cluster checks

            current_victims = set()
            current_bullies = set()
            potential_degradation = False

            # Check for deadline misses (victims)
            for c in self.containers:
                if c.is_rt and c.deadline_misses > 0 and (self.env.now - c.last_completion_time) < self.cluster.orchestration_interval * 2 : # Recent miss
                    # Check if budget is already maxed out (or near node capacity limit)
                    if not self.can_afford_budget_increase(c, c.cpu_budget_q * 0.1): # Cannot give 10% more
                        current_victims.add(c.id)
                        # print(f"{self.env.now:.2f}: Node {self.id}: Container {c.id} identified as VICTIM (missed deadline, cannot increase budget).")


            # Check for interference sources (bullies)
            for c in self.containers:
                 if c.llc_miss_rate > INTERFERENCE_THRESHOLD_LLC or c.mem_bw_usage > INTERFERENCE_THRESHOLD_MEM:
                      # Correlation check (simplified): Did *any* RT task miss deadline recently?
                      # A real system needs better correlation.
                      if any(vic_id in current_victims for vic_id in [cont.id for cont in self.containers if cont.is_rt]):
                          current_bullies.add(c.id)
                          # print(f"{self.env.now:.2f}: Node {self.id}: Container {c.id} identified as potential BULLY (high interference metrics + victim present).")


            # Check for overload
            cpu_util = self.get_current_cpu_utilization() / self.config['cpu_cores']
            mem_util = self.get_current_mem_usage() / self.mem_capacity
            net_util = self.get_current_network_usage() / self.network_bw_capacity
            self.is_overloaded = cpu_util > 0.9 or mem_util > 0.9 or net_util > 0.9
            # if self.is_overloaded:
            #      print(f"{self.env.now:.2f}: Node {self.id} identified as OVERLOADED (CPU={cpu_util:.2f}, Mem={mem_util:.2f}, Net={net_util:.2f}).")

            # Report significant changes to Cluster
            if current_victims != self.victim_containers or \
               current_bullies != self.potential_bully_containers or \
               self.is_overloaded:
                self.victim_containers = current_victims
                self.potential_bully_containers = current_bullies
                self.cluster.receive_node_report(self.id, self.victim_containers, self.potential_bully_containers, self.is_overloaded)


class HybridPlanner:
    """Simulates the scalable hybrid offline planner."""
    def __init__(self, cluster):
        self.cluster = cluster

    def initial_placement(self, containers_to_place):
        print("\n--- Starting Hybrid Offline Planning ---")
        start_time = time.time()

        # --- Phase 1: Heuristic Placement and Dimensioning ---
        print("Phase 1: Running Heuristic Placement (Best Fit Decreasing - CPU, Network)")
        placement = {} # container_id -> node_id
        node_assignments = {node.id: [] for node in self.cluster.nodes}
        node_resources = {node.id: {'cpu': 0, 'mem': 0, 'net': 0} for node in self.cluster.nodes}
        initial_budgets = {} # container_id -> (Q, P)

        # Sort containers (e.g., by CPU requirement descending, prioritize RT)
        containers_to_place.sort(key=lambda c: (not c.is_rt, c.cpu_util_req * c.period), reverse=True) # Approx CPU demand

        for container in containers_to_place:
            best_node = None
            min_cpu_increase = float('inf') # Using BFD for CPU primarily

            q = calculate_initial_q(container.wcet, container.period, container.cpu_util_req)
            p = container.period if container.is_rt else 100
            container_cpu_share = q / p if p > 0 else 0

            for node in self.cluster.nodes:
                 # Check absolute capacity first
                 can_fit_raw = (node_resources[node.id]['cpu'] + container_cpu_share <= node.config['cpu_cores'] and
                                node_resources[node.id]['mem'] + container.mem_req <= node.config['memory'] and
                                node_resources[node.id]['net'] + container.network_bw_req <= node.config['network_bw'])

                 if can_fit_raw:
                      # BFD Logic: Find the node that fits and results in the 'least' remaining capacity (tightest fit for this resource)
                      # Simplified: Track the node that fits, prioritize nodes already used
                      # For BFD on CPU share:
                      current_node_cpu = node_resources[node.id]['cpu']
                      if best_node is None or current_node_cpu > node_resources[best_node.id]['cpu']: # Prefer fuller node
                          best_node = node

                      # Could also use more complex heuristics (multi-dimensional BFD)

            if best_node:
                 placement[container.id] = best_node.id
                 node_assignments[best_node.id].append(container)
                 node_resources[best_node.id]['cpu'] += container_cpu_share
                 node_resources[best_node.id]['mem'] += container.mem_req
                 node_resources[best_node.id]['net'] += container.network_bw_req
                 initial_budgets[container.id] = (q, p)
            else:
                 print(f"ERROR: Could not place container {container.id} heuristically!")
                 # Handle placement failure (e.g., add more nodes, reject container)
                 return None, None # Indicate failure

        heuristic_time = time.time() - start_time
        print(f"Phase 1 complete in {heuristic_time:.4f}s (Simulated: {HEURISTIC_PLANNING_TIME:.4f}s)")
        # Simulate the heuristic part taking some base time
        if heuristic_time < HEURISTIC_PLANNING_TIME:
             time.sleep(HEURISTIC_PLANNING_TIME - heuristic_time)


        # --- Phase 2: Identification of Critical Assignments ---
        print("Phase 2: Identifying Critical Nodes/Containers")
        critical_nodes = set()
        critical_containers = set()

        for node_id, resources in node_resources.items():
            node = self.cluster.get_node(node_id)
            cpu_util = resources['cpu'] / node.config['cpu_cores']
            mem_util = resources['mem'] / node.config['memory']
            net_util = resources['net'] / node.config['network_bw']
            if cpu_util > CRITICAL_UTIL_THRESHOLD or mem_util > CRITICAL_UTIL_THRESHOLD or net_util > CRITICAL_UTIL_THRESHOLD:
                critical_nodes.add(node_id)
                print(f" - Node {node_id} marked critical (high utilization)")

        for container_id, node_id in placement.items():
             container = self.cluster.get_container(container_id)
             if container.is_rt:
                  # Estimate laxity (simplified - ignores contention)
                  q, p = initial_budgets[container_id]
                  if p > 0 and q > 0 :
                       estimated_rt = container.wcet / (q / p)
                       laxity = container.deadline - estimated_rt
                       if laxity < CRITICAL_LAXITY_THRESHOLD:
                            critical_containers.add(container_id)
                            critical_nodes.add(node_id) # Mark node as critical too
                            print(f" - Container {container_id} on Node {node_id} marked critical (low laxity: {laxity:.2f}ms)")

        # --- Phase 3: Targeted SMT/OMT Refinement (Simulated) ---
        print(f"Phase 3: Running Simulated SMT/OMT on {len(critical_nodes)} critical nodes and {len(critical_containers)} containers")
        smt_start_time = time.time()
        refined_budgets = initial_budgets.copy()
        placement_feasible = True

        # Simulate SMT solver time based on number of critical elements
        simulated_smt_duration = len(critical_nodes | critical_containers) * SMT_CHECK_TIME_PER_CRITICAL
        time.sleep(simulated_smt_duration) # Simulate the SMT check time

       
        for node_id in critical_nodes:
             node = self.cluster.get_node(node_id)
             cpu_load = node_resources[node_id]['cpu'] / node.config['cpu_cores']
             if cpu_load > 1.05: # If heuristic packed > 105% CPU
                  print(f" - SMT Check FAILED (Simulated) for Node {node_id} - likely infeasible packing.")
                  placement_feasible = False
                  # Need logic here to trigger re-planning or adjustment
                  break # Stop checking if one fails

        if placement_feasible:
             print(f"Phase 3 SMT Check Passed (Simulated)")
        else:
             print(f"Phase 3 SMT Check FAILED (Simulated) - Refinement needed (not implemented)")
             # Potentially try to adjust budgets down slightly or re-run heuristic for failed nodes
             return None, None # Indicate failure

        total_planning_time = (time.time() - start_time) + HEURISTIC_PLANNING_TIME
        print(f"--- Hybrid Offline Planning Complete in {total_planning_time:.4f}s (Simulated) ---")

        return placement, refined_budgets


class EHROFCluster:
    """Manages the entire cluster orchestration."""
    def __init__(self, env):
        self.env = env
        self.nodes = []
        self.containers = {} # All containers, {id: container_obj}
        self.planner = HybridPlanner(self)
        self.orchestration_interval = 100 # How often cluster logic runs
        self.action = env.process(self.run_orchestration())
        self.node_reports = {} # {node_id: {'victims': set(), 'bullies': set(), 'overloaded': bool}}
        self.migration_cooldown = {} # {container_id: end_time}
        self.stats = {'migrations': 0, 'planning_time': 0}

    def add_node(self, node_type):
        node = Node(self.env, self, node_type)
        self.nodes.append(node)
        print(f"{self.env.now:.2f}: Added Node {node.id} ({node_type})")
        return node

    def add_container_object(self, container):
         self.containers[container.id] = container

    def deploy_initial_containers(self, initial_containers):
        """Uses the Hybrid Planner for initial deployment."""
        self.containers.update({c.id: c for c in initial_containers})
        start_time = time.time()
        placement, budgets = self.planner.initial_placement(initial_containers)
        self.stats['planning_time'] = time.time() - start_time

        if placement and budgets:
            for container_id, node_id in placement.items():
                container = self.get_container(container_id)
                node = self.get_node(node_id)
                q, p = budgets[container_id]
                if not node.add_container(container, q, p):
                    print(f"ERROR: Node {node_id} could not add container {container_id} after planning!")
                    # Handle this error - maybe try placing elsewhere?
            print("Initial deployment complete.")
        else:
            print("ERROR: Initial planning failed.")
            # Handle planning failure

    def get_node(self, node_id):
        for node in self.nodes:
            if node.id == node_id:
                return node
        return None

    def get_container(self, container_id):
        return self.containers.get(container_id)

    def receive_node_report(self, node_id, victims, bullies, is_overloaded):
         # print(f"{self.env.now:.2f}: Cluster received report from Node {node_id}: Victims={victims}, Bullies={bullies}, Overloaded={is_overloaded}")
         self.node_reports[node_id] = {'victims': victims, 'bullies': bullies, 'overloaded': is_overloaded}
         # Trigger immediate check if report is critical
         if victims or bullies or is_overloaded:
              if not self.action.is_alive: # Avoid rescheduling if already running/pending
                   self.action = self.env.process(self.run_orchestration(immediate=True))


    def run_orchestration(self, immediate=False):
        """Main cluster control loop for monitoring and migration."""
        if not immediate:
             yield self.env.timeout(self.orchestration_interval)

        # print(f"\n{self.env.now:.2f}: === Cluster Orchestration Cycle ===")
        migration_candidates = [] # List of (reason, container_id, source_node_id)

        # Process reports
        for node_id, report in self.node_reports.items():
            if report['victims']:
                 for victim_id in report['victims']:
                      if self.env.now > self.migration_cooldown.get(victim_id, 0):
                           migration_candidates.append(('victim', victim_id, node_id))
            if report['bullies']:
                 for bully_id in report['bullies']:
                      if self.env.now > self.migration_cooldown.get(bully_id, 0):
                           migration_candidates.append(('bully', bully_id, node_id))
            if report['overloaded']:
                 # Find a container to migrate off overloaded node (simple: largest CPU user?)
                 node = self.get_node(node_id)
                 if node:
                      candidates = sorted([c for c in node.containers if self.env.now > self.migration_cooldown.get(c.id, 0)],
                                           key=lambda c: c.get_effective_cpu_share(), reverse=True)
                      if candidates:
                           migration_candidates.append(('overload', candidates[0].id, node_id))

        if not migration_candidates:
             # print("No migration triggers this cycle.")
             # Schedule next regular run if this was immediate
             if immediate: self.action = self.env.process(self.run_orchestration())
             return

        # --- Migration Decision Logic ---
        # Prioritize migrating bullies over victims if both are flagged on the same node
        # Simplification: Process one migration per cycle for stability
        print(f"{self.env.now:.2f}: Migration Candidates: {migration_candidates}")

        chosen_migration = None
        # Prioritize bullies
        bully_migrations = [(r, c, n) for r, c, n in migration_candidates if r == 'bully']
        if bully_migrations:
             chosen_migration = random.choice(bully_migrations)
        else:
             # Then victims or overload
             chosen_migration = random.choice(migration_candidates)

        if chosen_migration:
            reason, container_id, source_node_id = chosen_migration
            container = self.get_container(container_id)
            source_node = self.get_node(source_node_id)

            if not container or not source_node or container.migration_in_progress:
                 print(f"Skipping migration for {container_id} - invalid state.")
            else:
                print(f"{self.env.now:.2f}: Attempting migration for {container_id} from {source_node_id} (Reason: {reason})")
                self.env.process(self.execute_migration(container, source_node))


        # Clear reports for next cycle
        self.node_reports = {}
        # Schedule next run
        if immediate and self.action.is_alive: # If triggered immediately, ensure next regular one is scheduled
             pass # Already rescheduled potentially by the trigger report logic
        elif not self.action.is_alive:
              self.action = self.env.process(self.run_orchestration())


    def find_target_node(self, container_to_migrate, source_node):
        """Finds a suitable node for migration."""
        best_target = None
        min_load = float('inf')

        for node in self.nodes:
            if node == source_node: continue

            # Check capacity (consider Q/P from original container)
            q, p = container_to_migrate.cpu_budget_q, container_to_migrate.cpu_period_p
            if node.can_accommodate(container_to_migrate, q, p):
                # Policy: Choose least loaded node (simple load balancing)
                current_cpu_load = node.get_current_cpu_utilization()
                if current_cpu_load < min_load:
                    # Interference check: Avoid placing on node with known bullies if migrating victim?
                    is_victim = container_to_migrate.id in source_node.victim_containers
                    target_has_bully = any(c.is_bully for c in node.containers)
                    if is_victim and target_has_bully:
                        continue # Avoid moving victim to another bully node if possible

                    min_load = current_cpu_load
                    best_target = node

        return best_target

    def execute_migration(self, container, source_node):
        """Simulates the container migration process."""
        container.migration_in_progress = True
        target_node = self.find_target_node(container, source_node)

        if target_node:
            print(f"{self.env.now:.2f}: Migrating {container.id} from {source_node.id} -> {target_node.id}")
            self.stats['migrations'] += 1

            # 1. Pause/Checkpoint on source (interrupt task)
            if container.running_process and not container.running_process.triggered:
                 try:
                     container.running_process.interrupt("migration")
                 except RuntimeError: pass # Already finished

            # 2. Remove from source node's list (logically)
            source_node.remove_container(container)

            # 3. Simulate transfer time (cost model)
            migration_cost = MIGRATION_COST_BASE + container.mem_req * MIGRATION_COST_PER_MEM
            yield self.env.timeout(migration_cost)

            # 4. Add to target node
            q, p = container.cpu_budget_q, container.cpu_period_p # Keep original budget for now
            if target_node.add_container(container, q, p):
                 print(f"{self.env.now:.2f}: Migration complete for {container.id} to {target_node.id}")
                 # Add cooldown period
                 self.migration_cooldown[container.id] = self.env.now + migration_cost * 2 # Cooldown proportional to cost
            else:
                 # Migration failed! Target node couldn't accommodate after all?
                 # Put it back on source for now (or handle error better)
                 print(f"ERROR: Migration failed for {container.id}! Target {target_node.id} rejected. Returning to {source_node.id}")
                 source_node.add_container(container, q, p) # Re-add

        else:
            print(f"Could not find suitable target node for migrating {container.id} from {source_node.id}")
            # Implement fallback: Maybe adjust budget on source if possible?

        container.migration_in_progress = False


    def get_final_stats(self):
        total_misses = 0
        total_activations = 0
        avg_latencies = []
        for c in self.containers.values():
             if c.is_rt:
                  total_misses += c.deadline_misses
                  total_activations += c.total_activations
                  if c.response_times:
                       avg_latencies.append(statistics.mean(c.response_times))

        dmr = (total_misses / total_activations) * 100 if total_activations > 0 else 0
        avg_latency = statistics.mean(avg_latencies) if avg_latencies else 0
        cpu_utils = [n.get_current_cpu_utilization() / n.config['cpu_cores'] for n in self.nodes]
        avg_cpu_util = statistics.mean(cpu_utils) * 100 if cpu_utils else 0

        print("\n--- Simulation Complete ---")
        print(f"Total Migrations: {self.stats['migrations']}")
        print(f"Total Deadline Misses: {total_misses}")
        print(f"Total RT Activations: {total_activations}")
        print(f"Overall Deadline Miss Ratio (DMR): {dmr:.2f}%")
        print(f"Average RT Task Latency: {avg_latency:.2f} ms")
        print(f"Average Node CPU Utilization: {avg_cpu_util:.2f}%")
        print(f"Initial Planning Time (Simulated): {self.stats['planning_time']:.4f}s")


# --- Simulation Setup ---
if __name__ == "__main__":
    env = simpy.Environment()
    cluster = EHROFCluster(env)

    # Create Nodes
    for i in range(NUM_NODES):
        node_type = random.choice(list(NODE_TYPES.keys()))
        cluster.add_node(node_type)

    # Create Initial Containers
    initial_containers = []
    total_initial = NUM_NODES * CONTAINERS_PER_NODE_INITIAL
    for i in range(total_initial):
         is_rt = random.random() < RT_CONTAINER_PROB
         is_bully = random.random() < BULLY_CONTAINER_PROB if not is_rt else False # Bullies usually BE in this model
         container = Container(env, f"C{i}", is_rt, is_bully)
         initial_containers.append(container)

    # Deploy using eHROF Planner
    cluster.deploy_initial_containers(initial_containers)

    # Start Simulation
    print(f"\n--- Running Simulation for {SIMULATION_TIME} time units ---")
    env.run(until=SIMULATION_TIME)

    # Print Final Stats
    cluster.get_final_stats()