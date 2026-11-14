"""Enterprise Concurrent Swarm Router Engine.

Inspired by kyegomez/swarms and LangGraph orchestration patterns.
Provides stateful graph execution, agent dispatch, routing conditions,
and concurrent batch processing with strict typing and robust error handling.
"""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
import logging
from typing import Any, Callable, Dict, List, Optional, Protocol, Sequence, Union
import uuid

logger = logging.getLogger("enterprise_swarm.engine")
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [SwarmEngine] %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
logger.setLevel(logging.INFO)


class TaskStatus(str, Enum):
    """Execution status for swarm tasks."""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    ROUTED = "ROUTED"


@dataclass
class SwarmMessage:
    """Represents an atomic message payload in the Swarm Mesh."""
    role: str
    content: str
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SwarmState:
    """State schema passed across graph nodes and agent routers."""
    task_id: str
    input_payload: Dict[str, Any]
    status: TaskStatus = TaskStatus.PENDING
    history: List[SwarmMessage] = field(default_factory=list)
    context: Dict[str, Any] = field(default_factory=dict)
    current_node: str = "entrypoint"
    error: Optional[str] = None
    step_count: int = 0
    max_steps: int = 25

    def add_message(self, role: str, content: str, **metadata: Any) -> SwarmMessage:
        msg = SwarmMessage(role=role, content=content, metadata=metadata)
        self.history.append(msg)
        return msg


class AgentProtocol(Protocol):
    """Protocol defining the interface for Swarm Agents."""
    name: str

    async def execute(self, state: SwarmState) -> SwarmState:
        ...


class BaseSwarmAgent:
    """Configurable agent unit conforming to Kyegomez/Swarms specifications."""

    def __init__(
        self,
        name: str,
        role_description: str,
        handler: Optional[Callable[[SwarmState], SwarmState]] = None,
    ) -> None:
        self.name: str = name
        self.role_description: str = role_description
        self._handler: Optional[Callable[[SwarmState], SwarmState]] = handler

    async def execute(self, state: SwarmState) -> SwarmState:
        logger.info(f"Agent [{self.name}] executing on task: {state.task_id}")
        state.step_count += 1
        if state.step_count > state.max_steps:
            state.status = TaskStatus.FAILED
            state.error = f"Agent {self.name} exceeded max step execution depth: {state.max_steps}"
            return state

        try:
            if self._handler:
                if asyncio.iscoroutinefunction(self._handler):
                    state = await self._handler(state)
                else:
                    loop = asyncio.get_running_loop()
                    state = await loop.run_in_executor(None, self._handler, state)
            else:
                # Default processing behaviour
                output_content = (
                    f"Agent {self.name} processed task data for role: {self.role_description}"
                )
                state.add_message(role=self.name, content=output_content)
                state.context[f"{self.name}_output"] = output_content
                state.status = TaskStatus.RUNNING
        except Exception as exc:
            logger.exception(f"Agent [{self.name}] encountered an unhandled exception")
            state.status = TaskStatus.FAILED
            state.error = f"{type(exc).__name__}: {str(exc)}"

        return state


class SwarmRouter:
    """LangGraph-inspired stateful graph router for concurrent agent networks."""

    def __init__(self, name: str = "EnterpriseSwarmRouter") -> None:
        self.name: str = name
        self.nodes: Dict[str, AgentProtocol] = {}
        self.edges: Dict[str, List[str]] = {}
        self.conditional_edges: Dict[
            str, Callable[[SwarmState], Union[str, Sequence[str]]]
        ] = {}

    def add_node(self, name: str, agent: AgentProtocol) -> None:
        """Register an agent node in the swarm mesh."""
        self.nodes[name] = agent

    def add_edge(self, from_node: str, to_node: str) -> None:
        """Direct static routing edge between nodes."""
        if from_node not in self.edges:
            self.edges[from_node] = []
        self.edges[from_node].append(to_node)

    def add_conditional_edge(
        self,
        from_node: str,
        condition: Callable[[SwarmState], Union[str, Sequence[str]]],
    ) -> None:
        """Add dynamic routing condition based on current graph state."""
        self.conditional_edges[from_node] = condition

    async def route_step(self, current_node: str, state: SwarmState) -> List[str]:
        """Determine next hop(s) in execution topology."""
        next_hops: List[str] = []

        if current_node in self.conditional_edges:
            condition_fn = self.conditional_edges[current_node]
            decision = condition_fn(state)
            if isinstance(decision, str):
                next_hops = [decision]
            elif isinstance(decision, (list, tuple)):
                next_hops = list(decision)
        elif current_node in self.edges:
            next_hops = self.edges[current_node]

        return [h for h in next_hops if h in self.nodes or h == "__end__"]

    async def execute_task(self, initial_state: SwarmState) -> SwarmState:
        """Execute single state sequence through the swarm graph."""
        state = initial_state
        state.status = TaskStatus.RUNNING
        current_node_name = state.current_node

        while current_node_name != "__end__" and state.status == TaskStatus.RUNNING:
            if current_node_name not in self.nodes:
                state.status = TaskStatus.FAILED
                state.error = f"Node '{current_node_name}' not registered in SwarmRouter"
                break

            agent = self.nodes[current_node_name]
            state = await agent.execute(state)

            if state.status == TaskStatus.FAILED:
                logger.error(f"Task {state.task_id} failed at node {current_node_name}: {state.error}")
                break

            # Route to next target node(s)
            next_hops = await self.route_step(current_node_name, state)
            if not next_hops or "__end__" in next_hops:
                state.current_node = "__end__"
                state.status = TaskStatus.COMPLETED
                break

            # If single hop, sequential transition
            if len(next_hops) == 1:
                current_node_name = next_hops[0]
                state.current_node = current_node_name
            else:
                # Concurrent split: fan-out across multiple child agents
                logger.info(f"Fanning out concurrently to nodes: {next_hops}")
                tasks = [
                    self.nodes[hop].execute(state)
                    for hop in next_hops
                    if hop in self.nodes
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for res in results:
                    if isinstance(res, Exception):
                        state.status = TaskStatus.FAILED
                        state.error = f"Sub-node error: {str(res)}"
                        return state
                state.current_node = "__end__"
                state.status = TaskStatus.COMPLETED
                break

        return state


async def run_swarm_batch(
    tasks: Sequence[Dict[str, Any]],
    router: Optional[SwarmRouter] = None,
    concurrency_limit: int = 5,
) -> List[Dict[str, Any]]:
    """
    Exported enterprise batch runner.
    Concurrently processes a collection of task payloads through the SwarmRouter
    with semaphore rate-limiting, error isolation, and metrics capture.
    """
    if router is None:
        # Construct default standard router pipeline
        router = SwarmRouter(name="DefaultEnterpriseRouter")
        
        analyzer = BaseSwarmAgent(
            name="analyzer",
            role_description="Analyze incoming payload structure and triage requirements",
        )
        executor = BaseSwarmAgent(
            name="executor",
            role_description="Execute domain business operations based on payload",
        )
        validator = BaseSwarmAgent(
            name="validator",
            role_description="Verify response integrity and schema adherence",
        )
        
        router.add_node("entrypoint", analyzer)
        router.add_node("executor", executor)
        router.add_node("validator", validator)
        
        router.add_edge("entrypoint", "executor")
        router.add_edge("executor", "validator")
        router.add_edge("validator", "__end__")

    semaphore = asyncio.Semaphore(concurrency_limit)

    async def _process_task(payload: Dict[str, Any]) -> Dict[str, Any]:
        task_id = str(payload.get("task_id", uuid.uuid4()))
        state = SwarmState(task_id=task_id, input_payload=payload)
        
        start_time = datetime.now(timezone.utc)
        async with semaphore:
            try:
                result_state = await router.execute_task(state)
                duration_ms = (
                    datetime.now(timezone.utc) - start_time
                ).total_seconds() * 1000.0
                return {
                    "task_id": result_state.task_id,
                    "status": result_state.status.value,
                    "error": result_state.error,
                    "duration_ms": round(duration_ms, 2),
                    "context": result_state.context,
                    "messages_count": len(result_state.history),
                }
            except Exception as e:
                logger.exception(f"Unhandled error in batch runner for task {task_id}")
                return {
                    "task_id": task_id,
                    "status": TaskStatus.FAILED.value,
                    "error": f"Batch runner caught exception: {str(e)}",
                    "duration_ms": 0.0,
                    "context": {},
                    "messages_count": 0,
                }

    batch_tasks = [_process_task(item) for item in tasks]
    return await asyncio.gather(*batch_tasks)


def run_swarm_batch_sync(
    tasks: Sequence[Dict[str, Any]],
    router: Optional[SwarmRouter] = None,
    concurrency_limit: int = 5,
) -> List[Dict[str, Any]]:
    """Synchronous wrapper for run_swarm_batch."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(
                asyncio.run,
                run_swarm_batch(tasks, router=router, concurrency_limit=concurrency_limit),
            )
            return future.result()
    else:
        return asyncio.run(
            run_swarm_batch(tasks, router=router, concurrency_limit=concurrency_limit)
        )
