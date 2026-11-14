import unittest
import asyncio
from swarms_engine import (
    TaskStatus,
    SwarmMessage,
    SwarmState,
    BaseSwarmAgent,
    SwarmRouter,
    run_swarm_batch,
    run_swarm_batch_sync,
)

class TestSwarmsEngine(unittest.TestCase):
    def test_state_message_creation(self):
        state = SwarmState(task_id="t-1", input_payload={"query": "test"})
        msg = state.add_message(role="system", content="init", meta_key="value")
        self.assertEqual(len(state.history), 1)
        self.assertEqual(state.history[0].content, "init")
        self.assertEqual(state.history[0].metadata.get("meta_key"), "value")

    def test_router_sequential_execution(self):
        router = SwarmRouter("TestSeqRouter")
        agent_a = BaseSwarmAgent("agent_a", "Step A")
        agent_b = BaseSwarmAgent("agent_b", "Step B")
        router.add_node("entrypoint", agent_a)
        router.add_node("agent_b", agent_b)
        router.add_edge("entrypoint", "agent_b")
        router.add_edge("agent_b", "__end__")

        state = SwarmState(task_id="t-seq", input_payload={"data": 123})
        res = asyncio.run(router.execute_task(state))
        self.assertEqual(res.status, TaskStatus.COMPLETED)
        self.assertEqual(res.current_node, "__end__")
        self.assertIn("agent_a_output", res.context)
        self.assertIn("agent_b_output", res.context)

    def test_router_conditional_branching(self):
        router = SwarmRouter("TestCondRouter")
        entry = BaseSwarmAgent("entrypoint", "Entry")
        agent_true = BaseSwarmAgent("branch_true", "Branch True")
        agent_false = BaseSwarmAgent("branch_false", "Branch False")

        router.add_node("entrypoint", entry)
        router.add_node("branch_true", agent_true)
        router.add_node("branch_false", agent_false)

        def condition(state: SwarmState):
            if state.input_payload.get("condition") is True:
                return "branch_true"
            return "branch_false"

        router.add_conditional_edge("entrypoint", condition)
        router.add_edge("branch_true", "__end__")
        router.add_edge("branch_false", "__end__")

        state_true = SwarmState(task_id="t-cond-1", input_payload={"condition": True})
        res_true = asyncio.run(router.execute_task(state_true))
        self.assertEqual(res_true.status, TaskStatus.COMPLETED)
        self.assertIn("branch_true_output", res_true.context)
        self.assertNotIn("branch_false_output", res_true.context)

        state_false = SwarmState(task_id="t-cond-2", input_payload={"condition": False})
        res_false = asyncio.run(router.execute_task(state_false))
        self.assertEqual(res_false.status, TaskStatus.COMPLETED)
        self.assertIn("branch_false_output", res_false.context)
        self.assertNotIn("branch_true_output", res_false.context)

    def test_router_concurrent_fan_out(self):
        router = SwarmRouter("TestFanOut")
        entry = BaseSwarmAgent("entrypoint", "Entry")
        worker1 = BaseSwarmAgent("worker1", "Worker 1")
        worker2 = BaseSwarmAgent("worker2", "Worker 2")

        router.add_node("entrypoint", entry)
        router.add_node("worker1", worker1)
        router.add_node("worker2", worker2)

        router.add_conditional_edge("entrypoint", lambda state: ["worker1", "worker2"])

        state = SwarmState(task_id="t-fanout", input_payload={})
        res = asyncio.run(router.execute_task(state))
        self.assertEqual(res.status, TaskStatus.COMPLETED)
        self.assertEqual(res.current_node, "__end__")
        self.assertIn("worker1_output", res.context)
        self.assertIn("worker2_output", res.context)

    def test_run_swarm_batch(self):
        tasks = [
            {"task_id": "batch-1", "val": 1},
            {"task_id": "batch-2", "val": 2},
            {"task_id": "batch-3", "val": 3},
        ]
        results = asyncio.run(run_swarm_batch(tasks, concurrency_limit=2))
        self.assertEqual(len(results), 3)
        for r in results:
            self.assertEqual(r["status"], TaskStatus.COMPLETED.value)
            self.assertIsNone(r["error"])
            self.assertGreater(r["messages_count"], 0)

    def test_run_swarm_batch_sync(self):
        tasks = [{"task_id": "sync-1", "val": 100}]
        results = run_swarm_batch_sync(tasks)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["status"], TaskStatus.COMPLETED.value)

    def test_error_handling_unregistered_node(self):
        router = SwarmRouter("FaultyRouter")
        state = SwarmState(task_id="t-fault", input_payload={})
        res = asyncio.run(router.execute_task(state))
        self.assertEqual(res.status, TaskStatus.FAILED)
        self.assertIn("not registered", res.error)

if __name__ == "__main__":
    unittest.main()
