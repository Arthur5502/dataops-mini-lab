from __future__ import annotations

import asyncio
from temporalio.client import Client
from app.workflows import OrdersPipelineWorkflow


async def main() -> None:
    client = await Client.connect("localhost:7233")

    result = await client.execute_workflow(
        OrdersPipelineWorkflow.run,
        id="orders-pipeline-workflow-run",
        task_queue="orders-task-queue",
    )

    print("Workflow completed successfully.")
    print(result)


if __name__ == "__main__":
    asyncio.run(main())
