from __future__ import annotations
import asyncio
from datetime import datetime
from temporalio.client import Client
from app.workflows import OrdersPipelineWorkflow

async def main() -> None:
    client = await Client.connect("localhost:7233")

    workflow_id = f"orders-pipeline-{datetime.now().strftime('%Y%m%d-%H%M%S')}"

    result = await client.execute_workflow(
        OrdersPipelineWorkflow.run,
        id=workflow_id,
        task_queue="orders-task-queue",
    )

    print("Workflow completed successfully.")
    print(result)

if __name__ == "__main__":
    asyncio.run(main())
