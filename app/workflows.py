from __future__ import annotations

from datetime import timedelta
from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from app.activities import (
        aggregate_orders_activity,
        generate_csv_activity,
        load_csv_to_mongodb_activity,
    )


@workflow.defn
class OrdersPipelineWorkflow:
    @workflow.run
    async def run(self) -> dict:
        csv_path = await workflow.execute_activity(
            generate_csv_activity,
            start_to_close_timeout=timedelta(minutes=5),
        )

        load_result = await workflow.execute_activity(
            load_csv_to_mongodb_activity,
            csv_path,
            start_to_close_timeout=timedelta(minutes=10),
        )

        aggregation = await workflow.execute_activity(
            aggregate_orders_activity,
            start_to_close_timeout=timedelta(minutes=5),
        )

        return {
            "csv_path": csv_path,
            "load_result": load_result,
            "aggregation": aggregation,
        }
