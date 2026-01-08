from src.pubtator_utils.db_handler.db import Session
from src.pubtator_utils.db_handler.alembic_models.workflow import Workflow
from src.pubtator_utils.logs_handler.logger import SingletonLogger
import argparse
from datetime import datetime, timezone
import sys

logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


class WorkflowData:
    def insert_workflow_id(self, workflow_id):
        workflow =  Workflow(
                    workflow_id=workflow_id
        )
        with Session() as session:
            session.add(workflow)
            session.commit()
    
    def parse_to_utc(self, value: str) -> datetime:
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            raise ValueError(
                f"Invalid timestamp format: {value}. "
            )

        # If no timezone info → assume local and convert to UTC
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        return dt.astimezone(timezone.utc)

    def update_workflow_data(self, workflow_id, field, value):
        if not hasattr(Workflow, field):
            raise ValueError(f"Invalid field: {field}")
        if '_ts' in field:
            value = self.parse_to_utc(value=value)
        with Session() as session:
            session.query(Workflow).filter_by(
                workflow_id=workflow_id
            ).update({field: value})
            session.commit()
    

def main():
    parser = argparse.ArgumentParser(
        description="Insert and update workflow table",
        epilog=(
            "python workflow_data_cli.py insert "
            "--workflow_id 123"
        ),
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # Common args
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--workflow_id")

    # Insert command
    insert_parser = subparsers.add_parser(
        "insert",
        parents=[common],
        help="Insert workflow id",
    )

    # Update command
    update_parser = subparsers.add_parser(
        "update",
        parents=[common],
        help="Update any workflow field",
    )
    update_parser.add_argument("--field", required=True)
    update_parser.add_argument("--value", required=True)

    args = parser.parse_args()
    
    if not args.workflow_id:
        logger.error(
            "Workflow ID is not found. Nothing to update. Exiting.",
        )
        sys.exit(1)

    wf = WorkflowData()

    if args.command == "insert":
        wf.insert_workflow_id(workflow_id=args.workflow_id)

    elif args.command == "update":
        wf.update_workflow_data(
            workflow_id=args.workflow_id,
            field=args.field,
            value=args.value,
        )


if __name__ == "__main__":
    main()
