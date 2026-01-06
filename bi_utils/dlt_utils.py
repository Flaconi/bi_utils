import os
from typing import Optional
from dlt.common.pipeline import LoadInfo
from dlt.common.runtime.slack import send_slack_message
from slack_sdk import WebClient


def notify_schema_changes(load_info: LoadInfo) -> None:
    """
    Iterates through load_info to find schema updates and notifies via Slack
    
    Args:
        load_info: The LoadInfo object returned by pipeline.run()
    """

    SLACK_APP_TOKEN =  os.environ.get('SLACK_APP_TOKEN')
    SLACK_CHANNEL = 'C029847G9S5' # bi-dev-notifications
    client = WebClient(token=SLACK_APP_TOKEN)    

    pipeline_name = load_info.pipeline.pipeline_name
    # Iterate over each package in the load_info object
    for package in load_info.load_packages:
        # Iterate over each table in the schema_update of the current package
        for table_name, table in package.schema_update.items():
            # Iterate over each column in the current table
            for column_name, column in table["columns"].items():
                # Construct the message
                message_lines = [
                    f"*Warning*, schema-change detected in dlthub-pipeline: `{pipeline_name}`\n"
                    f"Table updated: `{table_name}`\n",
                    f"Column changed: `{column_name}`",
                    f"Data type: `{column['data_type']}`"
                ]
                                
                message = "\n".join(message_lines)

                client.chat_postMessage(
                    channel=SLACK_CHANNEL,
                    text=message
                )