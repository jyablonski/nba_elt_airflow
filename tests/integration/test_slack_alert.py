import unittest
from unittest.mock import patch, MagicMock

# Assuming your functions are in a module named 'include.utils'
from include.utils import jacobs_slack_alert


class TestJacobsSlackAlert(unittest.TestCase):
    @patch("include.utils.SlackWebhookOperator")
    def test_jacobs_slack_alert(self, mock_slack_operator):
        # Set up the mock return value
        mock_slack_instance = MagicMock()
        mock_slack_operator.return_value = mock_slack_instance
        slack_conn_id = "test_slack_conn"

        # Mock the context dictionary
        context = {
            "task_instance": MagicMock(
                dag_id="test_dag",
                task_id="test_task",
                log_url="http://test_log_url",
                task=MagicMock(owner="test_owner"),
            ),
            "exception": Exception("Test Exception"),
            "execution_date": "2023-07-13T00:00:00",
        }

        # Call the function
        alert_function = jacobs_slack_alert(slack_webhook_conn_id=slack_conn_id)
        alert_function(context)

        # Define the expected message
        expected_message = """
                :red_circle: Task Failed. 
            *Exception*: Test Exception
            *Task*: test_task
            *Dag*: test_dag 
            *Owner*: test_owner
            *Execution Time*: 2023-07-13T00:00:00  
            *Log Url*: http://test_log_url 
                """

        # Verify SlackWebhookOperator was called with correct parameters
        mock_slack_operator.assert_called_once_with(
            task_id="slack_task",
            slack_webhook_conn_id=slack_conn_id,
            message=expected_message,
        )

        # Verify the execute method was called
        mock_slack_instance.execute.assert_called_once_with(context=context)
