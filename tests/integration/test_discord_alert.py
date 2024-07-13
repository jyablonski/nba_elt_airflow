import unittest
from unittest.mock import patch, MagicMock

# Assuming your functions are in a module named 'include.utils'
from include.utils import discord_owner_ping, jacobs_discord_alert


class TestJacobsDiscordAlert(unittest.TestCase):
    def test_discord_owner_ping(self):
        # Test with a known owner
        self.assertEqual(
            discord_owner_ping("jacob"), "<@95723063835885568> <@995779012347572334>"
        )

        # Test with an unknown owner
        self.assertEqual(discord_owner_ping("unknown"), "unknown")

    @patch("include.utils.DiscordWebhookOperator")
    def test_jacobs_discord_alert(self, mock_discord_operator):
        # Set up the mock return value
        mock_discord_instance = MagicMock()
        mock_discord_operator.return_value = mock_discord_instance

        # Mock the context dictionary
        context = {
            "task_instance": MagicMock(
                dag_id="test_dag",
                task_id="test_task",
                log_url="http://test_log_url",
                task=MagicMock(owner="jacob"),
            ),
            "exception": Exception("Test Exception"),
            "execution_date": "2023-07-13T00:00:00",
        }

        # Call the function
        jacobs_discord_alert(context)

        # Define the expected message
        expected_message = """
            :red_circle: Task Failed. 
            *Exception*: Test Exception
            *Task*: test_task
            *Dag*: test_dag 
            *Owner*: <@95723063835885568> <@995779012347572334>
            *Execution Time*: 2023-07-13T00:00:00  
            *Log Url*: http://test_log_url 
            """

        # Verify DiscordWebhookOperator was called with correct parameters
        mock_discord_operator.assert_called_once_with(
            task_id="discord_failure_callback_test",
            http_conn_id="discord",
            message=expected_message,
        )

        # Verify the execute method was called
        mock_discord_instance.execute.assert_called_once_with(context=context)
