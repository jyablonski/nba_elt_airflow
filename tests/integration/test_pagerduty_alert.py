import unittest
from unittest.mock import patch, MagicMock

from include.utils import jacobs_pagerduty_notification


class TestJacobsPagerdutyNotification(unittest.TestCase):
    @patch("include.utils.send_pagerduty_notification")
    def test_jacobs_pagerduty_notification(self, mock_send_notification):
        mock_notification_instance = MagicMock()
        mock_send_notification.return_value = mock_notification_instance
        severity = "critical"
        class_type = "Test Pipeline"
        conn_id = "test_conn_id"

        context = {
            "task_instance": MagicMock(
                dag_id="test_dag", task_id="test_task", log_url="http://test_log_url"
            ),
            "exception": Exception("Test Exception"),
        }

        notification_function = jacobs_pagerduty_notification(
            pagerduty_events_conn_id=conn_id,
            severity=severity,
            class_type=class_type,
        )
        notification_function(context)

        mock_send_notification.assert_called_once_with(
            pagerduty_events_conn_id=conn_id,
            summary="DAG test_dag Failure",
            severity=severity,
            source="airflow dag_id: test_dag",
            dedup_key="test_dag-test_task",
            group="test_dag",
            component="airflow",
            class_type=class_type,
        )

        mock_notification_instance.notify.assert_called_once_with(context=context)
