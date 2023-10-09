""" Python Test DAG"""
from datetime import datetime

from airflow.decorators import dag, task

from include.aws_utils import write_to_s3
from include.utils import get_schedule_interval, jacobs_slack_alert
from include.rest_api_scrape.utils import scrape_endpoint

api_endpoint = "https://api.jyablonski.dev"
api_scrape_bucket = "jacobsbucket97-dev"

# send both an email alert + a slack alert to specified channel on any task failure
default_args = {
    "owner": "jacob",
    "depends_on_past": False,
    "email": ["jyablonski9@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "on_failure_callback": jacobs_slack_alert,
}


@dag(
    schedule=get_schedule_interval(None),
    start_date=datetime(2023, 7, 1),
    catchup=False,
    default_args=default_args,
    tags=["nba_elt_project"],
)
def nba_rest_api_scrape_dag():
    @task()
    def scrape_game_types(
        base_api_endpoint: str, task_endpoint: str, bucket: str, **context: dict
    ):
        date = context["data_interval_end"].strftime("%Y-%m-%d")
        file_path = f"rest_api_scrapes/{task_endpoint}/{task_endpoint}-{date}"

        game_types = scrape_endpoint(
            endpoint=task_endpoint,
            context=context,
            base_api_endpoint=base_api_endpoint,
        )

        write_to_s3(
            dataframe=game_types,
            s3_bucket=bucket,
            s3_path=file_path,
        )

        pass

    @task()
    def scrape_injuries(
        base_api_endpoint: str, task_endpoint: str, bucket: str, **context: dict
    ):
        date = context["data_interval_end"].strftime("%Y-%m-%d")
        file_path = f"rest_api_scrapes/{task_endpoint}/{task_endpoint}-{date}"

        game_types = scrape_endpoint(
            endpoint=task_endpoint,
            context=context,
            base_api_endpoint=base_api_endpoint,
        )

        write_to_s3(
            dataframe=game_types,
            s3_bucket=bucket,
            s3_path=file_path,
        )

        pass

    @task()
    def scrape_predictions(
        base_api_endpoint: str, task_endpoint: str, bucket: str, **context: dict
    ):
        date = context["data_interval_end"].strftime("%Y-%m-%d")
        file_path = f"rest_api_scrapes/{task_endpoint}/{task_endpoint}-{date}"

        game_types = scrape_endpoint(
            endpoint=task_endpoint,
            context=context,
            base_api_endpoint=base_api_endpoint,
        )

        write_to_s3(
            dataframe=game_types,
            s3_bucket=bucket,
            s3_path=file_path,
        )

        pass

    @task()
    def scrape_reddit_comments(
        base_api_endpoint: str, task_endpoint: str, bucket: str, **context: dict
    ):
        date = context["data_interval_end"].strftime("%Y-%m-%d")
        file_path = f"rest_api_scrapes/{task_endpoint}/{task_endpoint}-{date}"

        game_types = scrape_endpoint(
            endpoint=task_endpoint,
            context=context,
            base_api_endpoint=base_api_endpoint,
        )

        write_to_s3(
            dataframe=game_types,
            s3_bucket=bucket,
            s3_path=file_path,
        )

        pass

    @task()
    def scrape_schedule(
        base_api_endpoint: str, task_endpoint: str, bucket: str, **context: dict
    ):
        date = context["data_interval_end"].strftime("%Y-%m-%d")
        file_path = f"rest_api_scrapes/{task_endpoint}/{task_endpoint}-{date}"

        game_types = scrape_endpoint(
            endpoint=task_endpoint,
            context=context,
            base_api_endpoint=base_api_endpoint,
        )

        write_to_s3(
            dataframe=game_types,
            s3_bucket=bucket,
            s3_path=file_path,
        )

        pass

    @task()
    def scrape_player_stats(
        base_api_endpoint: str, task_endpoint: str, bucket: str, **context: dict
    ):
        date = context["data_interval_end"].strftime("%Y-%m-%d")
        file_path = f"rest_api_scrapes/{task_endpoint}/{task_endpoint}-{date}"

        game_types = scrape_endpoint(
            endpoint=task_endpoint,
            context=context,
            base_api_endpoint=base_api_endpoint,
        )

        write_to_s3(
            dataframe=game_types,
            s3_bucket=bucket,
            s3_path=file_path,
        )

        pass

    @task()
    def scrape_standings(
        base_api_endpoint: str, task_endpoint: str, bucket: str, **context: dict
    ):
        date = context["data_interval_end"].strftime("%Y-%m-%d")
        file_path = f"rest_api_scrapes/{task_endpoint}/{task_endpoint}-{date}"

        game_types = scrape_endpoint(
            endpoint=task_endpoint,
            context=context,
            base_api_endpoint=base_api_endpoint,
        )

        write_to_s3(
            dataframe=game_types,
            s3_bucket=bucket,
            s3_path=file_path,
        )

        pass

    @task()
    def scrape_team_ratings(
        base_api_endpoint: str, task_endpoint: str, bucket: str, **context: dict
    ):
        date = context["data_interval_end"].strftime("%Y-%m-%d")
        file_path = f"rest_api_scrapes/{task_endpoint}/{task_endpoint}-{date}"

        game_types = scrape_endpoint(
            endpoint=task_endpoint,
            context=context,
            base_api_endpoint=base_api_endpoint,
        )

        write_to_s3(
            dataframe=game_types,
            s3_bucket=bucket,
            s3_path=file_path,
        )

        pass

    @task()
    def scrape_transactions(
        base_api_endpoint: str, task_endpoint: str, bucket: str, **context: dict
    ):
        date = context["data_interval_end"].strftime("%Y-%m-%d")
        file_path = f"rest_api_scrapes/{task_endpoint}/{task_endpoint}-{date}"

        game_types = scrape_endpoint(
            endpoint=task_endpoint,
            context=context,
            base_api_endpoint=base_api_endpoint,
        )

        write_to_s3(
            dataframe=game_types,
            s3_bucket=bucket,
            s3_path=file_path,
        )

        pass

    @task()
    def scrape_transactions(
        base_api_endpoint: str, task_endpoint: str, bucket: str, **context: dict
    ):
        date = context["data_interval_end"].strftime("%Y-%m-%d")
        file_path = f"rest_api_scrapes/{task_endpoint}/{task_endpoint}-{date}"

        game_types = scrape_endpoint(
            endpoint=task_endpoint,
            context=context,
            base_api_endpoint=base_api_endpoint,
        )

        write_to_s3(
            dataframe=game_types,
            s3_bucket=bucket,
            s3_path=file_path,
        )

        pass

    @task()
    def scrape_twitter_comments(
        base_api_endpoint: str, task_endpoint: str, bucket: str, **context: dict
    ):
        date = context["data_interval_end"].strftime("%Y-%m-%d")
        file_path = f"rest_api_scrapes/{task_endpoint}/{task_endpoint}-{date}"

        game_types = scrape_endpoint(
            endpoint=task_endpoint,
            context=context,
            base_api_endpoint=base_api_endpoint,
        )

        write_to_s3(
            dataframe=game_types,
            s3_bucket=bucket,
            s3_path=file_path,
        )

        pass

    [
        scrape_game_types(
            base_api_endpoint=api_endpoint,
            task_endpoint="game_types",
            bucket=api_scrape_bucket,
        ),
        scrape_injuries(
            base_api_endpoint=api_endpoint,
            task_endpoint="injuries",
            bucket=api_scrape_bucket,
        )
        >> scrape_predictions(
            base_api_endpoint=api_endpoint,
            task_endpoint="predictions",
            bucket=api_scrape_bucket,
        ),
        scrape_reddit_comments(
            base_api_endpoint=api_endpoint,
            task_endpoint="reddit_comments",
            bucket=api_scrape_bucket,
        )
        >> scrape_schedule(
            base_api_endpoint=api_endpoint,
            task_endpoint="schedule",
            bucket=api_scrape_bucket,
        ),
        scrape_player_stats(
            base_api_endpoint=api_endpoint,
            task_endpoint="scorers",
            bucket=api_scrape_bucket,
        )
        >> scrape_standings(
            base_api_endpoint=api_endpoint,
            task_endpoint="standings",
            bucket=api_scrape_bucket,
        ),
        scrape_team_ratings(
            base_api_endpoint=api_endpoint,
            task_endpoint="team_ratings",
            bucket=api_scrape_bucket,
        )
        >> scrape_transactions(
            base_api_endpoint=api_endpoint,
            task_endpoint="transactions",
            bucket=api_scrape_bucket,
        ),
    ] >> scrape_twitter_comments(
        base_api_endpoint=api_endpoint,
        task_endpoint="twitter_comments",
        bucket=api_scrape_bucket,
    )


dag = nba_rest_api_scrape_dag()
