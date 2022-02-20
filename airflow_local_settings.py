# https://airflow.apache.org/docs/apache-airflow/stable/concepts/cluster-policies.html
# idont get this gahbage maybe i have it in the wrong file path/????????????????

# from airflow.models import DAG
# from airflow.models.baseoperator import BaseOperator
# from airflow.exceptions import AirflowClusterPolicyViolation

# def dag_policy(dag: DAG):
#     """Ensure that DAG has at least one tag"""
#     if not dag.tags:
#         raise AirflowClusterPolicyViolation(
#             f"DAG {dag.dag_id} has no tags. At least one tag required. File path: {dag.fileloc}"
#         )

# def task_must_have_owners(task: BaseOperator):
#     if not task.owner or task.owner.lower() == conf.get('operators', 'default_owner'):
#         raise AirflowClusterPolicyViolation(
#             f'''Task must have non-None non-default owner. Current value: {task.owner}'''
#         )
