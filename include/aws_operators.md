# AWS Operator Notes
You can use AWS Batch + ECS Operators from Airflow to run containers you store in ECR Repositories on AWS compute resources.

## AWS ECS Fargate
![image](https://user-images.githubusercontent.com/16946556/201789268-08362297-660d-4002-a928-70316fbe67b2.png)

Fargate is the easiest way to set up running containers from Airflow.  
- No EC2 instances have to be provisioned or managed.  
- You simply store your docker image in an ECR Repo and then make an ECS Task Definition in an ECS Cluster.
- Memory + vCPU is configured in the task definition.
- Logs can be viewed in Airflow.
- Airflow Task will continue running until the ECS Operator reaches a terminal state; it doesn't automatically "complete" after it triggers the API operation to start your job.

```
overrides={
    "containerOverrides": [
        {
            "name": "jacobs_hello_world_container",  # change this to any of the task_definitons created in ecs
            "environment": [
                {
                    "name": "dag_run_ts",
                    "value": "{{ ts }}",
                },  # https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html
                {
                    "name": "dag_run_date",
                    "value": " {{ ds }}",
                }
            ],
        }
    ]
},
awslogs_group="/ecs/hello-world-test",
awslogs_stream_prefix="ecs/jacobs_hello_world_container",
```
- Can add in environment variables `dag_run_ts` and `dag_run_date`
- `awslogs_group` comes from the AWS Cloudwatch Logs name.
- `awslogs_stream_prefix` starts with `ecs/` and is followed by the name of the container you're passing in the `overrides` parameter (here it is `jacobs_hello_world_container`)

Container Logs get pumped out from Cloudwatch into the Airflow UI.
![image](https://user-images.githubusercontent.com/16946556/201789105-f0b1aa7a-ba39-4923-945b-6c33f4bddbc4.png)

## AWS Batch
Can also use AWS Batch to run containers if you've already set all of that up.
- Job Queue + Job Definition have to be configured in AWS Batch.
- Airflow simply just calls the `SubmitJob` API Operation and waits for the container to reach a terminal state.

IAM Permissions needed.
```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "batch:SubmitJob",
                "batch:DescribeJobs"
            ],
            "Resource": "*"
        }
    ]
}
```

![image](https://user-images.githubusercontent.com/16946556/201793269-74629b5b-b970-40b4-a4c9-75867ee0a857.png)
I didn't figure out how to stream the logs of the container to Airflow UI; the container logs are available in Cloudwatch though.
- Cloudwatch Logs sent to `/aws/batch/job` with a stream called `jacobs-batch-definition/default/8136084052e943f6b38ea069c75808c6`
