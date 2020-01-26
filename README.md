# CIED PrusaSlicer

This is a tiny application which reads from rabbitmq, 
download the parameter files, run prusa-slicer with parameters, 
upload the rendered gcode to S3, and write back the execution to 
an another queue.

In the long run this app is meant to be a member of a bigger ecosystem.

Env vars:

| name               | desc     |
| ---                | ------   |
| AMQP_URL           | Amqp server url like `amqp://guest:guest@localhost:5672/`|
| AMQP_RECQ          | The name of the job receiver queue. |
| AMQP_JOBDONEQ      | The name of the job finished queue. |
| S3_REGION          | S3 region. |
| S3_BUCKET          | S3 bucket name. |
| S3_ENDPOINT        | For amazon should be sth like `https://s3.dualstack.${region}.amazonaws.com` |
| S3_ACCESSKEYID     | S3 access key id. |
| S3_SECRETACCESSKEY | S3 access key secret. |
| S3_DISABLESSL      | For local testing you probably want this to `true` for prod like envs `false` default `true`. |
