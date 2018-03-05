# DEWE.v4

DEWE.v4 is a platform providing Workflow-Exexution-as-a-Service. It includes three major components:

(1) Web Frontend 

- User login via OAuth using their accounts from Google, Facebook, LinkedIn, and Weibo.

- User defines a workflow by providing read and write access to their S3 bucket (using bucket policy). 

- Generate a visualization of the DAG.

- User requests to start workflow execution, and view the progress in a dash board. 

(2) Workflow Scheduling Engine

- Handles the scheduling of multiple workflows concurrently.

(3) Local Job Handler

- Executes workflow jobs by pulling a job queue and sends ACK to an ACK queue.

(4) Lambda Job Handler

- Executes workflows jobs when invoked and sends ACK to an ACK queue.
