### Preprocessing Auditor README

This reactor validates preprocessing job outputs and handles resubmission of these jobs, if necessary. It accepts an Abaco JSON message like:

```json
{
    "mpjId": "10779ebc-db12-5eac-9fcf-03deb2cb0c70",
    "tapis_jobId": "${JOB_ID}"
}
```

Similarly, the message can be passed Tapis job data by adding the reactor's webhook URI (`Reactor().create_webhook(maxuses=10)`) to a Tapis job definition:

```python
{
    "appId": "my-app-id",
    "inputs": {},
    ...
    "notifications": [
        {
            "event": "FINISHED",
            "persistent": "False",
            "url": ManagedPipelineJob().callback + "&status=${JOB_STATUS}"
        }, {
            "event": "FINISHED",
            "persistent": "False",
            "url": Reactor().create_webhook() + "&tapis_jobId=${JOB_ID}&mpjId=10779ebc-db12-5eac-9fcf-03deb2cb0c70"
        }
    ]
}
```

Validation is successful if output files `{archivePath}/*R1*.fastq.gz` and `{archivePath}/*R2*.fastq.gz` exist and file size is >= `settings.options.min_fastq_mb` megabytes for each. WIP: update the ManagedPipelineJob to status=VALIDATED by messaging the PipelineJobManager.

Failing validation will trigger a Tapis job resubmission, where the Tapis job is resubmitted with *exactly* the same job definition. Note that job notifications will be the same, so older Tapis jobs will not message this reactor when the resubmitted job finishes. WIP: Reactor checks if its webhook URI exists in the Tapis job notifications, and adds itself if they do not exist, allowing older Tapis jobs to iterate.

Resubmission will fail if either of the following are true:
- Greater than or equal to `settings.options.max_retries` Tapis jobs messaged ManagedPipelineJob `mpjId` with `status=[FINISHED | FAILED]`
- Greater than or equal to `settings.options.max_retries` files are found matching `{archivePath}/*.err`
