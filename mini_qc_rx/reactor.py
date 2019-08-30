from reactors.utils import Reactor
import json


def submit_agave_job(job_template):
    try:
        job_id = r.client.jobs.submit(body=job_template)['id']
    except Exception as e:
        r.logger.error("Error submitting job: {}".format(e))
        if e.response.content is not None:
            print(e.response.content)
        else:
            print(e)
    else:
        r.logger.info("Submitted job {}".format(job_id))
    finally:
        print(json.dumps(job_template, indent=4))


def main():
    """Main function"""
    global r
    r = Reactor()
    r.logger.info(r.settings.mini_qc_job)
    job_template = r.settings.mini_qc_job.copy()
    job_template['archivePath'] = "archive/jobs/${JOB_NAME}-${JOB_ID}"
    job_template['inputs'] = {}
    submit_agave_job(job_template)

    # callback = ""
    # job_template.notifs = [
    #     {
    #         'event': 'RUNNING',
    #         "persistent": True,
    #         'url': callback + '&status=${JOB_STATUS}'
    #     }, {
    #         'event': 'FAILED',
    #         "persistent": False,
    #         'url': callback + '&status=${JOB_STATUS}'
    #     }, {
    #         'event': 'FINISHED',
    #         "persistent": False,
    #         'url': callback + '&status=FINISHED'
    #     }]

if __name__ == '__main__':
    main()
