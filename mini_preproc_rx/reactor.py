from reactors.utils import Reactor
import json
from urllib.parse import urlencode


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
        print("job_template:")
        print(json.dumps(job_template, indent=4))


def main():
    """Main function"""
    global r
    r = Reactor()
    r.logger.info(r.settings.mini_preproc_job)
    job_template = r.settings.mini_preproc_job.copy()
    job_template['parameters'] = {}

    callback = "https://api.sd2e.org/actors/v2/P1Vmggjk0Yg7D/" + \
    "messages?x-nonce=SD2E_pm77GL10QeEk3"
    payload_encode = {
        'flagstat_remote_fp': job_template['inputs']['dl_file']
    }
    job_template['notifications'] = [{
        'event': 'FINISHED',
        "persistent": False,
        'url': callback + "&status=${JOB_STATUS}&" + urlencode(payload_encode)
    }]

    submit_agave_job(job_template)


if __name__ == '__main__':
    main()
