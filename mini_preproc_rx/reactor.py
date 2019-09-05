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
    DEBUG_MODE = getattr(r.settings, "debug", False)
    job_template = r.settings.mini_preproc_job.copy()
    downstream_callback = getattr(r.settings, "downstream_callback", "")

    if DEBUG_MODE:
        r.logger.debug("Running in debug mode, skipping downstream webhooks")
    else:
        payload_encode = {
            'flagstat_remote_fp': job_template['inputs']['fs_remote_fp'],
        }
        job_template['notifications'] = [{
            'event': 'FINISHED',
            "persistent": False,
            'url': downstream_callback + "&status=${JOB_STATUS}&" + urlencode(payload_encode)
        }]
    submit_agave_job(job_template)


if __name__ == '__main__':
    main()
