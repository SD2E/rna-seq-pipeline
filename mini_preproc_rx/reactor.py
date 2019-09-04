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
    #r.client.actors.sendMessage(actorId=target_actor, body={'message': 'a message'})
    #callback = "https://ente1285z7vq.x.pipedream.net/"
    job_template['notifications'] = [
        # {
        #     'event': 'RUNNING',
        #     "persistent": True,
        #     'url': callback + '&status=${JOB_STATUS}'
        # }, {
        #     'event': 'FAILED',
        #     "persistent": False,
        #     'url': callback + '&status=${JOB_STATUS}'
        # },
        {
            'event': 'RUNNING',
            "persistent": False,
            'url': callback  + '&status=${JOB_STATUS}'
        }]

    submit_agave_job(job_template)


if __name__ == '__main__':
    main()
