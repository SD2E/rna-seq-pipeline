from reactors.utils import Reactor
import json


def submit_agave_job(ag, job_template):
    try:
        job_id = ag.jobs.submit(body=job_template)['id']
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
    r = Reactor()
    r.logger.info(r.settings.mini_qc_job)
    job_template = r.settings.mini_qc_job.copy()
    return
    job_template = {}
    submit_agave_job(r.client, job_template)


if __name__ == '__main__':
    main()
