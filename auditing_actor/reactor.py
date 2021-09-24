from reactors.utils import Reactor, agaveutils
from agavepy.agave import Agave
from datacatalog.managers.pipelinejobs import ReactorManagedPipelineJob as Job
from urllib.parse import unquote, urlsplit
from inspect import getsource as gs
from pprint import pprint as pp
from requests.exceptions import HTTPError
import json
import os
import requests
from glob import glob
import pymongo
import copy


def require_keys(dict, keys_list):
    """Return `dict`. r.on_failure unless keys_list is a subset of
    dict.keys().
    """
    k_list = keys_list.copy()
    for k in k_list:
        try:
            _ = dict[k]
        except KeyError as e:
            r.on_failure("Error pulling attr from dict", e)
    return dict


def _print_webhooks_to_self(tapis_jobId):
    url = "https://api.sd2e.org/notifications/v2?associatedUuid={}".format(tapis_jobId)
    token = agaveutils.get_api_token(r.client)
    headers = {"Authorization": 'Bearer {}'.format(token),
               "Content-Type": "application/json"}
    notifs_resp = requests.get(url, headers=headers)
    r.logger.debug("Current webhooks on tapis job={}".format(tapis_jobId))
    for notif in notifs_resp.json()['result']:
        r.logger.debug("{}: {}".format(notif['event'], notif['url']))
    return notifs_resp.json()['result']


def add_self_to_notifs(tapis_jobId):
    """docs
    """
    # get notifications associated with tapis_jobId
    token = agaveutils.get_api_token(r.client)
    headers = {"Authorization": 'Bearer {}'.format(token),
               "Content-Type": "application/json"}
    notifs_resp = requests.get("https://api.sd2e.org/notifications/v2" +
                               "?associatedUuid={}".format(tapis_jobId),
                               headers=headers)
    webhooks_to_self = notifs_resp.json()['result']
    # get actor URI for each self.nonce. Should only be one
    self_nonce_urls = set([n.get('_links', {}).get('actor', '')
                           for n in r.list_nonces()])
    _print_webhooks_to_self(tapis_jobId)
    # list of events that should message this reactor
    add_events = ['FINISHED', 'FAILED']
    # remove from add_events if that event triggers a webhook to self
    for nonce_url in self_nonce_urls:
        for notif in webhooks_to_self:
            if nonce_url in notif['url'] and notif['event'] in add_events:
                add_events.remove(notif['event'])
                r.logger.debug("existing webhook for event " +
                               "{}".format(notif['event']))
    # add remaining webhooks if they do not exist
    for new_evt in add_events:
        r.logger.debug("Creating webhook for event {}".format(new_evt))
        data_binary = str({
            'associatedUuid': tapis_jobId,
            'event': new_evt,
            'url': "{}&mpjId={}&tapis_jobId={}".format(
                r.create_webhook(maxuses=1), r.context.mpjId, "${JOB_ID}")
        }).replace("'", '"').encode()
        new_evt_resp = requests.post("https://api.sd2e.org/notifications/v2",
                                     data=data_binary, headers=headers)
        if new_evt_resp.json().get('status') != "success":
            r.on_failure("Error creating new notification with " +
                         "data_binary={}".format(data_binary), Exception())
    # debug prints
    _print_webhooks_to_self(tapis_jobId)


def jobs_resubmit(job_self_link, notif_add_self=True):
    """Re-submit an Agave/Tapis job. The job is assigned a new uuid,
    but is submitted with exactly the same definition, parameters, and
    inputs. In the context of this reactor, this means it will also have
    the same archivePath. Equivalent to CLI command jobs-resubmit, or
    curl -sk -H "Authorization: Bearer $TOKEN" -H "Content-Type:
    application/json" -X POST '`job_self_link`/resubmit'
    """
    # curl resubmit endpoint
    url = os.path.join(job_self_link, "resubmit")
    token = agaveutils.get_api_token(r.client)
    headers = {"Authorization": 'Bearer {}'.format(token),
               "Content-Type": "application/json"}
    print(url, headers)
    resub_resp = requests.post(url, headers=headers)
    new_tapis_jobId = resub_resp.json().get('result', {}).get('id')
    r.logger.info("Resubmitted with new Tapis " +
                  "jobId={}".format(new_tapis_jobId))
    # r.logger.debug(resub_resp.json())
    # add webhooks to self on FINISHED and FAILED if they do not exist
    if notif_add_self:
        add_self_to_notifs(new_tapis_jobId)
    return new_tapis_jobId

def mpj_reset(mpjId, tapis_jobId, experiment_id, sample_id):
    from agavepy.agave import Agave
    from datacatalog.tokens import get_admin_token
    from datacatalog.managers.pipelinejobs.jobmanager import JobManager
    mg_auth = os.getenv('_REACTOR_MONGODB_AUTHN')
    mongodb={'authn':mg_auth, 'database': 'catalog_staging'}
    ag = r.client
    seed = os.getenv('_ATOKEN_SEED')
    os.environ["CATALOG_ADMIN_TOKEN_KEY"] = seed
    atoken = get_admin_token(seed)
    job = JobManager(mongodb, agave=ag).load(mpjId)
    r.logger.error("Resetting job: {}".format(mpjId))
    job.reset(token=atoken)
    try:
        job.ready(token=atoken)
    except Exception as e:
        print("can't set to ready ", job.state)
    try:
        #agpy = Agave.restore()
        #new_tapis_id = agpy.jobs.manage(body='{"action":"resubmit"}', jobId=tapis_jobId)['id']
        new_tapis_id = ag.jobs.manage(body='{"action":"resubmit"}', jobId=tapis_jobId)['id']
        job.run({"launched": new_tapis_id, "experiment_id": experiment_id, "sample_id": sample_id})
        r.logger.info("Tapis job resubmitted: {}".format(new_tapis_id))
    except Exception as e:
        r.logger.error("Error submitting job: {}".format(e))
        print(e)
        print("Almost definitely an issue with reactor/agavepy Auth")
    return



def query_jobs_table(query={}, projection={}, dbURI="", return_max=-1):
    """MongoDB.find(`query`, `projection`) in SD2E datacatalog. Looks
    in jobs view of catalog_staging db.
    """
    # handle defaults
    if not dbURI:
        dbURI = "REMOVED"
    # omit projection parameter if you want all fields returned
    args = [query]
    if projection:
        args.append(projection)
    jobs_table = pymongo.MongoClient(dbURI).catalog_staging.jobs
    results = []
    i = 1
    for job in jobs_table.find(*args):
        results.append(job)
        if return_max == -1:
            continue
        elif i >= return_max:
            break
        else:
            i += 1
    return results


def validate_archive(dir_path, glob_query, min_mb=0.0):
    """Checks for existence and file size for each file in
    `glob_query`. `dir_path` is prepended to each file name. Returns
    True if `glob_query` files exist and > `min_mb` MB, False otherwise.

    Args:
        dir_path (str): directory path prepended to each glob query
        glob_query (list): list of file names. Glob formats supported
        min_mb (float): minimum file size threshold in megabytes

    Returns:
        boolean
    """
    if not glob(dir_path):
        r.on_failure("Could not find directory at {}".format(dir_path),
                     FileNotFoundError())
        return False
    elif not glob(dir_path + "/*.err"):
        r.logger.warning("No error files found in archive directory" +
                         format(dir_path))

    file_names = []
    for fname in glob_query:
        fp = dir_path + fname
        match = glob(fp)
        if not match:
            r.logger.error("No file found at '{}'".format(fp))
            return False, 'NA'
        elif len(match) < 1:
            r.logger.warning("{} files found matching {}".format(
                len(match), fp))
        size_mb = os.path.getsize(match[0])/1048576.
        if size_mb < min_mb:
            r.logger.error("Insufficient file size for " +
                           "{} ({} MB < {} MB)".format(fp, size_mb, min_mb))
            return False, 'NA'
        os.path.normpath(match[0].split('/work/projects/SD2E-Community/prod/data')[1])
        file_names.append(match[0].split('/work/projects/SD2E-Community/prod/data')[1])
    return True, file_names


def count_tapis_msg(mpjId):
    """Counts number of {'status': ''} messages sent to PipelineJob with
    uuid `mpjId`
    """
    query = {
        'uuid': mpjId,
        'history': {'$elemMatch': {'data.launched': {'$exists': True}}}
    }
    # projection = {
    #     'history': 1 #{'$elemMatch': {'data.launched': {'$exists': True}}}
    # }
    # query db
    response = query_jobs_table(query=query, projection={})
    if not response:
        r.logger.error("Jobs table found no jobs querying against query={}".format(query))
        return int(0)
    # identify messages from Tapis jobs as ones with {'data': {'status': '*'}}
    tapis_messages = []
    for m in response[0]['history']:
        if type(m.get('data')) != dict:
            continue
        elif m['data'].get('launched'):
            # == 'FINISHED' || m.get('name') == 'finish':
            tapis_messages.append(m)
            experiment_id = m['data'].get('experiment_id')
            sample_id = m['data'].get('sample_id')
            measurement_id = response[0]['child_of']
            state = response[0]['state']
    return len(tapis_messages), experiment_id, sample_id, measurement_id, state


def mpj_validate(mpjId):
    from datacatalog.managers.pipelinejobs.jobmanager import JobManager
    mg_auth = os.getenv('_REACTOR_MONGODB_AUTHN')
    mongodb={'authn':mg_auth, 'database': 'catalog_staging'}
    #ag = r.client
    #job = JobManager(mongodb, agave=ag).load(mpjId)
    # if job.state != 'FINISHED':
    #     #job.finish()
    #     r.logger.error("Cannot Validate job in " +
    #                    "{} state".format(job.state))
    # else:
    #     #job.validate()
    #     #job.validated()
    #     jobs_manager = copy.copy(r.settings.pipelines)['job_manager_id']
    #     r.send_message(jobs_manager,
    #                    {
    #                        'uuid': mpjId,
    #                        'name': 'validate'
    #                    }
    #                    )
    #     r.send_message(jobs_manager,
    #                    {
    #                        'uuid': mpjId,
    #                        'name': 'validated'
    #                    }
    #                    )
    #     r.logger.info("Pipeline job validated: " +
    #                    "{}".format(mpjId))
    return


def archiveSystem_to_path(systemId):
    """docs
    """
    try:
        sys_resp = r.client.systems.get(systemId=systemId)
    except HTTPError as e:
        r.on_failure("Error getting systemId={} ".format(systemId) +
                     "from Tapis API", e)
    rootDir = sys_resp.get('storage', {}).get('rootDir', '')
    homeDir = sys_resp.get('storage', {}).get('homeDir', '/')
    path = os.path.normpath(rootDir + homeDir)
    if path == '/':
        r.logger.warning("systemId={} homeDir={}.".format(systemId, path) +
                         " Unusual homeDir path.")
    return path


def main():
    """Main function"""
    global r
    r = Reactor()
    ag = r.client
    # r.logger.debug(json.dumps(r.context, indent=4))

    # pull from message context and settings
    context=r.context  # Actor context
    print(json.dumps(context, indent=4))
    mpjId=context.mpjId
    m=context.message_dict
    tapis_jobId=m['id']
    if m['status'] != 'FINISHED':
        r.on_failure("Tapis jobId={} has status {}.".format(
            tapis_jobId, m['status']) + "Skipping validation.")
        exit(0)

    opts=require_keys(r.settings.options, ['max_retries', 'min_fastq_mb',
                                           'notif_add_self'])
    # force_resubmit=bool(True) to override max_retries
    opts['force_resubmit'] = getattr(opts, 'force_resubmit', False)
    if 'force_resubmit' in m:
        opts['force_resubmit'] = True
    r.logger.info("Validating datacatalog jobId=" +
                  "{}, tapis_jobId={}".format(mpjId, tapis_jobId))
    # rmpj.validating(data={})

    # query Tapis against tapis_jobId
    try:
        tapis_resp = ag.jobs.get(jobId=tapis_jobId)
    except HTTPError as e:
        r.on_failure("Error pulling jobId from Tapis API", e)
    # Make sure we have the necessary keys
    job = require_keys(tapis_resp, ['archiveSystem', 'archivePath',
                                    'status', '_links'])
    # job['_link']['self']['href']
    job['self_link'] = job['_links'].get('self', {}).get('href', '')
    # get the /work path from archiveSystem
    archiveSystem = archiveSystem_to_path(job['archiveSystem'])

    #out_files = ["/*R1*trimmed*.fastq.gz", "/*R2*trimmed*.fastq.gz"]
    out_files = ["/*1_*rimmed*.fastq.gz", "/*2_*rimmed*.fastq.gz"]
    try:
        if context.analysis_type:
            if context.analysis_type == 'alignment':
                analysis_type = 'alignment'
                out_files = ["/*RG.bam", "/*RG.bai"]
            else:
                analysis_type = 'preprocessing'
    except Exception as e:
        analysis_type = 'preprocessing'
        print(e)

    # check for existence and size of fastq files
    is_valid, file_names = validate_archive(os.path.join(
                                archiveSystem + job['archivePath']),
                                out_files,
                                min_mb=opts['min_fastq_mb'])

    if is_valid:
        r.logger.info("Outputs for preprocessing jobId=" +
                      "{} passed validation".format(tapis_jobId))
        num_tapis_msg, experiment_id, sample_id, measurement_id, state = count_tapis_msg(mpjId)
        #if state == 'RUNNING':


        # rmpj.validated(data={})
        # ag.actors.sendMessage(actorId='alignment rx',body={'message':''})
        alignment_rx = copy.copy(r.settings.pipelines)['alignment_rx']
        if analysis_type == 'preprocessing':
            r.send_message(alignment_rx,
                           {
                               'experiment_id': experiment_id,
                               'sample_id': sample_id,
                               'measurement_id': measurement_id,
                               'file_names': file_names
                           }
                           )
            mpj_validate(mpjId)
        if analysis_type == 'alignment':
            mpj_validate(mpjId)
    else:
        r.logger.error("Outputs for preprocessing jobId=" +
                       "{} failed validation".format(tapis_jobId))
        num_tapis_msg, experiment_id, sample_id, measurement_id, status = count_tapis_msg(mpjId)
        # resetting pipeline jobs actually deletes everything in the
        # archivePath (or it's supposed to), good idea but I'm
        # going to remove this
        #err_file_ct = len(glob(os.path.join(archiveSystem + job['archivePath'], "*.err")))

        # Only resubmit if the # error files in the archivePath
        # and Tapis jobs in the datacatalog are both less than max_retries
        r.logger.debug("Found {}".format(num_tapis_msg) +
                       " Tapis jobs in datacatalog")
        resubmit = bool(num_tapis_msg < opts['max_retries']
                        or opts['force_resubmit'] is True)
        if resubmit:
            r.logger.info("Resubmitting Tapis jobId={}".format(tapis_jobId))
            mpj_reset(mpjId, tapis_jobId, experiment_id, sample_id)
            #resubmit_resp = jobs_resubmit(job_self_link=job['self_link'],
                                          #notif_add_self=opts['notif_add_self'])
        else:
            #job_manager_id = opts.get('pipelines', {}).get('job_manager_id', '')
            #mpj_update(job_manager_id, 'fail')
            r.on_failure("Cannot resubmit jobId={}, max_retries={}".format(
                tapis_jobId, opts['max_retries']) +
                " has been met or exceeded")


if __name__ == '__main__':
    main()
