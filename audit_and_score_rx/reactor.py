from reactors.utils import Reactor, agaveutils
from urllib.parse import unquote, urlsplit
from inspect import getsource as gs
from pprint import pprint as pp
from requests.exceptions import HTTPError
import json
import os
import requests
from glob import glob
import pymongo


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


def ag_jobs_resubmit(job_self_link):
    """Re-submit an Agave/Tapis job. The job is assigned a new uuid,
    but is submitted with exactly the same definition, parameters, and
    inputs. In the context of this reactor, this means it will also have
    the same archivePath. Equivalent to CLI command jobs-resubmit, or
    curl -sk -H "Authorization: Bearer $TOKEN" -H "Content-Type:
    application/json" -X POST '`tapis_link`/resubmit'
    """
    url = os.path.join(job_self_link, "resubmit")
    token = agaveutils.get_api_token(r.client)
    headers = {"Authorization": 'Bearer {}'.format(token),
               "Content-Type": "application/json"}
    response = requests.post(url, headers=headers)
    return response


def query_jobs_table(query={}, projection={}, dbURI="", return_max=-1):
    """MongoDB.find(`query`, `projection`) in SD2E datacatalog. Looks
    in jobs view of catalog_staging db.
    """
    # handle defaults
    if not dbURI:
        dbURI = '***REMOVED***'
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


def validate_archive(dir_path, glob_query, min_mb=1.):
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
    for fname in glob_query:
        fp = dir_path + fname
        match = glob(fp)
        if not match:
            r.logger.error("No file found at '{}'".format(fp))
            return False
        elif len(match) < 1:
            r.logger.warning("{} files found matching {}".format(
                len(match), fp))
        size_mb = os.path.getsize(match[0])/1048576.
        if size_mb < min_mb:
            r.logger.error("Insufficient file size for " +
                           "{} ({} MB < {} MB)".format(fp, size_mb, min_mb))
            return False
    return True


def count_tapis_msg(mpjId):
    """Counts number of {'status': ''} messages sent to PipelineJob with
    uuid `mpjId`
    """
    query = {
        'uuid': mpjId,
        'history': {'$elemMatch': {'data.launched': {'$exists': True}}}
    }
    projection = {
        'history': 1 #{'$elemMatch': {'data.launched': {'$exists': True}}}
    }
    # query db
    response = query_jobs_table(query=query, projection=projection)
    if not response:
        r.logger.error("Jobs table found no jobs querying against query={}".format(query))
        return int(0)
    # identify messages from Tapis jobs as ones with {'data': {'status': '*'}}
    tapis_messages = []
    for m in response[0]['history']:
        if type(m.get('data')) != dict:
            continue
        elif m['data'].get('status') in ("FINISHED", "FAILED"):
            # == 'FINISHED' || m.get('name') == 'finish':
            tapis_messages.append(m)
    return len(tapis_messages)


def main():
    """Main function"""
    global r
    r = Reactor()
    ag = r.client
    r.logger.debug(json.dumps(r.context, indent=4))

    # pull from message context and settings
    msg = require_keys(r.context, ['mpjId', 'tapis_jobId'])
    settings = require_keys(r.settings.options, ['max_retries', 'work_mount',
                                                 'min_fastq_mb'])
    r.logger.info("Validating datacatalog jobId={}".format(msg['mpjId']))
    # rmpj.validating(data={})

    # query Tapis against tapis_jobId
    try:
        tapis_resp = ag.jobs.get(jobId=msg['tapis_jobId'])
    except HTTPError as e:
        r.on_failure("Error pulling jobId from Tapis API", e)
    # Make sure we have the necessary keys
    job = require_keys(tapis_resp, ['archiveSystem', 'archivePath',
                                    'status', '_links'])
    # job['_link']['self']['href']
    job['self_link'] = jobs['_links'].get('self', {}).get('href', '')

    # check for existence and size of fastq files
    is_valid = validate_archive(settings['work_mount'] + job['archivePath'],
                                ["/*R1*.fastq.gz", "/*R2*.fastq.gz"],
                                min_mb=settings['min_fastq_mb'])
    if job['status'] != 'FINISHED':
        r.on_failure("Tapis jobId={} {}.".format(
            msg['tapis_jobId'], job['status']) + "Skipping resubmission.")
    elif is_valid:
        r.logger.info("Outputs for preprocessing jobId=" +
                      "{} passed validation".format(msg['tapis_jobId']))
        # rmpj.validated(data={})
        # ag.actors.sendMessage(actorId='alignment rx',body={'message':''})
    else:
        r.logger.error("Outputs for preprocessing jobId=" +
                       "{} failed validation".format(msg['tapis_jobId']))
        num_tapis_msg = count_tapis_msg(msg['mpjId'])
        err_file_ct = len(glob(settings['work_mount'] +
                               job['archivePath'] + "/*.err"))
        # Only resubmit if the # error files in the archivePath
        # and Tapis jobs in the datacatalog are both less than max_retries
        if err_file_ct >= settings['max_retries'] or num_tapis_msg >= settings['max_retries']:
            r.on_failure("Cannot resubmit jobId={}, max_retries={}".format(
                msg['tapis_jobId'], settings['max_retries']) +
                " has been met or exceeded")
            # rmpj.fail(data={})
            return
        else:
            # resubmit Tapis job
            r.logger.info("Resubmitting Tapis jobId={}".format(msg['tapis_jobId']))
            resubmit_resp = ag_jobs_resubmit(job_self_link=job['self_link'])
            r.logger.info(resubmit_resp.content)


if __name__ == '__main__':
    main()
