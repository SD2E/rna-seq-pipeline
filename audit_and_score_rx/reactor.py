from reactors.utils import Reactor, agaveutils
from urllib.parse import unquote, urlsplit
from inspect import getsource as gs
from pprint import pprint as pp
from requests.exceptions import HTTPError
import json
import os
from glob import glob
import pymongo
from datacatalog.managers.pipelinejobs import ReactorManagedPipelineJob as Job


def get_datacat_jobs(query={}, projection={}, dbURI="", return_max=-1):
    """Some docs"""
    # handle defaults
    if not query:
        query = { 'pipeline_uuid': "106d3f7f-07dc-596f-86f9-df75083e52cc"}
    if not dbURI:
        dbURI='***REMOVED***'
    # omit projection parameter for all fields
    args = [query]
    if projection:
        args.append(projection)
    db = pymongo.MongoClient(dbURI).catalog_staging
    jobs_table = db.jobs
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


def get_from_dcuuid(datacatalog_uuid, extra_fields=[]):
    """Given `datacatalog_uuid` corresponding to a datacatalog
    job, return the Tapis job ID. Also returns projected MongoDB
    response.

    Args:
        datacatalog_uuid (str): UUID of datacatalog PipelineJob
        extra_fields (list): list of keynames of additional fields
        to return in response object e.g. ['archive_system', 'session']
        Defaults to empty list.

    Returns:
        tuple: (MongoDB response, Tapis job ID)
    """
    # assemble query and projection
    query = {
        'uuid': datacatalog_uuid,
        'history': {'$elemMatch': {'data.launched': {'$exists': True}}}
    }
    projection = {
        'history': {'$elemMatch': {'data.launched': {'$exists': True}}}
    }
    # append extra_fields per arg
    for f in extra_fields:
        projection[f] = 1
    # query db
    datacat_response = get_datacat_jobs(query=query, projection=projection)
    if not datacat_response:
        r.logger.error("Jobs table found no jobs querying against query={}".format(query))
        return ({}, "")
    try:
        tapis_jobId = datacat_response[0]['history'][0]['data']['launched']
    except (IndexError, KeyError) as e:
        # really shouldnt happen since query, projection, and indexing are consistent
        r.on_failure("Error parsing datacatalog_uuid={}".format(datacatalog_uuid), e)
        #raise e
    return (datacat_response[0], tapis_jobId)


def dl_from_agave(url):
    """Given agave-type file path `url`, attempts to download file
    and returns the path of the copy at cwd.

    Args:
        url (str): Agave-accessible URL pointing to file e.g.
        agave://data-tacc-work/folder/subfolder/file.txt

    Returns:
        str: path to local copy of file
    """
    assert type(url) == str
    url_split = urlsplit(url)
    filename = url_split[2][url_split[2].rfind("/")+1:]
    #r.logger.debug("url_split={}".format(url_split))
    #r.logger.debug("filename={}".format(filename))
    try:
        local_fp = agaveutils.agave_download_file(
            agaveClient=r.client,
            agaveAbsolutePath=url_split[2],
            systemId=url_split[1],
            localFilename=filename)
    except Exception as e:
        try:
            print(e.response.content)
            assert e.response.content is not None
        except AssertionError:
            print("Response is NoneType")
        except AttributeError:
            pass
        finally:
            r.logger.error(e)
            print(e)
            return ""
    else:
        return local_fp


def check_fastq(dir_path, glob_query, min_mb=1.):
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
        r.on_failure("Could not find directory at {}".format(dir_path))
        return False
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


def main():
    """Main function"""
    global r
    r = Reactor()
    ag = r.client
    # r.logger.debug(json.dumps(r.context, indent=4))

    sample_args = {
        "experiments": ['experiment.tacc.10001'],
        "samples": ['sample.tacc.20001'],
        "measurements1": ['measurement.tacc.0xDEADBEF1'],
        "measurements2": ['10483e8d-6602-532a-8941-176ce20dd05a', 'measurement.tacc.0xDEADBEF0'],
        "measurements3": ['measurement.tacc.0xDEADBEEF', 'measurement.tacc.0xDEADBEF0'],
        "data_w_inputs": {'alpha': 0.5, 'inputs': ['agave://data-sd2e-community/uploads/tacc/example/345.txt'], 'parameters': {'ref1': 'agave://data-sd2e-community/reference/novel_chassis/uma_refs/MG1655_WT/MG1655_WT.fa'}}
    }

    mpj_kwargs = {
        "experiment_id": "eho-dummy-id",
        "data": {
            "experiment_id": "experiment.tacc.10001"
        },
        "archive_patterns": {},
        "product_patterns": {}
    }

    rmpj = ReactorManagedPipelineJob(r, **mpj_kwargs)
    print(rmpj)

    return
    # pull datacat_jobId from context
    #datacat_jobId = getattr(r.context, 'datacatalog_jobId', '')
    r.logger.info("Pulled datacatalog jobId={}".format(datacat_jobId))

    # query the MongoDB jobs table
    (datacat_response, tapis_jobId) = get_from_dcuuid(
        datacat_jobId, ['archive_system', 'archive_path'])
    job_dir = '/work/projects/SD2E-Community/prod/data/' + \
        datacat_response['archive_path']
    pp(datacat_response)

    r.logger.info("Tapis jobId={}".format(tapis_jobId))
    # check for existence and size of fastq files
    valid = check_fastq(job_dir,
                        ["/*R1*.fastq.gz", "/*R2*.fastq.gz"],
                        min_mb=10)
    print(valid)


    # try:
    #     print(ag.files.list(systemId=datacat_response['archive_system'],
    #                         filePath=datacat_response['archive_path']))
    # except HTTPError as e:
    #     print(e)
    # r.logger.info(glob(archive_dir))
    # r.logger.info(os.listdir(archive_dir))

if __name__ == '__main__':
    main()
