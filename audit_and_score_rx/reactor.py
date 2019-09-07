from reactors.utils import Reactor, agaveutils
from os import getcwd, path
import os
from urllib.parse import unquote, urlsplit
import json
from inspect import getsource as gs
from pprint import pprint as pp


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
        #r.logger.error("Jobs table found no jobs querying against query={}".format(query))
        return ({}, "")
    try:
        tapis_jobId = datacat_response[0]['history'][0]['data']['launched']
    except (IndexError, KeyError) as e:
        # really shouldnt happen since query, projection, and indexing are consistent
        # r.on_failure("Error parsing datacatalog_uuid={}".format(datacatalog_uuid), e)
        raise e
    return (datacat_response, tapis_jobId)


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


def main():
    """Main function"""
    global r
    r = Reactor()
    ag = r.client
    r.logger.debug(json.dumps(r.context, indent=4))

    (datacat_response, tapis_jobId) = get_from_dcuuid('10779ebc-db12-5eac-9fcf-03deb2cb0c70',
                                                     ['archive_system', 'archive_path'])
    print(tapis_jobId)
    pp(datacat_response)
    # return
    # # Pull attr from message context
    # message_dict = getattr(r.context, 'message_dict', {})
    # fs_remote_fp = unquote(getattr(r.context, 'flagstat_remote_fp', ""))
    # if not message_dict:
    #     r.on_failure("Failed to pull Reactor.context.message_dict")
    # if not fs_remote_fp:
    #     r.on_failure("Failed to pull Reactor.context.flagstat_remote_fp")
    #
    # try:
    #     fs_preproc_fp = [f.value.default for f in
    #                      ag.apps.get(appId=message_dict['appId'])['outputs']
    #                      if f['id'] == "flagstat"][0]
    # except (AttributeError, IndexError) as e:
    #     r.on_failure("Failed to pull default flagstat output from " +
    #                  "app definition", e)
    # r.logger.info(fs_preproc_fp)
    #
    # #r.logger.info(ag.jobs.get(jobId=jobId))
    # try:
    #     response = ag.files.download(systemId='data-tacc-work-eho',
    #                                  filePath='archive/jobs/eho-mini_preproc_app-test-fdc4716b-9010-4e84-ae0e-089c51021e5e-007/flagstat_out.txt')
    #     r.logger.info(response)
    #     r.logger.info(type(response))
    #     r.logger.info(dir(response))
    #     print(response.text)
    #     print(response.url)
    #     print(response.request.body)
    #     print(response.history)
    #     print(response.headers)
    #     print(dict(response.json()))
    # except Exception as e:
    #     print(e)
    # try:
    #     r.logger.info(ag.files.downloadFromDefaultSystem())
    # except Exception as e:
    #     print(e)
    # try:
    #     r.logger.info(ag.files.importData(systemId='data-tacc-work-eho',
    #                                       filePath='archive/jobs/eho-mini_preproc_app-test-fdc4716b-9010-4e84-ae0e-089c51021e5e-007/flagstat_out.txt'))
    # except Exception as e:
    #     print(e)
    # try:
    #     r.logger.info(ag.files.importToDefaultSystem())
    # except Exception as e:
    #     print(e)
    # for f in os.listdir("./"):
    #     print(f)
    #
    # return
    # # Download flagstat file to cwd
    # # fs_remote_fp = "agave://data-sd2e-community/products/v2/106bd127e2d257acb9be11ed06042e68/PAVyR8Dv1evr40LyJ52dX0DP/OZY85OoqyjJ2jZz2JAqLdR0J/sample.ginkgo.13108575.experiment.ginkgo.19606.19637.19708.19709_MG1655_NAND_Circuit_replicate_4_time_18.0:hours_temp_37.0_arabinose_0.5_mM_IPTG_0.00025_mM.rnaseq.original.bwa.flagstat.txt"
    # try:
    #     fs_fp = dl_from_agave(fs_remote_fp)
    # except Exception as e:
    #     r.on_failure("Failed to download file from {}".format(fs_remote_fp), e)
    # if not fs_fp:
    #     r.on_failure("Failed to download file from {}".format(fs_remote_fp))
    #
    # r.logger.debug("fs_fp={}".format(fs_fp))
    # # Check file size
    # fs_bytes = path.getsize(fs_fp)
    # r.logger.info("File is {} bytes".format(fs_bytes))

if __name__ == '__main__':
    main()
