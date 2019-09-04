from reactors.utils import Reactor, agaveutils
from os import getcwd
from urllib.parse import unquote, urlsplit
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
    r.logger.debug("url_split={}".format(url_split))
    r.logger.debug("filename={}".format(filename))
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
    r.logger.debug(json.dumps(r.context, indent=4))

    # Pull flagstat file path from message context
    # try:
    #     dl_fp = r.context.dl_fp
    # except AttributeError as e:
    #     r.logger.error("Expected Reactor.context to have " + \
    #                    "attribute dl_fp:\n{}".format(e))
    # else:
    #     r.logger.info("Encoded: {}\n".format(r.context.dl_fp))
    #     r.logger.info("Unencoded: {}\n".format(unquote(r.context.dl_fp)))

    # Download flagstat file to cwd
    flagstat_remote = "agave://data-sd2e-community/products/v2/106bd127e2d257acb9be11ed06042e68/PAVyR8Dv1evr40LyJ52dX0DP/OZY85OoqyjJ2jZz2JAqLdR0J/sample.ginkgo.13108575.experiment.ginkgo.19606.19637.19708.19709_MG1655_NAND_Circuit_replicate_4_time_18.0:hours_temp_37.0_arabinose_0.5_mM_IPTG_0.00025_mM.rnaseq.original.bwa.flagstat.txt"
    flagstat_local = dl_from_agave(flagstat_remote)
    print(flagstat_local)

if __name__ == '__main__':
    main()
