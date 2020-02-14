from reactors.utils import Reactor, agaveutils
from agavepy.agave import Agave
from datacatalog.managers.pipelinejobs import ReactorManagedPipelineJob as Job
import json
import pprint
import copy
import pymongo
import glob


def parse_manifest(r):
    ag = r.client  # Agave client
    context = r.context  # Actor context
    m = context.message_dict

    # start parsing manfiest
    manifestUrl = m.get('uri')
    if manifestUrl is None:
        try:
            manifestUrl = context.manifestUrl
        except Exception as e:
            print("No manifestUrl specified")
            exit(1)
    (agaveStorageSystem, dirPath, manifestFileName) = \
        agaveutils.from_agave_uri(uri=manifestUrl)
    # get the manifest and start parsing it
    manifestPath = dirPath + "/" + manifestFileName
    try:
        mani_file = agaveutils.agave_download_file(agaveClient=ag,
                                                   agaveAbsolutePath=manifestPath,
                                                   systemId=agaveStorageSystem,
                                                   localFilename=manifestFileName)
    except Exception as e:
        r.on_failure("failed to get manifest {}".format(manifestUrl), e)

    if mani_file is None:
        r.on_failure("failed to get manifest {}".format(manifestUrl), e)

    try:
        manifest = json.load(open(manifestFileName))
    except Exception as e:
        r.on_failure("failed to load manifest {}".format(manifestUrl), e)

    return manifest

def dataframe_query(experiment_id):
    dbURI = '***REMOVED***'
    client = pymongo.MongoClient(dbURI)
    db = client.catalog_staging
    jobs_table = db.jobs


    # we'll query for every job that was run on a certain experiment,
    # this will capture preprocessing, alignment, and dataframe production jobs
    # TODO: add exp_id to top-level 'data' field instead of history
    query = {}
    #query['history.data.experiment_id'] = 'experiment.ginkgo.19606.19637.19708.19709'
    query['history.data.experiment_id'] = experiment_id
    query["pipeline_uuid"] = "106231a1-0c78-5067-b53b-11a33f4e1495"
    query['state'] = 'FINISHED'
    dataframe_record = []
    for job in jobs_table.find(query):
        dataframe_record.append(job)

    if len(dataframe_record) > 1:
        print("Multiple Dataframe Records Recieved for Query: ", query)
        r.logger.info("Multiple Dataframe Records Recieved for Query {}".format(query))
        exit(1)
    return dataframe_record


def submit_job(r, manifest, dataframe_record):
    ag = r.client
    rna_list = []
    for sample in manifest["samples"]:
        mes_types = [measurement["measurement_type"] for measurement in sample["measurements"]]
        if mes_types != []:
            if mes_types[0] == "RNA_SEQ":
                rna_list.append(sample)

    for sample in rna_list:
        norm = [measurement for measurement in sample['measurements'] if measurement['library_prep'] == 'NORMAL'][0]
        raw = [file for file in norm['files'] if file['lab_label'] == ['RAW']]
        if len(norm) > 0:
            norm['files'] = raw
            sample['measurements'] = [norm]

    manifest['samples'] = rna_list

    experiment_id = manifest['experiment_id']
    measurements = [sample['measurements'][0]['measurement_id'] for sample in manifest['samples']]
    sample_ids = [sample["sample_id"] for sample in manifest['samples']]
    archive_path = dataframe_record[0]['archive_path']

    df_files = [archive_path + '/ReadCountMatrix_preCAD.tsv',
                archive_path + '/ReadCountMatrix_preCAD_FPKM.tsv',
                archive_path + '/ReadCountMatrix_preCAD_TPM.tsv']

    job_def = copy.copy(r.settings.metadataJob)
    job_def["name"] = experiment_id
    parameters = job_def.parameters
    parameters["experiment_id"] = experiment_id
    job_def.parameters = parameters

    data = {
        "experiment_id": experiment_id
        }
    archive_patterns = [
       {'level': '2', 'patterns': ['.csv$']}
    ]

    product_patterns = [
        {'patterns': ['.csv$'],
        'derived_using': [],
        'derived_from': df_files
        }
    ]

    mpj = Job(r, experiment_id=experiment_id, data=data,
              archive_patterns=archive_patterns,
              product_patterns=product_patterns)
              #setup_archive_path=False)
    mpj.setup()
    print(json.dumps(job_def, indent=4))

    pprint.pprint(r.settings.pipelines)
    print("data:")
    pprint.pprint(data)
    print("product_patterns:")
    pprint.pprint(product_patterns)
    print("archive_patterns:")
    pprint.pprint(archive_patterns)

    print("JOB UUID: ", mpj.uuid)
    r.logger.info("Created Pipeline job {}".format(mpj.uuid))
    job_def.archivePath = mpj.archive_path

    notif = [{'event': 'RUNNING',
              "persistent": True,
              'url': mpj.callback + '&status=${JOB_STATUS}'},
             {'event': 'FAILED',
              "persistent": False,
              'url': mpj.callback + '&status=${JOB_STATUS}'},
             {'event': 'FINISHED',
              "persistent": False,
              'url': mpj.callback + '&status=${JOB_STATUS}'}]

    #notifications = job_template["notifications"]
    #if notifications is not None:
    #    for item in notifications:
    #        notif.append(item)

    job_def["notifications"] = notif
    print(job_def)
    try:
        job_id = ag.jobs.submit(body=job_def)['id']
        print(json.dumps(job_def, indent=4))
        r.logger.info("Submitted Agave job {}".format(job_id))
        mpj.run({"launched": job_id, "experiment_id": experiment_id})
    except Exception as e:
        print(json.dumps(job_def, indent=4))
        r.logger.error("Error submitting job: {}".format(e))
        print(e.response.content)

    return




    # multiqc Job
    # job_def = copy.copy(r.settings.multiqcJob)
    # inputs = job_def.parameters
    # job_def["name"] = "multiqc-" + str(lastAction)
    # inputs["input_dir1"] = "/work/projects/SD2E-Community/prod/data/%s/%s/" % \
    #           (archivePath, "preprocessed")
    # inputs["input_dir2"] = "/work/projects/SD2E-Community/prod/data/%s/%s/" % \
    #           (archivePath, "alignments")
    # job_def.parameters = inputs
    # job_def.archiveSystem = archiveSystem
    # job_def.archivePath = archivePath + "/multiqc/"
    # custom_pipeline = CustomPipelineJob(r, archivePath)
    # custom_pipeline.setup(data=job_def)
    #
    # notif = [{'event': 'RUNNING',
    #           'url': custom_pipeline.callback + '&status=${STATUS}'+'&pipeline_id=' + custom_pipeline.pipeline_uuid + '&event=RUNNING'},
    #          {'event': 'FAILED',
    #           'url': custom_pipeline.callback + '&status=${STATUS}' +'&pipeline_id=' + custom_pipeline.pipeline_uuid + '&event=FAILED'},
    #          {'event': 'FINSIHED',
    #           'url': custom_pipeline.callback + '&status=${STATUS}'+'&pipeline_id=' + custom_pipeline.pipeline_uuid + '&event=${JOB_STATUS}'}]
    # notifications = job_def["notifications"]
    # if notifications is not None:
    #     for item in notifications:
    #         notif.append(item)
    #
    # job_def["notifications"] = notif
    #
    # print job_def
    # try:
    #     job_id = ag.jobs.submit(body=job_def)['id']
    #     print(json.dumps(job_def, indent=4))
    #     custom_pipeline.run()
    # except Exception as e:
    #     print(json.dumps(job_def, indent=4))
    #     print("Error submitting job: {}".format(e))
    #     print e.response.content
    #
    # r.logger.info("Submitted Agave job {}".format(job_id))
    #
    # return

def main():
    """Main function"""
    r = Reactor()

    manifest = parse_manifest(r)
    dataframe_record = dataframe_query(manifest['experiment_id'])
    submit_job(r, manifest, dataframe_record)
    return


if __name__ == '__main__':
    main()
