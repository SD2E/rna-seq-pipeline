from reactors.utils import Reactor, agaveutils
from agavepy.agave import Agave
from datacatalog.managers.pipelinejobs import ReactorManagedPipelineJob as Job
import json
import pprint
import copy


def manifest(r):
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

    experiment_id = manifest['experiment_id']
    measurements = [sample['measurements'][0]['measurement_id'] for sample in manifest['samples']]
    bam_files = [sample['bam_file'] for sample in manifest['samples']]

    job_def = copy.copy(r.settings.bundleJob)
    job_def["name"] = experiment_id
    ag = r.client
    inputs = job_def.parameters
    inputs["path_gff"] = "reference/novel_chassis/uma_refs/amin_genes_no_parts_1.1.0.gff"
    inputs["path_bam_dir"] = 'products/v2/106bd127e2d257acb9be11ed06042e68/'
    job_def.parameters = inputs

    data = {
        "inputs": {
            'path_gff': inputs["path_gff"],
            'path_bam_dir"': inputs["path_bam_dir"]
            }
        }
    archive_patterns = [
       {'level': '2', 'patterns': ['.txt$', '.tsv$']}
    ]

    product_patterns = [
        {'patterns': ['.txt$', '.tsv$'],
        'derived_using': [
            inputs["path_gff"],
            inputs["path_bam_dir"]
            ],
        'derived_from': bam_files
        }
    ]

    pprint.pprint(r.settings.pipelines)
    print("data:")
    pprint.pprint(data)
    print("product_patterns:")
    pprint.pprint(product_patterns)
    print("archive_patterns:")
    pprint.pprint(archive_patterns)

    #mpj = Job(r, measurement_id=measurements, data=data, archive_patterns=archive_patterns, product_patterns=product_patterns)
    mpj = Job(r, experiment_id=experiment_id, data=data, archive_patterns=archive_patterns, product_patterns=product_patterns)
    mpj.setup()

    print("JOB UUID: ", mpj.uuid)
    job_def.archivePath = mpj.archive_path

    notif = [{'event': 'RUNNING',
              "persistent": True,
              'url': mpj.callback + '&status=${JOB_STATUS}'},
             {'event': 'FAILED',
              "persistent": False,
              'url': mpj.callback + '&status=${JOB_STATUS}'},
             {'event': 'ARCHIVING_FINISHED',
              "persistent": False,
              'url': mpj.callback + '&status=FINISHED'}]

    #notifications = job_template["notifications"]
    #if notifications is not None:
    #    for item in notifications:
    #        notif.append(item)

    job_def["notifications"] = notif
    print(job_def)
    try:
        job_id = ag.jobs.submit(body=job_def)['id']
        print(json.dumps(job_def, indent=4))
        mpj.run({"launched": job_id, "experiment_id": experiment_id})
        r.logger.info("Submitted Agave job {}".format(job_id))
    except Exception as e:
        print(json.dumps(job_def, indent=4))
        r.logger.error("Error submitting job: {}".format(e))
        print(e.response.content)



#def check_mongo(r):
    # dbURI = '***REMOVED***'
    # client = pymongo.MongoClient(dbURI)
    # db = client.catalog_staging
    # filesdb = db.files
    # jobs = db.jobs
    #
    # query={}
    # ## Plan is to query of experiment_id to make sure all files from an exp have
    # ## been processed, but I need to add exp_id to the pipeline jobs objects
    # ## for now I'm just going to hardcode an archive path
    # query['archive_path'] = {'$regex': '/products/v2/106bd127e2d257acb9be11ed06042e68'}
    # results = []
    # for match in jobs.find(query):
    #     results.append(match)
    #
    # archive_paths = {}
    # for sample in results:
    #     try:
    #         archive_paths[[data['data']['sample_id'] for data in sample['history'] if data['name'] == 'run'][0]] = sample['archive_path']
    #     except Exception as e:
    #         print(e)




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
    #           'url': custom_pipeline.callback + '&status=${STATUS}'+'&pipeline_id=' + custom_pipeline.pipeline_uuid + '&event=FINISHED'}]
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
    manifest(r)


if __name__ == '__main__':
    main()
