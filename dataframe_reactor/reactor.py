from reactors.utils import Reactor, agaveutils
from agavepy.agave import Agave
from datacatalog.managers.pipelinejobs import ReactorManagedPipelineJob as Job
import json
import pprint
import copy
import pymongo
import glob


def manifest(r, archive_paths):
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

    rna_list = []
    for sample in manifest["samples"]:
        mes_types = [measurement["measurement_type"] for measurement in sample["measurements"]]
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
    # Clean up the archive_paths dict to only include the relevant sample_paths
    archive_paths = {sample_id:path for sample_id,path in archive_paths.items() if sample_id in sample_ids}
    # Write as a local file for stupid agavepy upload, update w/ bacanora when ready
    with open('sample_paths.json', 'w') as outfile:
        json.dump(archive_paths, outfile, indent=4)

    #bam_files = [sample['bam_file'] for sample in manifest['samples']]
    bam_files = [path.split('/work/projects/SD2E-Community/prod/data/')[1] for path in archive_paths.values()]


    job_def = copy.copy(r.settings.bundleJob)
    job_def["name"] = experiment_id
    ag = r.client
    parameters = job_def.parameters
    parameters["path_gff"] = "/reference/novel_chassis/uma_refs/amin_genes_no_parts_1.1.0.gff"
    job_def.parameters = parameters

    data = {
        "inputs": {
            'path_gff': parameters["path_gff"]
            }
        }
    archive_patterns = [
       {'level': '2', 'patterns': ['.txt$', '.tsv$']}
    ]

    product_patterns = [
        {'patterns': ['.txt$', '.tsv$'],
        'derived_using': [
            parameters["path_gff"]
            ],
        'derived_from': bam_files
        }
    ]
    #mpj = Job(r, measurement_id=measurements, data=data, archive_patterns=archive_patterns, product_patterns=product_patterns)
    mpj = Job(r, experiment_id=experiment_id, data=data, archive_patterns=archive_patterns, product_patterns=product_patterns)
    mpj.setup()
    #ag.files.importData(filePath=mpj.archive_path, systemId='data-sd2e-community', fileName = 'sample_paths.json', fileToUpload=open('sample_paths.json', 'rb'))
    inputs = job_def.inputs
    #inputs["sample_paths"] = 'agave://data-sd2e-community/' + mpj.archive_path + '/sample_paths.json'
    ag.files.importData(filePath='/testing/agavepy_write/', systemId='data-tacc-work-urrutia', fileName = 'sample_paths.json', fileToUpload=open('sample_paths.json', 'rb'))
    inputs["sample_paths"] = 'agave://data-tacc-work-urrutia/testing/agavepy_write/sample_paths.json'
    #inputs["sample_paths"] = 'agave://data-tacc-work-urrutia/wrangler/Ginkgo/experiment.ginkgo.19606.19637.19708.19709_NAND_Titration/data_paths.json'
    job_def.inputs = inputs

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
        r.logger.info("Submitted Agave job {}".format(job_id))
        mpj.run({"launched": job_id, "experiment_id": experiment_id})
    except Exception as e:
        print(json.dumps(job_def, indent=4))
        r.logger.error("Error submitting job: {}".format(e))
        print(e.response.content)




def mongo_query(r):
    dbURI = '***REMOVED***'
    client = pymongo.MongoClient(dbURI)
    db = client.catalog_staging
    #filesdb = db.files
    jobs = db.jobs
    query={}
    #query['name'] = {'$regex': 'RG.bam'}
    query['archive_path'] = {'$regex': '/products/v2/106bd127e2d257acb9be11ed06042e68/'}
    #query['archive_path'] = {'$regex': '/products/v2/106d3f7f07dc596f86f9df75083e52cc'}
    bwa_results = []
    #for match in filesdb.find(query):
    for match in jobs.find(query):
        bwa_results.append(match)

    archive_paths = {}
    good_list = []
    fail_list = []


    for sample in bwa_results:
        try:
            sample_id = [data['data'] for data in sample['history'] if data['name'] == 'run'][0]['sample_id']
            path = '/work/projects/SD2E-Community/prod/data/' + sample['archive_path']+ '/'
            try:
                bwa=glob.glob(path + sample_id + "*RG.bam")[0]
            except Exception as e:
                bwa=None
            archive_paths[sample_id] = bwa
        except Exception as e:
            print(e)
            fail_list.append(sample)

    manifest(r,archive_paths)




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
    #manifest(r)
    mongo_query(r)


if __name__ == '__main__':
    main()
