from reactors.utils import Reactor, agaveutils
from agavepy.agave import Agave
from datacatalog.managers.pipelinejobs import ReactorManagedPipelineJob as Job
import json
import pprint
import copy
import os
from datacatalog.tokens import get_admin_token, validate_token


def manifest(r):
    ag = r.client  # Agave client
    context = r.context  # Actor context
    m = context.message_dict
    print(json.dumps(m, indent=4))

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
        mani_file = agaveutils.agave_download_file(
                    agaveClient=ag,
                    agaveAbsolutePath=manifestPath,
                    systemId=agaveStorageSystem,
                    localFilename=manifestFileName
                    )
    except Exception as e:
        r.on_failure("failed to get manifest {}".format(manifestUrl), e)

    if mani_file is None:
        r.on_failure("failed to get manifest {}".format(manifestUrl), e)

    # check for manual override of the manifest path
    if 'manifestPathOverwrite' in m:
        dirPath = '/' + m['manifestPathOverwrite']

    try:
        manifest = json.load(open(manifestFileName))
    except Exception as e:
        r.on_failure("failed to load manifest {}".format(manifestUrl), e)
    r.logger.info("Processing: {}".format(manifestPath))

    experiment_id = manifest['experiment_id']

    rna_list = []
    bad_mes = []
    for sample in manifest["samples"]:
        mes_types = [measurement["measurement_type"] for measurement in sample["measurements"]]
        if mes_types != []:
            if mes_types[0] == "RNA_SEQ":
                rna_list.append(sample)

    if rna_list == []:
        r.logger.info("No RNA-seq measurements found in manifest: {}".format(manifestUrl))
        exit(0)

    for sample in rna_list:
        sample_id = sample['sample_id']
        sampleDict = {}
        filesDict = {}
        sampleDict[sample_id] = filesDict
        files = [file["name"] for measurement in sample["measurements"]
                 for file in measurement["files"]
                 if file["lab_label"] == ["RAW"]]

        for file in files:
            if any(R1 in file for R1 in ["_R1_","_R1.","-R1-","-R1.", "_1.fa"]):
                readDirection = "R1"
            if any(R2 in file for R2 in ["_R2_","_R2.","-R2-","-R2.", "_2.fa"]):
                readDirection = "R2"
            print(readDirection + " " + file)

            if readDirection in filesDict.keys():
                print("Two files for the same read direction discovered: \
                      Sample " + sampleName + "grouping lanes together")
                filesDict['multiple_lanes'] = True
            else:
                filesDict['multiple_lanes'] = False
            filesDict[readDirection] = file

        job_def = copy.copy(r.settings.rnaseqJob)
        job_def["name"] = sampleDict[sample_id]['R1'].split('/')[-1].split('_')[0].split('-')[0] + "-preprocessing-"
        inputs = job_def["parameters"]
        #inputs["path_read1"] = '/work/projects/SD2E-Community/prod/data/'+archivePath+'/'+sample['R1'].split('/')[-1]
        #inputs["path_read2"] = '/work/projects/SD2E-Community/prod/data/'+archivePath+'/'+sample['R2'].split('/')[-1]
        inputs["path_read1"] = os.path.normpath(dirPath + '/' + sampleDict[sample_id]['R1']).replace('//','/')
        inputs["path_read2"] = os.path.normpath(dirPath + '/' + sampleDict[sample_id]['R2']).replace('//','/')
        inputs["multiple_lanes"] = sampleDict[sample_id]['multiple_lanes']
        #archivePath = archivePath + "/miniaturized_library_prep/"
        job_def.parameters = inputs
        # Need to think about this, dangerous for samples w/ multiple
        # measurements reported ie. mini and normal lib preps
        measurement_id = sample['measurements'][0]['measurement_id']
        archive_patterns = [
           {'level': '1', 'patterns': ['.fastq.gz$']}
        ]

        product_patterns = [
            {'patterns': ['.fastq.gz$'],
            #'derived_using': [
                #inputs["path_filterseqs"]
            #    ],
            'derived_from': [
                inputs["path_read1"],
                inputs["path_read2"]
                ]
            }
        ]

        data = {
            "inputs": [
                inputs["path_read1"],
                inputs["path_read2"]
                ],
            "parameters": {
                k: inputs[k] for k in inputs if k in ('path_filterseqs')
                },
            "experiment_id": experiment_id,
            "sample_id": sample_id
            }

        print("MEASURMENT_ID:")
        pprint.pprint(measurement_id)
        print("DATA:")
        pprint.pprint(data)
        print("ARCHIVE PATTERNS:")
        pprint.pprint(archive_patterns)
        print("PRODUCT PATTERNS:")
        pprint.pprint(product_patterns)

        # mpj = Job(r, measurement_id=measurement_id, data=data,
        #           archive_patterns=archive_patterns,
        #           product_patterns=product_patterns,
        #           setup_archive_path=False)
        # mpj.setup()
        #print("JOB UUID: ", mpj.uuid)
        #r.logger.info("Submitted Pipeline Job {}".format(mpj.uuid))
        #job_def.archivePath = mpj.archive_path
        #job_def.archivePath = 'shared-q1-workshop/urrutia/experiment.ginkgo.23121_Novel-chassis_2.0_DNA-seq_strain_verfication/preprocessed/'
<<<<<<< HEAD
        #
        # notif = [{'event': 'RUNNING',
        #           "persistent": True,
        #           'url': mpj.callback + '&status=${JOB_STATUS}'},
        #          {'event': 'FAILED',
        #           "persistent": False,
        #           'url': mpj.callback + '&status=${JOB_STATUS}'},
        #          {'event': 'FINISHED',
        #           "persistent": False,
        #           'url': mpj.callback + '&status=FINISHED'}]
=======
        try:
            audit_nonce = os.getenv('_REACTOR_AUDIT_RNASEQ_NONCE')
            pipeline_config = copy.copy(r.settings.pipelines)
            audit_rnaseq_id = pipeline_config['audit_rnaseq_id']
            api_server = pipeline_config['api_server']
            audit_rnaseq_callback = api_server + '/actors/v2/' + audit_rnaseq_id + '/messages?x-nonce=' + audit_nonce
        except Exception as e:
            print(e)
            r.logger.error("Unable to generate Audit callback")

        notif = [{'event': 'RUNNING',
                  "persistent": True,
                  'url': mpj.callback + '&status=${JOB_STATUS}'},
                 {'event': 'FAILED',
                  "persistent": False,
                  'url': mpj.callback + '&status=${JOB_STATUS}'},
                 {'event': 'FINISHED',
                  "persistent": False,
                  'url': mpj.callback + '&status=${JOB_STATUS}'},
                  {'event': 'FINISHED',
                   "persistent": False,
                   'url': audit_rnaseq_callback + '&status=${JOB_STATUS}' +
                   '&analysis_type=preprocessing' +
                   '&mpjId=' + mpj.uuid}]
>>>>>>> 420b7e8d67febcc65d6120b869b58c2e468d81ad

        #notifications = job_template["notifications"]
        #if notifications is not None:
        #   for item in notifications:
        #       notif.append(item)

<<<<<<< HEAD
        #job_def["notifications"] = notif
        print(json.dumps(job_def, indent=4))
        # try:
        #     job_id = ag.jobs.submit(body=job_def)['id']
        #     print(json.dumps(job_def, indent=4))
        #     mpj.run({"launched": job_id, "experiment_id": experiment_id, "sample_id": sample_id})
        #     r.logger.info("Submitted Agave job {}".format(job_id))
        # except Exception as e:
        #     print(json.dumps(job_def, indent=4))
        #     r.logger.error("Error submitting job: {}".format(e))
        #     print(e.response.content)
    r.logger.info("Sucessfully Processed: {}".format(manifestPath))
    r.logger.info("Could've Submitted {} Pipeline Jobs ".format(len(rna_list)))
=======
        job_def["notifications"] = notif
        mpj_state = get_job_state(mpj.uuid, ag)
        try:
            if mpj_state in ['CREATED', 'RESET']:
                job_id = ag.jobs.submit(body=job_def)['id']
                print(json.dumps(job_def, indent=4))
                r.logger.info("Submitted Agave job {}".format(job_id))
                mpj.run({"launched": job_id, "sample_id": sample_id, "experiment_id": experiment_id})
            else:
                r.logger.info("Unable to submit job, in state {}".format(mpj.state))
        except Exception as e:
            print(json.dumps(job_def, indent=4))
            r.logger.error("Error submitting job: {}".format(e))
            print(e.response.content)

>>>>>>> 420b7e8d67febcc65d6120b869b58c2e468d81ad

def get_job_state(id, ag):
    from datacatalog.managers.pipelinejobs.jobmanager import JobManager
    mongodb={'authn': os.getenv('_REACTOR_MONGODB_AUTHN'), 'database': 'catalog_staging'}
    mjob = JobManager(mongodb, agave=ag).load(id)
    return mjob.state

def main():
    """Main function"""
    r = Reactor()
    manifest(r)


if __name__ == '__main__':
    main()
