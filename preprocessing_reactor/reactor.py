from reactors.utils import Reactor, agaveutils
from agavepy.agave import Agave
from datacatalog.managers.pipelinejobs import ReactorManagedPipelineJob as Job
import json
import pprint
import copy
from datacatalog.tokens import get_admin_token, validate_token


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

    try:
        manifest = json.load(open(manifestFileName))
    except Exception as e:
        r.on_failure("failed to load manifest {}".format(manifestUrl), e)

    experiment_id = manifest['experiment_id']

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

    for sample in rna_list:
        sample_id = sample['sample_id']
        sampleDict = {}
        filesDict = {}
        sampleDict[sample_id] = filesDict
        files = [file["name"] for measurement in sample["measurements"]
                 for file in measurement["files"]
                 if file["lab_label"] == ["RAW"]]

        for file in files:
            if any(R1 in file for R1 in ["_R1_","_R1.","-R1-","-R1."]):
                readDirection = "R1"
            if any(R2 in file for R2 in ["_R2_","_R2.","-R2-","-R2."]):
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
        inputs["path_read1"] = '/uploads/ginkgo/201901/NovelChassis-NAND-Ecoli-Titration/' + sampleDict[sample_id]['R1']
        inputs["path_read2"] = '/uploads/ginkgo/201901/NovelChassis-NAND-Ecoli-Titration/' + sampleDict[sample_id]['R2']
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
                }
            }

        print("MEASURMENT_ID:")
        pprint.pprint(measurement_id)
        print("DATA:")
        pprint.pprint(data)
        print("ARCHIVE PATTERNS:")
        pprint.pprint(archive_patterns)
        print("PRODUCT PATTERNS:")
        pprint.pprint(product_patterns)

        mpj = Job(r, measurement_id=measurement_id, data=data, archive_patterns=archive_patterns, product_patterns=product_patterns)
        mpj.setup()
        print("JOB UUID: ", mpj.uuid)
        r.logger.info("JOB UUID: ", mpj.uuid)
        job_def.archivePath = mpj.archive_path
        #job_def.archivePath = 'test'

        notif = [{'event': 'RUNNING',
                  "persistent": True,
                  'url': mpj.callback + '&status=${JOB_STATUS}'},
                 {'event': 'FAILED',
                  "persistent": False,
                  'url': mpj.callback + '&status=${JOB_STATUS}'},
                 {'event': 'ARCHIVING_FINISHED',
                  "persistent": False,
                  'url': mpj.callback + '&status=FINISHED'}]

        notifications = job_template["notifications"]
        # if notifications is not None:
        #    for item in notifications:
        #        notif.append(item)
        # akey = copy.copy(r.settings.mongodb.admin_key)
        # print(akey)
        # atoken = get_admin_token(akey)
        # mpj.reset(token=atoken)

        job_def["notifications"] = notif
        print(json.dumps(job_def, indent=4))
        try:
            job_id = ag.jobs.submit(body=job_def)['id']
            print(json.dumps(job_def, indent=4))
            mpj.run({"launched": job_id, "experiment_id": experiment_id, "sample_id": sample_id})
            r.logger.info("Submitted Agave job {}".format(job_id))
        except Exception as e:
            print(json.dumps(job_def, indent=4))
            r.logger.error("Error submitting job: {}".format(e))
            print(e.response.content)




def main():
    """Main function"""
    r = Reactor()
    manifest(r)


if __name__ == '__main__':
    main()
