from reactors.utils import Reactor, agaveutils
from agavepy.agave import Agave
from datacatalog.managers.pipelinejobs import ReactorManagedPipelineJob as Job
import json
import pprint
import pymongo
import copy
import os


def alignment(r):
    ag = r.client # Agave client
    context = r.context  # Actor context
    m = context.message_dict

    experiment_id = m['experiment_id']
    sample_id = m['sample_id']
    measurement_id = m['measurement_id']
    files = m['file_names']
    for file in files:
        if any(R1 in file for R1 in ["_R1_","_R1.","-R1-","-R1."]):
            R1 = file
        elif any(R2 in file for R2 in ["_R2_","_R2.","-R2-","-R2."]):
            R2 = file
        else:
            r.logger.error("Could not determine R1/R2 readmates")
    strain = strain_query(sample_id)

    job_template = copy.copy(r.settings.bwaJob)

    #for sample,metadata in meta_data.items():
    job_template['name'] = sample_id
    job_template['parameters']['path_read1'] = R1
    job_template['parameters']['path_read2'] = R2
    job_template['parameters']['path_fasta'] = '/reference/novel_chassis/b_subtilis/' + strain + '/' + strain + '.fa'
    job_template['parameters']['path_interval_file'] = '/reference/novel_chassis/b_subtilis/' + strain + '/' + strain + '.interval_list'
    job_template['parameters']['path_dict_file'] = '/reference/novel_chassis/b_subtilis/' + strain + '/' + strain + '.dict'
    job_template['parameters']['outname'] = sample_id + '_' + strain


    data = {
        "inputs": [
            job_template['parameters']['path_read1'],
            job_template['parameters']['path_read2']
            ],
        "parameters": {
            'path_fasta': job_template['parameters']['path_fasta'],
            'path_interval_file': job_template['parameters']['path_interval_file'],
            'path_dict_file': job_template['parameters']['path_dict_file']
            },
        "sample_id": sample_id,
        "experiment_id": experiment_id
        }
    archive_patterns = [
       {'level': '1', 'patterns': ['.bam$', '.sam$', '.bedgraph$']}
    ]

    product_patterns = [
        {'patterns': ['.bam$', '.sam$', '.bedgraph$'],
        'derived_using': [
            job_template['parameters']['path_fasta']
            ],
        'derived_from': [
            job_template['parameters']['path_read1'],
            job_template['parameters']['path_read2']
            ]
        }
    ]
    #pprint.pprint(r.settings.mongodb)
    #pprint.pprint(r.settings.pipelines)
    print("mesasurment_id:")
    pprint.pprint(measurement_id)
    print("sample:")
    pprint.pprint(sample_id)
    print("data:")
    pprint.pprint(data)
    print("product_patterns:")
    pprint.pprint(product_patterns)
    print("archive_patterns:")
    pprint.pprint(archive_patterns)
    print("job template:")
    pprint.pprint(job_template)

    mpj = Job(r, measurement_id=measurement_id, data=data,
              archive_patterns=archive_patterns,
              product_patterns=product_patterns,
              setup_archive_path=False)
    mpj.setup()

    r.logger.info("Submitted Pipeline Job {}".format(mpj.uuid))
    archivePath = mpj.archive_path
    job_template["archivePath"] = archivePath
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
              'url': mpj.callback + '&status=FINISHED'},
             {'event': 'FINISHED',
              "persistent": False,
              'url': audit_rnaseq_callback + '&status=FINISHED' +
              '&analysis_type=alignment' +
              '&mpjId=' + mpj.uuid}]

    job_template["notifications"] = notif
    mpj_state = get_job_state(mpj.uuid, ag)
    try:
        if mpj_state in ['CREATED', 'RUNNING', 'RESET']:
            job_id = ag.jobs.submit(body=job_template)['id']
            print(json.dumps(job_template, indent=4))
            r.logger.info("Submitted Agave job {}".format(job_id))
            mpj.run({"launched": job_id, "sample_id": sample_id, "experiment_id": experiment_id})
        else:
            r.logger.info("Unable to submit job, in state {}".format(mpj_state))
    except Exception as e:
        print(json.dumps(job_template, indent=4))
        r.logger.error("Error submitting job: {}".format(e))
        if e.response.content is not None:
            print(e.response.content)
        else:
            print(e)

# Returns state of pipeline job
def get_job_state(mpjId, ag):
    from datacatalog.managers.pipelinejobs.jobmanager import JobManager
    mongodb={'authn': os.getenv('_REACTOR_MONGODB_AUTHN'), 'database': 'catalog_staging'}
    mjob = JobManager(mongodb, agave=ag).load(mpjId)
    return mjob.state

# Takes sample id and retuns strain
def strain_query(sample_id):
    dbURI = '***REMOVED***'
    client = pymongo.MongoClient(dbURI)
    db = client.catalog_staging
    samples = db.samples
    query={}
    query['sample_id'] = sample_id
    results = []
    for match in samples.find(query):
        results.append(match)
    if len(results) > 1:
        r.logger.error("More than one sample result found")
    if len(results) == 0:
        r.logger.error("No results found for sample: {}".format(sample_id))
    strain = '_'.join(results[0]['strain']['label'].split(' '))

    return strain


def main():
    """Main function"""
    global r
    r = Reactor()
    alignment(r)

# body={
#     'experiment_id': experiment_id,
#     'sample_id': sample_id,
#     'measurement_id': measurement_id,
#     'file_names': file_names
#     }
# )
if __name__ == '__main__':
    main()
