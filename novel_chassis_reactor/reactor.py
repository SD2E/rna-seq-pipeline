from reactors.utils import Reactor, agaveutils
from agavepy.agave import Agave
from datacatalog.managers.pipelinejobs import ReactorManagedPipelineJob as Job
import json
import pprint
import pymongo

#def manifest(r):
def manifest(r, archive_paths):

    ag = r.client # Agave client
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

    meta_data = {}

    for sample in rna_list:
        sample_data = {}
        sample_id = sample['sample_id']
        sample_data['timepoint'] = str(sample['measurements'][0]['timepoint']['value']) + ":hours"
        sample_data['strain'] = sample['strain']['label']
        sample_data['temperature'] = sample['temperature']['value']
        sample_data['replicate'] = sample['replicate']
        sample_data['arabinose'] = [content['value'] for content in sample['contents'] if content['name']['sbh_uri'] == 'https://hub.sd2e.org/user/sd2e/design/Larabinose/1']
        sample_data['IPTG'] = [content['value'] for content in sample['contents'] if content['name']['sbh_uri'] == 'https://hub.sd2e.org/user/sd2e/design/IPTG/1']
        #file_name = '/products/v1/58ab5692-9485-525f-a099-202c1a565fd7/87bf086e-418d-55e4-b222-ecc668c646a5/f42d3f7f-07dc-596f-86f9-df75083e52cc/wired-hare-20190128T220021Z/preprocessed/' + [file['name'] for file in sample['measurements'][0]['files']][0].split('-')[0]
        #sample_data['R1'] = file_name+"-R1_trimmed.fastq.gz"
        #sample_data['R2'] = file_name+"-R2_trimmed.fastq.gz"
        sample_data['R1'] = archive_paths[sample_id]["trimmed_R1"]
        sample_data['R2'] = archive_paths[sample_id]["trimmed_R2"]
        sample_data['measurement_id'] = sample['measurements'][0]['measurement_id']
        meta_data[sample_id] = sample_data

    job_template = {
      "name": "sample_id",
      "appId": "urrutia-novel_chassis_app-0.1.1",
      "archive": 'true',
      "archiveSystem": "data-sd2e-community",
      "batchQueue": "normal",
      "archivePath": "",
      "maxRunTime": "08:00:00",
      "inputs": {
      },
      "parameters": {
        "path_read1": "/work/projects/SD2E-Community/prod/data/uploads/biofab/201809/17016/op_91344/141667-M9-Kan_S3_L001_R1_001.fastq.gz",
        "path_read2": "/work/projects/SD2E-Community/prod/data/uploads/biofab/201809/17016/op_91344/141667-M9-Kan_S3_L001_R2_001.fastq.gz",
        "path_fasta": "/work/projects/SD2E-Community/prod/data/reference/novel_chassis/uma_refs/MG1655_NAND_Circuit/MG1655_NAND_Circuit.fa",
        "path_gff": "/reference/novel_chassis/uma_refs/amin_genes_no_parts_1.1.0.gff",
        "path_interval_file": "/work/projects/SD2E-Community/prod/data/reference/novel_chassis/uma_refs/MG1655_NAND_Circuit/MG1655_NAND_Circuit.interval_list",
        "path_ref_flat": "/reference/novel_chassis/uma_refs/modified.ecoli.MG1655.refFlat.txt",
        "path_dict_file": "/work/projects/SD2E-Community/prod/data/reference/novel_chassis/uma_refs/MG1655_NAND_Circuit/MG1655_NAND_Circuit.dict",
        "outname": "141667-M9-Kan_MG1655_NAND_Circuit_replicate_4_time_5_hour_NC_E_coli_NAND_37C"
      },
      "notifications": []
    }

    for sample,metadata in meta_data.items():
        job_template['name'] = sample
        job_template['parameters']['path_read1'] = metadata['R1']
        job_template['parameters']['path_read2'] = metadata['R2']
        job_template['parameters']['path_fasta'] = '/reference/novel_chassis/uma_refs/' + metadata['strain'] + '/' + metadata['strain'] + '.fa'
        job_template['parameters']['path_interval_file'] = '/reference/novel_chassis/uma_refs/' + metadata['strain'] + '/' + metadata['strain'] + '.interval_list'
        job_template['parameters']['path_dict_file'] = '/reference/novel_chassis/uma_refs/' + metadata['strain'] + '/' + metadata['strain'] + '.dict'
        if not metadata['arabinose']:
            arabinose = "0_mM"
        else:
            #arabinose = '25_mM'
            arabinose = str(metadata['arabinose'][0]*1000) + '_mM'
        if not metadata['IPTG']:
            IPTG = "0_mM"
        else:
            #IPTG = '0.25_mM'
            IPTG = str(metadata['IPTG'][0]*1000) + '_mM'
        job_template['parameters']['outname'] = sample + '_' + metadata['strain'] + '_replicate_' + str(metadata['replicate']) + '_time_' + metadata['timepoint'] + '_temp_' + str(metadata['temperature']) + '_arabinose_' + arabinose + '_IPTG_' + IPTG
        #print(json.dumps(job_template,indent=4))


        data = {
            "inputs": [
                job_template['parameters']['path_read1'],
                job_template['parameters']['path_read2']
                ],
            "parameters": {
                'path_fasta': job_template['parameters']['path_fasta'],
                'path_interval_file': job_template['parameters']['path_interval_file'],
                'path_dict_file': job_template['parameters']['path_dict_file']
                }
            }
        archive_patterns = [
           {'level': '1', 'patterns': ['.bam$', '.sam$', '.bedgraph$']}
        ]

        product_patterns = [
            {'patterns': ['.bam$', '.sam$', '.bedgraph$'],
            'derived_using': [
                job_template['parameters']['path_fasta'],
                job_template['parameters']['path_interval_file'],
                job_template['parameters']['path_dict_file']
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
        pprint.pprint(metadata['measurement_id'])
        print("sample:")
        pprint.pprint(sample)
        print("data:")
        pprint.pprint(data)
        print("product_patterns:")
        pprint.pprint(product_patterns)
        print("archive_patterns:")
        pprint.pprint(archive_patterns)
        print("job template:")
        pprint.pprint(job_template)

        mpj = Job(r, measurement_id=metadata['measurement_id'], data=data, archive_patterns=archive_patterns, product_patterns=product_patterns)
        mpj.setup()

        r.logger.info("Submitted Pipeline Job {}".format(mpj.uuid))
        archivePath = mpj.archive_path
        job_template["archivePath"] = archivePath

        notif = [{'event': 'RUNNING',
                  "persistent": True,
                  'url': mpj.callback + '&status=${JOB_STATUS}'},
                 {'event': 'FAILED',
                  "persistent": False,
                  'url': mpj.callback + '&status=${JOB_STATUS}'},
                 {'event': 'ARCHIVING_FINISHED',
                  "persistent": False,
                  'url': mpj.callback + '&status=FINISHED'}]



        job_template["notifications"] = notif

        try:
            job_id = ag.jobs.submit(body=job_template)['id']
            print(json.dumps(job_template, indent=4))
            r.logger.info("Submitted Agave job {}".format(job_id))
            mpj.run({"launched": job_id, "sample_id": sample, "experiment_id": experiment_id})
        except Exception as e:
            print(json.dumps(job_template, indent=4))
            r.logger.error("Error submitting job: {}".format(e))
            #print(e.response.content)
            print(e)

def mongo_query(r):
    dbURI = '***REMOVED***'
    client = pymongo.MongoClient(dbURI)
    db = client.catalog_staging
    #filesdb = db.files
    jobs = db.jobs

    query={}
    #editing manually for now, in future will probably work of exp_id
    query['archive_path'] = {'$regex': '/products/v2/106d3f7f07dc596f86f9df75083e52cc'}
    #query['archive_path'] = {'$regex': '/products/v1/58ab5692-9485-525f-a099-202c1a565fd7/87bf086e-418d-55e4-b222-ecc668c646a5/f42d3f7f-07dc-596f-86f9-df75083e52cc/wired-hare-20190128T220021Z/preprocessed/'}
    results = []
    #for match in filesdb.find(query):
    for match in jobs.find(query):
        results.append(match)

    archive_paths = {}
    good_list = []
    fail_list = []
    for sample in results:
        try:
            sample_id = [data['data'] for data in sample['history'] if data['name'] == 'run'][0]['sample_id']
            #archive_paths[sample_id] = {"archive_path": sample['archive_path']}
            #archive_paths[sample_id]['inputs'] = sample['data']['inputs']
            path_list = sample['data']['inputs']
            r1 = path_list[0]
            r2 = path_list[1]
            #file_name = r1.split("/")[-1].split('-')[0]
            file_name = r1.split("/")[-1].split('R1')[0]
            archive_path = sample['archive_path'] + '/'
            archive_paths[sample_id] = {"raw_R1": r1}
            archive_paths[sample_id]["raw_R2"] = r2
            #archive_paths[sample_id]["trimmed_R1"] = archive_path+file_name+"-R1_trimmed.fastq.gz"
            #archive_paths[sample_id]["trimmed_R2"] = archive_path+file_name+"-R2_trimmed.fastq.gz"
            archive_paths[sample_id]["trimmed_R1"] = archive_path+file_name+"R1_001_trimmed.fastq.gz"
            archive_paths[sample_id]["trimmed_R2"] = archive_path+file_name+"R2_001_trimmed.fastq.gz"
            archive_paths[sample_id]['job_id'] = sample['uuid']
            good_list.append(sample)
        except Exception as e:
            print(e)
            fail_list.append(sample)

    manifest(r,archive_paths)


def main():
    """Main function"""
    r = Reactor()
    #manifest(r)
    mongo_query(r)


if __name__ == '__main__':
    main()
