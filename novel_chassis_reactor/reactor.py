from reactors.utils import Reactor, agaveutils
from agavepy.agave import Agave
from datacatalog.managers.pipelinejobs import ReactorManagedPipelineJob as Job
import json
import pprint


def manifest(r):

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

    meta_data = {}

    for sample in manifest['samples']:
        sample_data = {}
        sample_data['timepoint'] = str(sample['measurements'][0]['timepoint']['value']) + ":hours"
        sample_data['strain'] = sample['strain']['label']
        sample_data['temperature'] = sample['temperature']['value']
        sample_data['replicate'] = sample['replicate']
        sample_data['arabinose'] = [content['name']['label'] for content in sample['contents'] if content['name']['label'] == 'Larabinose']
        sample_data['IPTG'] = [content['name']['label'] for content in sample['contents'] if content['name']['label'] == 'IPTG']
        file_name = '/products/v1/58ab5692-9485-525f-a099-202c1a565fd7/850e742e-e67a-5e99-bc04-c60d1eec9a41/f42d3f7f-07dc-596f-86f9-df75083e52cc/large-wasp-20190116T213307Z/preprocessed/' + [file['name'] for file in sample['measurements'][0]['files']][0].split('-')[0]
        sample_data['R1'] = file_name + '-R1_rRNA_free_reads.fastq.gz'
        sample_data['R2'] = file_name + '-R2_rRNA_free_reads.fastq.gz'
        sample_data['measurement_id'] = sample['measurements'][0]['measurement_id']
        meta_data[sample['sample_id']] = sample_data

    job_template = {
      "name": "sample_id",
      "appId": "urrutia-novel_chassis_app-0.1.1",
      "archive": 'true',
      "archiveSystem": "data-sd2e-community",
      "archivePath": "",
      "maxRunTime": "48:00:00",
      "inputs": {
      },
      "parameters": {
        "path_read1": "/work/projects/SD2E-Community/prod/data/uploads/biofab/201809/17016/op_91344/141667-M9-Kan_S3_L001_R1_001.fastq.gz",
        "path_read2": "/work/projects/SD2E-Community/prod/data/uploads/biofab/201809/17016/op_91344/141667-M9-Kan_S3_L001_R2_001.fastq.gz",
        "path_fasta": "/work/projects/SD2E-Community/prod/data/reference/novel_chassis/uma_refs/MG1655_NAND_Circuit/MG1655_NAND_Circuit.fa",
        "path_gff": "/reference/novel_chassis/uma_refs/amin_genes_1.1.0.gff",
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
            arabinose = '25_mM' #str(metadata['arabinose'][0]*1000) + '_mM'
        if not metadata['IPTG']:
            IPTG = "0_mM"
        else:
            IPTG = '0.25_mM' #str(metadata['IPTG'][0]*1000) + '_mM'
        job_template['parameters']['outname'] = sample + '_' + metadata['strain'] + '_replicate_' + str(metadata['replicate']) + '_time_' + metadata['timepoint'] + '_temp_' + str(metadata['temperature']) + '_arabinose_' + arabinose + '_IPTG_' + IPTG
        #print(json.dumps(job_template,indent=4))


        data = {
            "inputs": {
                'path_read1': job_template['parameters']['path_read1'],
                'path_read2': job_template['parameters']['path_read2']
                },
            "parameters": {
                'path_fasta': job_template['parameters']['path_fasta'],
                'path_interval_file': job_template['parameters']['path_interval_file'],
                'path_dict_file': job_template['parameters']['path_dict_file']
                }
            }
        pprint.pprint(r.settings.mongodb)
        pprint.pprint(r.settings.pipelines)
        pprint.pprint(metadata['measurement_id'])
        pprint.pprint(sample)
        pprint.pprint(data)
        pprint.pprint(ag)
        #mpj = Job(r.settings.development.mongodb, r.settings.pipelines, measurement_id=metadata['measurement_id'], sample_id=sample, data = data, agave=ag)
        mpj = Job(r, measurement_id=metadata['measurement_id'], data=data)
        mpj.setup()
        print("JOB UUID: ", mpj.uuid)
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

        #notifications = job_template["notifications"]
        #if notifications is not None:
        #    for item in notifications:
        #        notif.append(item)

        job_template["notifications"] = notif

        try:
            job_id = ag.jobs.submit(body=job_template)['id']
            print(json.dumps(job_template, indent=4))
            mpj.run({"launched": job_id, "sample_id": sample})
            r.logger.info("Submitted Agave job {}".format(job_id))
        except Exception as e:
            print(json.dumps(job_template, indent=4))
            r.logger.error("Error submitting job: {}".format(e))
            print(e.response.content)




def main():
    """Main function"""
    r = Reactor()
    manifest(r)


if __name__ == '__main__':
    main()
