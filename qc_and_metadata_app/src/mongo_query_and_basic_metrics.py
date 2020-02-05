# import required modules
import csv
import pprint
import json
import os
import pandas as pd
import subprocess
import signal
import pymongo
import zipfile
import glob
import numpy as np
import sys
from qc_from_raw_counts import *

def mongo_query(experiment_id):
    # We're going to query off the staging version of the jobs_table
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

    # We'll create seperate lists for each type of job
    preprocessing_jobs = {}
    alignment_jobs = {}
    dataframe_jobs = {}

    for job in jobs_table.find(query):
        # sort based on pipeline uuid,
        # could also use appId, which is nested in history.data
        # TODO: add to top-level data in the future
        # Brittle and positional,
        # TODO: surface sample_id and outname in top-level 'data'
        try:
            sample_id = [entry['data'] for entry in job['history'] if entry['name'] == 'run'][0]['sample_id']
        except Exception as e:
            experiment_id = [entry['data'] for entry in job['history'] if entry['name'] == 'run'][0]['experiment_id']
        if job["pipeline_uuid"] == "106d3f7f-07dc-596f-86f9-df75083e52cc":
            preprocessing_jobs[sample_id] = job
        if job["pipeline_uuid"] == "106bd127-e2d2-57ac-b9be-11ed06042e68":
            alignment_jobs[sample_id] = job
        if job["pipeline_uuid"] == "106231a1-0c78-5067-b53b-11a33f4e1495":
            dataframe_jobs[experiment_id] = job

    # Ok, so we need a few different pieces of info here:
    # experimental metadata (query data_catalog for raw files)
    # Q30% from FastQC (nested in a zipped .txt file)
    # number reads before mapping (probably also use FastQC results for this)
    # and mapped reads (bwa log?)

    # create a list of input R1s so we can query data catalog for metadata
    raw_inputs = []
    raw_inputs = [job['data']['inputs'][0] for sample,job in preprocessing_jobs.items()]

    query = {}
    query['filename'] = {'$in': raw_inputs}

    # set our query db to the science table
    science_table = db.science_table
    metadata_query_results = []
    for file_metadata in science_table.find(query):
        metadata_query_results.append(file_metadata)

    return metadata_query_results, preprocessing_jobs, alignment_jobs, dataframe_jobs




def metadata_construction(metadata_query_results):
    # Create an empty meta_data dictonary,
    # key will be sample_id, the value will be
    # another dictonary with metadata_type: value
    meta_data = {}
    for sample in metadata_query_results:
        metadata = {}
        metadata["Timepoint"] = sample['timepoint']['value']
        metadata["Strain"] = sample['strain']
        metadata["Temperature"] = sample["temperature"]["value"]
        #metadata["Temperature"] = 37
        metadata["Replicate"] = sample['replicate']
        # This part is a little messy, lots of room for improvement here.
        # Different labs use different names for arabinose/IPTG
        # but the synbiohub uri's are the same, so we use those
        #
        # Also some labs/experiments report concentrations,
        # but others don't so I created a try/except
        # to catch the concentration where available, and report
        # presence/absense otherwise

        # first I use list comprehension to pull out sample_contents for arabinose
        # if there is no arabinose present this will return an empty list
        arabinose_content = [content for content in sample['sample_contents']
                    if content['name']['sbh_uri'] ==
                    'https://hub.sd2e.org/user/sd2e/design/Larabinose/1']
        # If the list is empty, then we know no arabinose
        if not arabinose_content:
            arabinose = 0
            # arabinose_unit = 'NA'
            arabinose_unit = 'M'
        # If the list has contents we try to pull out concentration/unit,
        # or explictly define concentration/unit if it's not available
        if arabinose_content:
            try:
                arabinose = arabinose_content[0]['value']
                arabinose_unit = arabinose_content[0]['unit']
            except Exception as e:
                # Bit of a cheat here, if concentration/unit is not available,
                # then I hard code it here
                print("No arabinose concentration available for ", sample['sample_id'])
                # arabinose = 'Present'
                #arabinose = 0.025
                arabinose = 'NA'
                arabinose_unit = 'NA'
        # I use the same approcah for IPTG concentration/units
        IPTG_content = [content for content in sample['sample_contents']
                    if content['name']['sbh_uri'] ==
                    'https://hub.sd2e.org/user/sd2e/design/IPTG/1']
        if not IPTG_content:
            IPTG = 0
            # IPTG_unit = 'NA'
            IPTG_unit = 'M'
        if IPTG_content:
            try:
                IPTG = IPTG_content[0]['value']
                IPTG_unit = IPTG_content[0]['unit']
            except Exception as e:
                print("No IPTG concentration available for ", sample['sample_id'])
                # IPTG = "Present"
                #IPTG = 0.00025
                IPTG = 'NA'
                IPTG_unit = 'NA'
        Vanillic_acid_content = [content for content in sample['sample_contents']
                    if content['name']['sbh_uri'] ==
                    'https://hub.sd2e.org/user/sd2e/design/Vanillic0x20Acid/1']
        if not Vanillic_acid_content:
            Vanillic_acid = 0
            # IPTG_unit = 'NA'
            Vanillic_acid_unit = 'M'
        if Vanillic_acid_content:
            try:
                Vanillic_acid = Vanillic_acid_content[0]['value']
                Vanillic_acid_unit = Vanillic_acid_content[0]['unit']
            except Exception as e:
                print("No Vanillic_acid concentration available for ", sample['sample_id'])
                # IPTG = "Present"
                #IPTG = 0.00025
                Vanillic_acid = 'NA'
                Vanillic_acid_unit = 'NA'

        Cuminic_acid_content = [content for content in sample['sample_contents']
                    if content['name']['sbh_uri'] ==
                    'https://hub.sd2e.org/user/sd2e/design/Cuminic0x20Acid/1']
        if not Cuminic_acid_content:
            Cuminic_acid = 0
            # Cuminic_acid_unit = 'NA'
            Cuminic_acid_unit = 'M'
        if Cuminic_acid_content:
            try:
                Cuminic_acid = Cuminic_acid_content[0]['value']
                Cuminic_acid_unit = Cuminic_acid_content[0]['unit']
            except Exception as e:
                print("No Cuminic Acid concentration available for ", sample['sample_id'])
                # IPTG = "Present"
                #IPTG = 0.00025
                Cuminic_acid = 'NA'
                Cuminic_acid_unit = 'NA'

        Xylose_content = [content for content in sample['sample_contents']
                    if content['name']['sbh_uri'] ==
                    'https://hub.sd2e.org/user/sd2e/design/Xylose/1']
        if not Xylose_content:
            Xylose = 0
            # Xylose_unit = 'NA'
            Xylose_unit = 'M'
        if Xylose_content:
            try:
                Xylose = Xylose_content[0]['value']
                Xylose_unit = Xylose_content[0]['unit']
            except Exception as e:
                print("No Xylose concentration available for ", sample['sample_id'])
                # Xylose = "Present"
                #Xylose = 0.00025
                Xylose = 'NA'
                Xylose_unit = 'NA'

        Dextrose_content = [content for content in sample['sample_contents']
                    if content['name']['sbh_uri'] ==
                    'https://hub.sd2e.org/user/sd2e/design/sigma_G8270/1']
        if not Dextrose_content:
            Dextrose = 0
            # Dextrose_unit = 'NA'
            Dextrose_unit = 'M'
        if Dextrose_content:
            try:
                Dextrose = Dextrose_content[0]['value']
                Dextrose_unit = Dextrose_content[0]['unit']
            except Exception as e:
                print("No Dextrose concentration available for ", sample['sample_id'])
                # Dextrose = "Present"
                #Dextrose = 0.00025
                Dextrose = 'NA'
                Dextrose_unit = 'NA'
        metadata['Arabinose'] = arabinose
        metadata['Arabinose_unit'] = arabinose_unit
        metadata['Cuminic_acid'] = Cuminic_acid
        metadata['Cuminic_acid_unit'] = Cuminic_acid_unit
        metadata['Vanillic_acid'] = Vanillic_acid
        metadata['Vanillic_acid_unit'] = Vanillic_acid_unit
        metadata['Xylose'] = Xylose
        metadata['Xylose_unit'] = Xylose_unit
        metadata['Dextrose'] = Dextrose
        metadata['Dextrose_unit'] = Dextrose_unit
        metadata['IPTG'] = IPTG
        metadata['IPTG_unit'] = IPTG_unit
        # Another try/except to capture input state where available
        try:
            metadata['Strain_input_state'] = sample["strain_input_state"]
        except Exception as e:
            metadata['Strain_input_state'] = 'NA'

        try:
            metadata['QC_total_RNA_conc_ng_ul_NUM'] = sample['ginkgo_rnaseq_metadata']['total_RNA_conc_ng_ul']
        except Exception as e:
            metadata['QC_total_RNA_conc_ng_ul_NUM'] = 'NA'
        try:
            metadata['QC_volume_for_pooling_NUM'] = sample['ginkgo_rnaseq_metadata']['volume_for_pooling']
        except Exception as e:
            metadata['QC_volume_for_pooling_NUM'] ='NA'
        try:
            metadata['QC_input_rRNA_depletion_ng_NUM'] = sample['ginkgo_rnaseq_metadata']['input_rRNA_depletion_ng']
        except Exception as e:
            metadata['QC_input_rRNA_depletion_ng_NUM'] = 'NA'
        try:
            metadata['QC_library_conc_nM_NUM'] = sample['ginkgo_rnaseq_metadata']['library_conc_nM']
        except Exception as e:
            metadata['QC_library_conc_nM_NUM'] = 'NA'
        try:
            metadata['QC_H2O_NUM'] = sample['ginkgo_rnaseq_metadata']['H2O']
        except Exception as e:
            metadata['QC_H2O_NUM'] = 'NA'
        try:
            metadata['QC_RQN_NUM'] = sample['ginkgo_rnaseq_metadata']['RQN']
        except Exception as e:
            metadata['QC_RQN_NUM'] = 'NA'
        try:
            metadata['QC_input_rRNA_depletion_ng_uL_NUM'] = sample['ginkgo_rnaseq_metadata']['input_rRNA_depletion_ng_uL']
        except Exception as e:
            metadata['QC_input_rRNA_depletion_ng_uL_NUM'] ='NA'
        try:
            metadata['QC_input_rRNA_depletion_uL_NUM'] = sample['ginkgo_rnaseq_metadata']['input_rRNA_depletion_uL']
        except Exception as e:
            metadata['QC_input_rRNA_depletion_uL_NUM'] = 'NA'

        meta_data[sample['sample_id']] = metadata
    return meta_data



def crawl_file_system(prefix, meta_data, preprocessing_jobs, alignment_jobs):
    # This is sub-optimal, every time we run we have to run through all these
    # output files just to get basic stats.
    # TODO: run multiqc before this and just pull the ONE multiqc job
    # TODO: annotate these w/ tags in the data catalog so we don't re-run what
    # we alredy have
    # Now that we've created our meta_data dictonary
    # we can pull Q30% from FastQC (nested in a zipped .txt file)
    # number reads before mapping (probably also use FastQC results for this)
    # and mapped reads (bwa log?) from the relevant output files
    # and add this to our meta_data dictionary
    for sample_id, job in preprocessing_jobs.items():
        #print(sample_id)
        #prefix = '/home/jupyter/sd2e-community/'
        #prefix = '/work/projects/SD2E-Community/prod/data/'
        fastqc_r1 = prefix + job['archive_path'] + '/' + job['data']['inputs'][0].split('/')[-1].split('_R')[0] + '_R1_001_trimmed_fastqc.zip'
        fastqc_r2 = prefix + job['archive_path'] + '/' + job['data']['inputs'][0].split('/')[-1].split('_R')[0] + '_R2_001_trimmed_fastqc.zip'
        command = "zipgrep 'Total Sequences' " + fastqc_r1 + " | grep 'fastqc_data.txt'"
        try:
            output = subprocess.check_output(command, shell=True, preexec_fn=lambda: signal.signal(signal.SIGPIPE, signal.SIG_DFL))
        except Exception as e:
            output = 'NA'
        total_counts_r1 = str(output).split('\\t')[-1].split('\\n')[0]
        command = "zipgrep 'Total Sequences' " + fastqc_r2 + " | grep 'fastqc_data.txt'"
        try:
            output = subprocess.check_output(command, shell=True, preexec_fn=lambda: signal.signal(signal.SIGPIPE, signal.SIG_DFL))
        except Exception as e:
            output = 'NA'
        total_counts_r2 = str(output).split('\\t')[-1].split('\\n')[0]
        #print(total_counts_r1, total_counts_r2)

        # Brittle and positional,
        # TODO: surface sample_id and outname in top-level 'data'
        #sample_id = [entry['data'] for entry in job['history'] if entry['name'] == 'run'][0]['sample_id']
        #alignment = [alignment_job in alignment_jobs if alignment_job['sample_id'] == sample_id]
        alignment = alignment_jobs[sample_id]
        #print(alignment)
        #outname = [entry['data'] for entry in alignment['history'] if entry['name'] == 'run'][1]['parameters']['outname']
        try:
            flagstat = glob.glob(prefix+alignment_jobs[sample_id]['archive_path']+'/*.rnaseq.original.bwa.flagstat.txt')[0]
            #flagstat = prefix + alignment['archive_path'] + '/' + outname + '.rnaseq.original.bwa.flagstat.txt'
            #print(flagstat)
            command = "grep 'mapped (' " + flagstat
            output = subprocess.check_output(command, shell=True, preexec_fn=lambda: signal.signal(signal.SIGPIPE, signal.SIG_DFL))
            percent_mapped = str(output).split('(')[-1].split('%')[0]
            percent_mapped = float(percent_mapped)/100
            number_mapped = str(output).split(' mapped')[0].split('b\'')[-1]
            if float(number_mapped) >= 500000:
                bool_mapped = "True"
            else:
                bool_mapped = "False"
        except Exception as e:
            print("Missing alginment stats for sample ", sample_id)
            print(e)
            percent_mapped = 'NA'
            number_mapped = 'NA'
            bool_mapped = 'False'
        #print(number_mapped, percent_mapped)

        ## Now we need to unzip those fastqc folders,
        ## pull out the Quality scores section
        ## and calculate %Q30 from that report
        # Unzip the fastqc report to your current wd
        try:
            zip_ref = zipfile.ZipFile(fastqc_r1, 'r')
            zip_ref.extractall('.')
            zip_ref.close()
        except Exception as e:
            print("couldn't unzip: ", fastqc_r1)
        # pull quality scores from the fastqc_data.txt report
        command = "sed -n '1,/Quality/d;/END_MODULE/q;p' " + fastqc_r1.split('/')[-1].split('.')[0] + "/fastqc_data.txt"
        try:
            output = subprocess.check_output(command, shell=True, preexec_fn=lambda: signal.signal(signal.SIGPIPE, signal.SIG_DFL))
            os.system("rm -rf " + fastqc_r1.split('/')[-1].split('.')[0])
             #delete the local unzipped folder
            # Convert from bytes output to string
            string_output = output.decode('ascii')
            # split string output into list of lists based on tabs/newlines
            list_output = [quality.split('\t') for quality in string_output.split('\n')]
            # convert the list of lists into a datframe
            df = pd.DataFrame(list_output, columns = ["quality", "num_reads"], dtype = np.float64)
            #explictly make quality scores numeric
            df['quality'] = pd.to_numeric(df['quality'])
            # sum the total reads
            total_reads = df.sum()['num_reads']
            # sum the reads w/ a score over 30
            count_over_30 = df.loc[df['quality'] >= 30].sum()['num_reads']
            # simple caluclation to get %q30
            r1_q30 = count_over_30/total_reads
            #print(r1_q30)
        except Exception as e:
            r1_q30 = 'NA'


        #now we'll just repeat with R2
        # Unzip the fastqc report to your current wd
        try:
            zip_ref = zipfile.ZipFile(fastqc_r2, 'r')
            zip_ref.extractall('.')
            zip_ref.close()
        except Exception as e:
            continue
        # pull quality scores from the fastqc_data.txt report
        command = "sed -n '1,/Quality/d;/END_MODULE/q;p' " + fastqc_r2.split('/')[-1].split('.')[0] + "/fastqc_data.txt"
        try:
            output = subprocess.check_output(command, shell=True, preexec_fn=lambda: signal.signal(signal.SIGPIPE, signal.SIG_DFL))
        #delete the local unzipped folder
            os.system("rm -rf " + fastqc_r2.split('/')[-1].split('.')[0])
            # Convert from bytes output to string
            string_output = output.decode('ascii')
            # split string output based on tabs/newlines
            list_output = [quality.split('\t') for quality in string_output.split('\n')]
             # convert the list into a datframe
            df = pd.DataFrame(list_output, columns = ["quality", "num_reads"], dtype = np.float64)
            # explictly make quality scores numeric
            df['quality'] = pd.to_numeric(df['quality'])
            # sum the total reads
            total_reads = df.sum()['num_reads']
            # sum the reads w/ a score over 30
            count_over_30 = df.loc[df['quality'] >= 30].sum()['num_reads']
            # simple caluclation to get %q30
            r2_q30 = count_over_30/total_reads
        except Exception as e:
            r2_q30 = 'NA'

        # Write out to metadata json
        meta_data[sample_id]['QC_trimmed_reads_R1_NUM'] = total_counts_r1
        meta_data[sample_id]['QC_trimmed_reads_R2_NUM'] = total_counts_r2
        meta_data[sample_id]['QC_%mapped_reads_NUM'] = percent_mapped
        meta_data[sample_id]['QC_mapped_reads_NUM'] = number_mapped
        meta_data[sample_id]['QC_mapped_reads_BOOL'] = bool_mapped
        meta_data[sample_id]['QC_%Q30_R1_NUM'] = r1_q30
        meta_data[sample_id]['QC_%Q30_R2_NUM'] = r2_q30

    return meta_data


def write_to_csv(meta_data, experiment_id):
    #meta_data2 = {k:v for k,v in meta_data.items() if k in al_list}
    #with open("ss_experiment.ginkgo.19606.19637.19708.19709_run_metadata.csv", 'w', newline='') as myfile:
    with open(experiment_id +'_QC_and_metadata.csv', 'w', newline='') as myfile:
        wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
        # just use the metadata keys from the first dictonary object as column names
        wr.writerow(['sample_id'] + list(meta_data[list(meta_data.keys())[0]].keys()))
        # the Key will be sample ID and the value will be another dictionary of meta_data_type:value
        #for sample_id,sample_metadata in meta_data.items():
        for sample_id,sample_metadata in meta_data.items():
            # create a list to store meta_data values
            value_list =[]
            # for each meta_data_type:value, we append the value to our value list
            for data_type,value in sample_metadata.items():
                value_list.append(str(value))
            # now we just add our sample_id (key) to the list of values
            # and that will be a row we write into our csv
            csv_row = []
            csv_row = [sample_id] + value_list
            wr.writerow(csv_row)
    return


def write_dataframes(experiment_id, prefix, dataframe_jobs, qc_metadata, filename):
    counts = pd.read_csv(prefix +
                         dataframe_jobs[experiment_id]['archive_path'] +
                         '/' + filename, sep='\t')
    counts.set_index('gene_id', inplace=True)
    counts = pd.merge(qc_metadata, counts,
                              on=list(qc_metadata.columns),
                              how='outer',
                              left_index=True,
                              right_index=True).T
    counts.T.to_csv(experiment_id + '_' + filename.split('.')[0] + '.csv')
    counts.to_csv(experiment_id + '_' + filename.split('.')[0] + '_transposed.csv')
    return

def main(experiment_id):
    # """Main function"""
    # r = Reactor()
    # ag = r.client # Agave client
    # context = r.context  # Actor context
    # m = context.message_dict
    # experiment_id = m.get('experiment_id')

    # HPC_filesystem_prefix
    prefix = '/work/projects/SD2E-Community/prod/data/'

    # Run a mongo query on the jobs table to get job metadata
    (metadata_query_results, preprocessing_jobs,
        alignment_jobs, dataframe_jobs) = mongo_query(experiment_id)
    # Construct meta_data dictionary from query results
    meta_data = metadata_construction(metadata_query_results)
    # Add QC info to meta_data dict, reading job output files for this
    meta_data = crawl_file_system(prefix, meta_data, preprocessing_jobs,
                                  alignment_jobs)
    # Convert dictionary to a dataframe, easiest to just write/read to csv
    write_to_csv(meta_data, experiment_id)
    df_metadata = pd.read_csv(experiment_id + '_QC_and_metadata.csv')
    # Get metadata factors from the dict (temp/time/etc)
    factors = [metadata_key for metadata_key
               in meta_data[list(meta_data.keys())[0]]
               if metadata_key.split("_")[0] != 'QC'
               and metadata_key not in ['Replicate', 'Arabinose_unit',
                                        'IPTG_unit', 'Strain_input_state',
                                        'Vanillic_acid_unit', 'Dextrose_unit',
                                        'Cuminic_acid_unit', 'Xylose_unit']
               and all(value[metadata_key] == 0 for value in meta_data.values()) == False
               and all(value[metadata_key] == 'NA' for value in meta_data.values()) == False]

    print("Metadata factors for replicate groupings: ", factors)
    # Read in the raw counts file to run the coorelations
    df_counts = pd.read_csv(prefix +
                            dataframe_jobs[experiment_id]['archive_path'] +
                            '/ReadCountMatrix_preCAD.tsv', sep='\t')
    # Run correlation to get between sample correlations
    (qc_metadata, raw_counts) = sample_coors(factors, df_counts, df_metadata)
    # Write out QC and Metadata dataframe
    qc_metadata.to_csv(experiment_id + '_QC_and_metadata.csv')
    # Write out counts w/ qc/metadata appended, and the transposed versions
    write_dataframes(experiment_id, prefix, dataframe_jobs, qc_metadata,
                     'ReadCountMatrix_preCAD.tsv')
    write_dataframes(experiment_id, prefix, dataframe_jobs, qc_metadata,
                     'ReadCountMatrix_preCAD_FPKM.tsv')
    write_dataframes(experiment_id, prefix, dataframe_jobs, qc_metadata,
                     'ReadCountMatrix_preCAD_TPM.tsv')

    return


if __name__ == '__main__':
    main(sys.argv[1])
