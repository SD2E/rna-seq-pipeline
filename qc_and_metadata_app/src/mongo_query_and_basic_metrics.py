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
import math
from collections import Counter

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
        metadata["timepoint"] = sample['timepoint']['value']
        metadata["strain"] = sample['strain']
        metadata["temperature"] = sample["temperature"]["value"]
        metadata["replicate"] = sample['replicate']
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
                arabinose = 0.025
                arabinose_unit = 'M'
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
                IPTG = 0.00025
                IPTG_unit = 'M'
        metadata['arabinose'] = arabinose
        metadata['arabinose_unit'] = arabinose_unit
        metadata['IPTG'] = IPTG
        metadata['IPTG_unit'] = IPTG_unit
        # Another try/except to capture input state where available
        try:
            metadata['strain_input_state'] = sample["strain_input_state"]
        except Exception as e:
            metadata['strain_input_state'] = 'NA'

        metadata['QC_total_RNA_conc_ng_ul_NUM'] = sample['ginkgo_rnaseq_metadata']['total_RNA_conc_ng_ul']
        metadata['QC_volume_for_pooling_NUM'] = sample['ginkgo_rnaseq_metadata']['volume_for_pooling']
        metadata['QC_input_rRNA_depletion_ng_NUM'] = sample['ginkgo_rnaseq_metadata']['input_rRNA_depletion_ng']
        metadata['QC_library_conc_nM_NUM'] = sample['ginkgo_rnaseq_metadata']['library_conc_nM']
        metadata['QC_H2O_NUM'] = sample['ginkgo_rnaseq_metadata']['H2O']
        metadata['QC_RQN_NUM'] = sample['ginkgo_rnaseq_metadata']['RQN']
        metadata['QC_input_rRNA_depletion_ng_uL_NUM'] = sample['ginkgo_rnaseq_metadata']['input_rRNA_depletion_ng_uL']
        metadata['QC_input_rRNA_depletion_uL_NUM'] = sample['ginkgo_rnaseq_metadata']['input_rRNA_depletion_uL']

        meta_data[sample['sample_id']] = metadata
    return meta_data



def crawl_file_system(prefix, meta_data, preprocessing_jobs, alignment_jobs):
    # Now that we've created our meta_data dictonary
    # we can pull Q30% from FastQC (nested in a zipped .txt file)
    # number reads before mapping (probably also use FastQC results for this)
    # and mapped reads (bwa log?) from the relevant output files
    # and add this to our meta_data dictionary
    for sample_id,job in preprocessing_jobs.items():
        #print(sample_id)
        #prefix = '/home/jupyter/sd2e-community/'
        #prefix = '/work/projects/SD2E-Community/prod/data/'
        fastqc_r1 = prefix + job['archive_path'] + '/' + job['data']['inputs'][0].split('/')[-1].split('_R')[0] + '_R1_001_trimmed_fastqc.zip'
        fastqc_r2 = prefix + job['archive_path'] + '/' + job['data']['inputs'][0].split('/')[-1].split('_R')[0] + '_R2_001_trimmed_fastqc.zip'
        command = "zipgrep 'Total Sequences' " + fastqc_r1 + " | grep 'fastqc_data.txt'"
        output = subprocess.check_output(command, shell=True, preexec_fn=lambda: signal.signal(signal.SIGPIPE, signal.SIG_DFL))
        total_counts_r1 = str(output).split('\\t')[-1].split('\\n')[0]
        command = "zipgrep 'Total Sequences' " + fastqc_r2 + " | grep 'fastqc_data.txt'"
        output = subprocess.check_output(command, shell=True, preexec_fn=lambda: signal.signal(signal.SIGPIPE, signal.SIG_DFL))
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
        zip_ref = zipfile.ZipFile(fastqc_r1, 'r')
        zip_ref.extractall('.')
        zip_ref.close()
        # pull quality scores from the fastqc_data.txt report
        command = "sed -n '1,/Quality/d;/END_MODULE/q;p' " + fastqc_r1.split('/')[-1].split('.')[0] + "/fastqc_data.txt"
        output = subprocess.check_output(command, shell=True, preexec_fn=lambda: signal.signal(signal.SIGPIPE, signal.SIG_DFL))
        #delete the local unzipped folder
        os.system("rm -rf " + fastqc_r1.split('/')[-1].split('.')[0])
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

        #now we'll just repeat with R2
        # Unzip the fastqc report to your current wd
        zip_ref = zipfile.ZipFile(fastqc_r2, 'r')
        zip_ref.extractall('.')
        zip_ref.close()
        # pull quality scores from the fastqc_data.txt report
        command = "sed -n '1,/Quality/d;/END_MODULE/q;p' " + fastqc_r2.split('/')[-1].split('.')[0] + "/fastqc_data.txt"
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
        #print(r2_q30)
        # Delete unzipped fastqc reports so they don't clog up WD
        os.system("rm -rf " + fastqc_r1.split('/')[-1].split('.')[0])
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



def tag_low_mapped_reads(dataframe, genes, nmapped=5e5):
    """
    Introduce a NMap QC flag.
    This is not sensitive to prior QC steps so we dont need to filter out BAD flagged
    samples prior to assessment.
    """
    if 'QC_nmap_BOOL' not in list(dataframe.columns):
        dataframe['QC_nmap_BOOL'] = False

    dataframe_tagged = dataframe[dataframe[genes].sum(axis=1) > nmapped]
    dataframe['QC_nmap_BOOL'].loc[dataframe_tagged.index] = True
    bad_indexes = [x for x in dataframe.index if x not in dataframe_tagged.index]
    dataframe['QC_nmap_BOOL'].loc[bad_indexes] = False
    dataframe['QC_nmap_NUM'] = dataframe[genes].sum(axis=1)
    return dataframe


def tag_low_correlation_biological_replicates(dataframe, genes, factors, cc=0.8):
    """
    Introduce an in-group correlation QC flag.
    This QC checks correlation between samples so previously tagged bad samples
    should be removed prior to assessment.
    """
    if 'QC_gcorr_BOOL' not in list(dataframe.columns):
        dataframe['QC_gcorr_BOOL'] = False

    # Collect previous QC flags if they exist
    bad_index = []
    qc_cols = [x for x in dataframe.columns if 'QC' in x if x != 'QC_gcorr_BOOL']
    print(dataframe.shape)
    if qc_cols:
        for col in qc_cols:
            bad_index.append(list(dataframe[dataframe[col] == False].index))

    # Subset the dataframe removing bad flagged samples before running QC
    bad_index = list(set([l for sublist in bad_index for l in sublist]))
    search_index = [x for x in dataframe.index if x not in bad_index]
    dataframe_filtered = dataframe.loc[search_index]

    # For each group:
    #   until there are no samples remaining or no samples violate the QC:
    #     for each sample:
    #       count how many other samples it falls below the correlation QC threshold
    #     remove the sample with the most QC violations
    df_corr_passed = []
    for group, d in dataframe_filtered[list(genes) + factors].groupby(factors):
        d_ = d.copy()
        if d_.shape[0] <= 1:
            df_corr_passed.append(d_)
            continue
        while d_.shape[0] > 0:
            d_mat = d_[genes].values.astype('float64')
            corrcoef = np.corrcoef(d_mat)
            trius = np.triu_indices(d_mat.shape[0], k=1)
            triu_cc = zip(zip(trius[0], trius[1]), corrcoef[trius])

            sample_corr = {}
            for corr in triu_cc:
                samps = corr[0]
                if samps[0] not in sample_corr:
                    sample_corr[samps[0]] = [corr[1]]
                else:
                    sample_corr[samps[0]].append(corr[1])
                if samps[1] not in sample_corr:
                    sample_corr[samps[1]] = [corr[1]]
                else:
                    sample_corr[samps[1]].append(corr[1])

            low_corr_count = {}
            for sample in sample_corr:
                low_corr_count[sample] = len([x for x in sample_corr[sample] if x < cc])

            if len([k for k, v in low_corr_count.items() if v != 0]) == 0:
                break

            worst_corr = [k for k, v in low_corr_count.items() if v == max(low_corr_count.values())]
            d_.drop(d_.index[worst_corr], axis=0, inplace=True)

        df_corr_passed.append(d_)
    df_corr_passed = pd.concat(df_corr_passed)
    dataframe['QC_gcorr_BOOL'].loc[df_corr_passed.index] = True
    return dataframe

def main(experiment_id):
    # """Main function"""
    # r = Reactor()
    # ag = r.client # Agave client
    # context = r.context  # Actor context
    # m = context.message_dict
    # experiment_id = m.get('experiment_id')
    prefix = '/work/projects/SD2E-Community/prod/data/'

    (metadata_query_results, preprocessing_jobs, alignment_jobs, dataframe_jobs) \
        = mongo_query(experiment_id)
    meta_data = metadata_construction(metadata_query_results)
    meta_data = crawl_file_system(prefix, meta_data, preprocessing_jobs, alignment_jobs)
    write_to_csv(meta_data, experiment_id)



    """
    Starting from a raw counts dataframe and metadata dataframe flag all samples below N mapped reads and
    below CC in-group correlation using a recursive drop-out algorithm.
    Flags: (bool)   True  = OK
                    False = BAD
    """

    # Gather up the experimental test factors.
    # eg. ['timepoint', 'strain', 'temperature', 'Arabinose', 'IPTG']
    #factors = get_group_conditions_from_metadata()
    # somewhat brittle way to procure metadata keys
    # grabs metadata keys for the first sample in the
    # metadata dict, and filters out any existing QC flags
    factors = [metadata_key for metadata_key in meta_data[list(meta_data.keys())[0]] if metadata_key.split("_")[0] != 'QC']

    # Depending on the starting point we need to have an initial dataframe.
    # Collect all the raw count dataframes produced in this project.
    # count_dataframes = get_raw_count_dataframe()
    df_counts = pd.read_csv(prefix+dataframe_jobs[experiment_id]['archive_path'] + '/ReadCountMatrix_preCAD.tsv', sep='\t')

    # These belong to the same project / species so they should have identical gene lists.
    # We assign the gene_id to the index for all dataframes to allow joining.
    #for df in count_dataframes:
    #    df.set_index('gene_id', inplace=True)
    #genes = count_dataframes[0].index
    df_counts.set_index('gene_id', inplace=True)
    genes = df_counts.index

    # If theres more than one dataframe, join them.
    #if len(count_dataframes) > 1:
    #    df_counts = pd.concat(count_dataframes, axis=1)
    #else:
    #    df_counts = count_dataframes[0]


    # Get the metadata dataframe
    #df_metadata = get_metadata_dataframe()
    df_metadata = pd.read_csv(experiment_id + '_QC_and_metadata.csv')
    df_metadata = df_metadata.set_index('sample_id')
    df_metadata = df_metadata.T

    df = pd.merge(df_metadata, df_counts, on=list(df_metadata.columns), how='outer', left_index=True,
                  right_index=True).T

    df = tag_low_mapped_reads(df, genes, 5e5)
    print('Filtered out {}/{} ({:.2%}) samples'.format(df[df['QC_nmap_BOOL'] == False].shape[0], df.shape[0],
                                                       df[df['QC_nmap_BOOL'] == False].shape[0] / df.shape[0]))


    df = tag_low_correlation_biological_replicates(df, genes, factors, 0.90)
    print('Filtered out {}/{} ({:.2%}) samples'.format(df[df['QC_gcorr_BOOL'] == False].shape[0], df.shape[0],
                                                       df[df['QC_gcorr_BOOL'] == False].shape[0] / df.shape[0]))

    qc_cols = [x for x in df.columns if 'QC' in x]
    met_vals = [x for x in df.columns if x in factors]
    filter_cols = qc_cols + met_vals
    #df.T.loc[qc_cols].to_csv(experiment_id + '_metadata.csv')
    QC_METADATA = df.T.loc[filter_cols]
    QC_METADATA.to_csv(experiment_id + '_QC_and_metadata.csv')
    df.T.to_csv(experiment_id + 'ReadCountMatrix.csv')
    df.to_csv(experiment_id + 'ReadCountMatrix_transposed.csv')


    df_FPKM_counts = pd.read_csv(prefix+dataframe_jobs[experiment_id]['archive_path'] + '/ReadCountMatrix_preCAD_FPKM.tsv', sep='\t')
    df_FPKM_counts.set_index('gene_id', inplace=True)
    df_FPKM_counts = pd.merge(QC_METADATA, df_FPKM_counts, on=list(QC_METADATA.columns), how='outer', left_index=True,
                  right_index=True).T
    df_FPKM_counts.T.to_csv(experiment_id + 'ReadCountMatrix_FPKM.csv')
    df_FPKM_counts.to_csv(experiment_id + 'ReadCountMatrix_FPKM_transposed.csv')

    df_TPM_counts = pd.read_csv(prefix+dataframe_jobs[experiment_id]['archive_path'] + '/ReadCountMatrix_preCAD_TPM.tsv', sep='\t')
    df_TPM_counts.set_index('gene_id', inplace=True)
    df_TPM_counts = pd.merge(QC_METADATA, df_TPM_counts, on=list(QC_METADATA.columns), how='outer', left_index=True,
                  right_index=True).T
    df_TPM_counts.T.to_csv(experiment_id + 'ReadCountMatrix_TPM.csv')
    df_TPM_counts.to_csv(experiment_id + 'ReadCountMatrix_TPM_transposed.csv')
    return


if __name__ == '__main__':
    main(sys.argv[1])
