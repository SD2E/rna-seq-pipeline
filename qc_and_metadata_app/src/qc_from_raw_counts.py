import sys
import math
import pandas as pd
import numpy as np
from collections import Counter


def main():
    """
    Starting from a raw counts dataframe and metadata dataframe flag all samples below N mapped reads and
    below CC in-group correlation using a recursive drop-out algorithm.
    Flags: (bool)   True  = OK
                    False = BAD
    """

    # Gather up the experimental test factors.
    # eg. ['timepoint', 'strain', 'temperature', 'Arabinose', 'IPTG']
    factors = get_group_conditions_from_metadata()

    # Depending on the starting point we need to have an initial dataframe.
    # Collect all the raw count dataframes produced in this project.
    count_dataframes = get_raw_count_dataframe()

    # These belong to the same project / species so they should have identical gene lists.
    # We assign the gene_id to the index for all dataframes to allow joining.
    for df in count_dataframes:
        df.set_index('gene_id', inplace=True)
    genes = count_dataframes[0].index

    # If theres more than one dataframe, join them.
    if len(count_dataframes) > 1:
        df_counts = pd.concat(count_dataframes, axis=1)
    else:
        df_counts = count_dataframes[0]

    # Get the metadata dataframe
    df_metadata = get_metadata_dataframe()
    df_metadata = df_metadata.set_index('sample_id')
    df_metadata = df_metadata.T

    df = pd.merge(df_metadata, df_counts, on=list(df_metadata.columns), how='outer', left_index=True,
                  right_index=True).T

    df = tag_low_mapped_reads(df, genes, 5e5)
    print('Filtered out {}/{} ({:.2%}) samples'.format(df[df['QC_nmap'] == False].shape[0], df.shape[0],
                                                       df[df['QC_nmap'] == False].shape[0] / df.shape[0]))


    df = tag_low_correlation_biological_replicates(df, genes, factors, 0.90)
    print('Filtered out {}/{} ({:.2%}) samples'.format(df[df['QC_gcorr'] == False].shape[0], df.shape[0],
                                                       df[df['QC_gcorr'] == False].shape[0] / df.shape[0]))

    qc_cols = [x for x in df.columns if 'QC' in x]
    qc_flag_output_name = get_project_name()
    df.T.loc[qc_cols].to_csv(qc_flag_output_name)
    return


def tag_low_mapped_reads(dataframe, genes, nmapped=5e5):
    """
    Introduce a NMap QC flag.
    This is not sensitive to prior QC steps so we dont need to filter out BAD flagged
    samples prior to assessment.
    """
    if 'QC_nmap' not in list(dataframe.columns):
        dataframe['QC_nmap'] = False

    dataframe_tagged = dataframe[dataframe[genes].sum(axis=1) > nmapped]
    dataframe['QC_nmap'].loc[dataframe_tagged.index] = True
    bad_indexes = [x for x in dataframe.index if x not in dataframe_tagged.index]
    dataframe['QC_nmap'].loc[bad_indexes] = False
    return dataframe


def tag_low_correlation_biological_replicates(dataframe, genes, factors, cc=0.8):
    """
    Introduce an in-group correlation QC flag.
    This QC checks correlation between samples so previously tagged bad samples
    should be removed prior to assessment.
    """
    if 'QC_gcorr' not in list(dataframe.columns):
        dataframe['QC_gcorr'] = False

    # Collect previous QC flags if they exist
    bad_index = []
    qc_cols = [x for x in dataframe.columns if 'QC' in x if x != 'QC_gcorr']
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
    dataframe['QC_gcorr'].loc[df_corr_passed.index] = True
    return dataframe


if __name__ == "__main__":
    status = main()
    sys.exit(status)
