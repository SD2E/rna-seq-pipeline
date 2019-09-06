### To Do

- Try ag.files.download for download flagstat_preproc_path
- audit_and_score_rx generates a nonce if requested by mini_preproc_rx
    - Security issue?...
- What does crawl_filesystem do?

        for each preprocessing job:
            # get total counts from preprocessing job
            grep 'Total Sequences' from trimmed_fastqc.zip R1 and R2
            get flagstat file from alignment job
            get % and # mapped from flagstat
            if # mapped >= 500000:
                bool_mapped = "True"
            else:
                bool_mapped = "False"

            # then extract stuff from fastqc reads
            zip_ref = zipfile.ZipFile(fastqc_r1, 'r')
            zip_ref.extractall('.')
            zip_ref.close()
            get quality and num_reads for each read
            sum num_reads
            sum([r for r in num_reads if quality >= 30])
            r1_q30 = count_over_30/total_reads
            # repeat for r1 and r2

            # write out metadata
            meta_data[sample_id_preproc]['QC_trimmed_reads_R1_NUM'] = total_counts_r1
            meta_data[sample_id_preproc]['QC_%mapped_reads_NUM'] = percent_mapped
            meta_data[sample_id_preproc]['QC_mapped_reads_NUM'] = number_mapped
            meta_data[sample_id_preproc]['QC_mapped_reads_BOOL'] = bool_mapped
            meta_data[sample_id_preproc]['QC_%Q30_R1_NUM'] = r1_q30

- How would audit and score do this?

        assert caller == alignment_job
        try:
            get preprocessing_job/fastqc from alignment_job
            # maybe from alignment_job.history.data
        except not found:
            if get preproc job from mongodb query:
                return
            else:
                continue
        except:
            r.on_failure("Could not find preproc job")

        # function to get app.outputs from jobID
        fastqc = agaveutils.agave_download_file(preproc['archive_path'])
        zipfile.extract(fastqc)
        # pull metadata

        # agave download again for the preproc flagstat
        # or r.client.files.download(archive path from preproc)


### App/Reactor Outline

- audit_and_score rx
	- Reactor webhook listens on:
		- Not sure yet, some options include
		- {path}/*R1_001_trimmed_fastqc.zip appears in a directory
			- Good sanity check
			- But not great as a front line webhook
		- preproc app is run (add to notif in preproc rx)
			- Probably the best way because it monitors job progress from Agave
		- preproc rx is run (send message directly from preproc rx)
		- Pass experiment/sample IDs
			- Sort of the same as crawl_file_system
