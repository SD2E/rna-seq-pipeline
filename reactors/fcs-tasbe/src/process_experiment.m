function [cm] = process_experiment(i_channels,color_model,color_files,color_pair_files,output,readings)

  pkg load io;

  i_channels
  color_model;
  color_files;
  color_pair_files;

  %bead_batch = "Lot AA01, AA02, AA03, AA04, AB01, AB02, AC01, GAA01-R"
  %bead_model = "SpheroTech RCP-30-5A"

  % convert the channels from structs to octave objects
  channels = {}
  for i = 1:length(i_channels)
    c = i_channels{i}
    channels{i} = Channel(c.name, c.Laser, c.FilterCenter,c.FilterWidth);
    channels{i} = setPrintName(channels{i},c.PrintName);
    channels{i} = setLineSpec(channels{i},c.LineSpec);
    
    for p = 1:length(color_pair_files)
      for q = 1:length(color_pair_files{p})
        if strcmp(color_pair_files{p}{q},c.name)
          "here"
          color_pair_files{p}{q} = channels{i}
        end
      end
    end
  end

  

  AGP =AutogateParameters();

  'calling autogate'
  color_model.blank_file
  autogate = GMMGating(color_model.blank_file,AGP,'plots');

  'done autogate'
  color_model;

  CM = ColorModel(color_model.bead_file, color_model.blank_file, channels, color_files, color_pair_files);
  %CM = set_bead_plot(CM,0)
  CM = set_translation_plot(CM,color_model.translation_plot);
  CM = set_noise_plot(CM,color_model.noise_plot);
  CM = set_bead_model(CM,color_model.bead_model);
  CM=set_translation_channel_min(CM,color_model.channel_mins);

  CM = set_bead_batch(CM, color_model.bead_batch);
  CM = set_FITC_channel_name(CM,color_model.fitc_channel_name);
  
  settings = TASBESettings();

  output.plots_folder
  settings = setSetting(settings,'path',output.plots_folder);  
  CM = add_filter(CM,autogate);
  'resolving'
  CM=resolve(CM, settings);
  'done resolve'
  
  experimentName = output.title; 
  bins = BinSequence(0,0.1,10,'log_bins');
  
  
  % Designate which channels have which roles
  AP = AnalysisParameters(bins,{});
  % Ignore any bins with less than valid count as noise
  AP=setMinValidCount(AP,100');
  % Ignore any raw fluorescence values less than this threshold as too contaminated by instrument noise
  AP=setPemDropThreshold(AP,0');
  % Add autofluorescence back in after removing for compensation?
  AP=setUseAutoFluorescence(AP,false');

  output 
  readings

  file_pairs = {}  
  
  for r = 1:length(readings)
   file_pairs{r,1} = readings{r}.label
   file_pairs{r,2} = readings{r}.files
   
  end
  file_pairs

  cn = {}
  for i=1:length(i_channels)
    cn{i} = i_channels{i}.PrintName

  end

  cn
  'call per analysis'
  CM
  file_pairs
  cn
  AP
  [results sampleresults] = per_color_constitutive_analysis(CM,file_pairs,cn,AP);
  'done analysis'

  n_conditions = size(file_pairs,1);
 
 
  % Make output plots
  OS = OutputSettings(output.title,'','','plots');
  OS.FixedInputAxis = [1e4 1e10];
  %plot_batch_histograms(results,sampleresults,OS,{'b','y','r'});

  results
  

  % Dump CSV files:
  fprintf('Dumping CSV files\n');
  fid = fopen(output.file,'w');
  fprintf(fid,'Device ID,datapoints,,,log10 Mean,,,Std.Dev. of mean (fold)\n'); 

  s = " "
  for i=1:3
    for j=1:length(cn)
      s=[s "," cn{j} ]
    end
  end  
 
  fprintf(fid,s); 
  fprintf(fid,'\n');

  for i=1:n_conditions
    fprintf(fid,'%s,',file_pairs{i,1});
    fprintf(fid,'%d,',sum(results{i}.bincounts));
    fprintf(fid,'%d,',log10(results{i}.means));
    fprintf(fid,'%d,',results{i}.stdofmeans);
    fprintf(fid,'\n');
  end
  fclose(fid);

  results

endfunction

