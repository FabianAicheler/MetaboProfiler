using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Xml;
using Thermo.Magellan.BL.Data;
using Thermo.Magellan.BL.Processing;
using Thermo.Magellan.BL.Processing.Interfaces;
using Thermo.Magellan.DataLayer.FileIO;
using Thermo.Magellan.EntityDataFramework;
using Thermo.Magellan.Exceptions;
using Thermo.Magellan.MassSpec;
using Thermo.Magellan.Utilities;
using Thermo.Metabolism.Algorithms;
using Thermo.Metabolism.DataObjects;
using Thermo.Metabolism.DataObjects.EntityDataObjects;
using Thermo.Metabolism.DataObjects.PeakModels;
using Thermo.Discoverer.Infrastructure.NodeParameters;

//using Thermo.Magellan.BL.ReportEntityData; //for own table
//using Thermo.Metabolism.DataObjects.EntityDataObjects;


using OpenMS.OpenMSFile;
using Thermo.Metabolism.Processing.Services.Interfaces;
using Thermo.Metabolism.DataObjects.Constants;
using Thermo.Metabolism.DomainObjects;

namespace OpenMS.AdapterNodes
{
	#region Node Setup

    [ProcessingNode("{96E83A50-E4E4-4CD8-B2D2-E9B2FB7C2743}",
		Category = CDProcessingNodeCategories.UnknownCompounds,
        DisplayName = "MetaboProfiler",
		MainVersion = 1,
		MinorVersion = 005,
        Description = "Detects and quantifies unknown compounds in the data using the OpenMS framework.")]

	[ConnectionPoint("IncomingSpectra",
		ConnectionDirection = ConnectionDirection.Incoming,
		ConnectionMultiplicity = ConnectionMultiplicity.Single,
		ConnectionMode = ConnectionMode.Manual,
		ConnectionRequirement = ConnectionRequirement.RequiredAtDesignTime,
		ConnectionDisplayName = ProcessingNodeCategories.SpectrumAndFeatureRetrieval,
		ConnectionDataHandlingType = ConnectionDataHandlingType.InMemory)]
	[ConnectionPointDataContract(
		"IncomingSpectra",
		MassSpecDataTypes.MSnSpectra)]

    [ConnectionPoint("OutgoingItems",
        ConnectionDirection = ConnectionDirection.Outgoing,
        ConnectionMultiplicity = ConnectionMultiplicity.Multiple,
        ConnectionMode = ConnectionMode.Manual,
        ConnectionRequirement = ConnectionRequirement.Optional,
        ConnectionDataHandlingType = ConnectionDataHandlingType.FileBased)]

    [ConnectionPointDataContract(
        "OutgoingItems",
        MetabolismDataTypes.UnknownCompoundIonInstances)]    

    [ConnectionPoint("OutgoingPeaks",
    ConnectionDirection = ConnectionDirection.Outgoing,
    ConnectionMultiplicity = ConnectionMultiplicity.Multiple,
    ConnectionMode = ConnectionMode.Manual,
    ConnectionRequirement = ConnectionRequirement.Optional,
    ConnectionDataHandlingType = ConnectionDataHandlingType.FileBased)]
    [ConnectionPointDataContract(
        "OutgoingPeaks",
        MetabolismDataTypes.ChromatogramPeaks)]


    [ConnectionPoint("consensusXml",
        ConnectionDirection = ConnectionDirection.Outgoing,
        ConnectionMultiplicity = ConnectionMultiplicity.Multiple,
        ConnectionMode = ConnectionMode.Manual,
        ConnectionRequirement = ConnectionRequirement.Optional,
        ConnectionDisplayName = ProcessingNodeCategories.DataInput,
        ConnectionDataHandlingType = ConnectionDataHandlingType.FileBased)]
    [ConnectionPointDataContract(
        "consensusXml",
        "consensusxml")]

    [ConnectionPoint("OutgoingPeakConsolidationProvider",
        ConnectionDirection = ConnectionDirection.Outgoing,
        ConnectionMultiplicity = ConnectionMultiplicity.Multiple,
        ConnectionMode = ConnectionMode.Manual,
        ConnectionRequirement = ConnectionRequirement.Optional,
        ConnectionDataHandlingType = ConnectionDataHandlingType.InMemory)]
    [ConnectionPointDataContract(
        "OutgoingPeakConsolidationProvider",
        MetabolismDataTypes.PeakConsolidationProvider)]

	[ProcessingNodeConstraints(UsageConstraint = UsageConstraint.OnlyOncePerWorkflow)]

	#endregion

    public partial class OpenMSFeatureFinderNode : ProcessingNode<UnknownFeatureConsolidationProvider, ConsensusXMLFile>,
        IResultsSink<MassSpectrumCollection>
	{

        private int m_currentStep;
        private int m_numSteps;
        private int m_numFiles; 
		private readonly SpectrumDescriptorCollection m_spectrumDescriptors = new SpectrumDescriptorCollection();
	    private ConsensusXMLFile m_consensusXML;
        private featureXMLFile m_decharge_fm;
        private ConsensusXMLFile m_decharge_cm;

        private Dictionary<ulong, List<ulong>> m_cons_to_feat_dict;


	    private List<WorkflowInputFile> m_workflowInputFiles;

        #region Parameters
        [MassToleranceParameter(
            Category = "1. Feature Finding", /// Accurate Mass Search",
            DisplayName = "Mass Tolerance",
            Description = "This parameter specifies the mass tolerance for XIC creation and metabolite feature finding.",
            Subset = "ppm", // required by current design
            DefaultValue = "5 ppm",
            MinimumValue = "0.2 ppm",
            MaximumValue = "100 ppm",
            IntendedPurpose = ParameterPurpose.MassTolerance,
            Position = 1)]
        public MassToleranceParameter MassTolerance;

        [DoubleParameter(Category = "1. Feature Finding",
            DisplayName = "Noise Threshold",
            Description = "This parameter specifies the intensity threshold below which peaks are rejected as noise.",
            DefaultValue = "10000")] //10000 based on observed intensities in instrument data
        public DoubleParameter NoiseThreshold;

        //[StringSelectionParameter(Category = "1. Metabolite Feature Finding", /// Accurate Mass Search",
        //    DisplayName = "Ion mode",
        //    SelectionValues = new string[] { "positive", "negative" })]
        //public SimpleSelectionParameter<string> ion_mode;

        //[StringSelectionParameter(Category = "1. Metabolite Feature Finding", /// Accurate Mass Search",
        //    DisplayName = "Report mode of Accurate Mass Search",
        //    SelectionValues = new string[] { "all", "top3", "best" })]
        //public SimpleSelectionParameter<string> report_mode;

        [BooleanParameter(Category = "2. Feature Linking",
            DisplayName = "Do Map Alignment",
            Description = "This parameter specifies whether a linear map alignment should be performed.",
            DefaultValue = "true",
            Position = 1)]
        public BooleanParameter do_map_alignment;

        /// <summary>
        /// This parameter specifies the maximum allowed retention time difference for features to be linked together.
        /// </summary>
        [DoubleParameter(
            Category = "2. Feature Linking",
            DisplayName = "Max. RT Difference [min]",
            Description = "This parameter specifies the maximum allowed retention time difference for feature pairs during model building of the alignment and during feature linking.",
            DefaultValue = "0.33",
            Position = 2)]
        public DoubleParameter RTThreshold;

        /// <summary>
        /// This parameter specifies the maximum allowed m/z difference for features to be linked together.
        /// </summary>
        [DoubleParameter(
            Category = "2. Feature Linking",
            DisplayName = "Max. m/z Difference [ppm]",
            Description = "This parameter specifies the maximum allowed m/z difference in ppm for feature pairs during model building of the alignment and during feature linking.",
            DefaultValue = "10",
            Position = 3)]
        public DoubleParameter MZThreshold;

        //[AbstractIonParameter(
        //                Category = "3. Ionization",
        //                DataDescriptor = Thermo.Metabolism.DomainObjects.CommonDataNames.AbstractIon,// .CommonDataNamesAbstractIon,
        //                DisplayName = "Ions",
        //                Description = "This parameter allows the selection of multiple ion definitions from a predefined list.",
        //                DefaultValue = "[M+H]+1",
        //                IsMultiSelect = true,
        //                ValueRequired = true,
        //                Position = 1)]
        //public AbstractIonParameter AbstractIons;

        [IntegerParameter(
            Category = "3. Ionization",
            DisplayName = "Linked feature number to consider for adduct grouping",
            Description = "Minimum required number of samples a feature has to occur in to be considered for adduct grouping.",
            DefaultValue = "2",
            MinimumValue = "1",
            Position = 2)]
        public IntegerParameter FileFilter_min_samples;

        [MultilineStringParameter(
            Category = "3. Ionization",
            DisplayName = "Considered adducts",
            Description = "Adducts considered for possible adduct combinations of ions.",
            DefaultValue = "H+:0.9;Na+:0.1",
            FileExtension = ".adducts",
            EditorGuid = "6ED9DDD9-6372-4ED8-99B5-7EE61C3BE57C")]
        public MultilineStringParameter Decharger_potential_adducts;

        [IntegerParameter(
            Category = "3. Ionization",
            DisplayName = "Max. charge",
            Description = "Maximal allowed charge of adduct combinations.",
            DefaultValue = "2",
            MinimumValue = "1",
            Position = 2)]
        public IntegerParameter Decharger_max_charge;


        [DoubleParameter(
            Category = "3. Ionization",
            DisplayName = "Max. allowed mass error (Th)",
            Description = "The allowed mass error (in Th) between deduced compound and its decharged ions.",
            DefaultValue = "0.001",
            MinimumValue = "0.00000001",
            Position = 2)]
        public DoubleParameter Decharger_mass_max_diff;

        [DoubleParameter(
            Category = "3. Ionization",
            DisplayName = "Max. allowed retention difference (s)",
            Description = "Allowed retention distance (in seconds) to consider feature pairs.",
            DefaultValue = "1.0",
            MinimumValue = "0.0",
            Position = 2)]
        public DoubleParameter Decharger_rt_max_diff;

        [BooleanParameter(Category = "4. Output",
        DisplayName = "Save Tool Results",
        Description = "This parameter specifies whether the OpenMS tool output should be saved in addition to the Compound Discoverer result files.",
        DefaultValue = "true",
        Position = 1)]
        public BooleanParameter do_save;

	    #endregion

		/// <summary>
		/// Initializes the progress.
		/// </summary>
		/// <returns></returns>
        public override ProgressInitializationHint InitializeProgress()
        {
            return new ProgressInitializationHint(4 * ProcessingServices.CurrentWorkflow.GetWorkflow().GetWorkflowInputFiles().ToList().Count, ProgressDependenceType.Independent);
        }
        
		/// <summary>
		/// Portion of mass spectra received.
		/// </summary>
		public void OnResultsSent(IProcessingNode sender, MassSpectrumCollection result)
		{
			ArgumentHelper.AssertNotNull(result, "result");
			m_spectrumDescriptors.AddRange(ProcessingServices.SpectrumProcessingService.StoreSpectraInCache(this, result));
		}
        
		/// <summary>
		/// Called when the parent node finished the data processing.
		/// </summary>
		/// <param name="sender">The parent node.</param>
		/// <param name="eventArgs">The result event arguments.</param>
        public override void OnParentNodeFinished(IProcessingNode sender, ResultsArguments eventArgs)
        {
            // determine number of inputfiles which have to be converted
            m_workflowInputFiles = EntityDataService.CreateEntityItemReader().ReadAll<WorkflowInputFile>().ToList();
            m_numFiles = m_workflowInputFiles.Count;

            //estimate time needed
            m_currentStep = 0; // current step in internal pipeline, used for progress bar 
            //number of steps: 
            
            
            m_numSteps = 1 + //export to MzML: 1
                m_numFiles +          //1 per file for FFM
                m_numFiles +          //1 per file for Import of OpenMS results
                3 * m_numFiles + m_numFiles +     //XIC: 3 per file create, 1 persist
                m_numFiles;           //mass trace: 1 per persist
            if (m_numFiles > 1)
            {
                m_numSteps += m_numFiles; //FeatureLinking
            }
            if (m_numFiles > 1 && do_map_alignment.Value)
            {
                m_numSteps += m_numFiles ; //MapAlign
            }




            var exportedList = new List<string>(m_numFiles);

            #region previouscode
            //// Group spectra by file id and process 
            //foreach (var spectrumDescriptorsGroupedByFileId in m_spectrumDescriptors
            //    .Where(w => (w.ScanEvent.MSOrder == MSOrderType.MS1))//.Where(w=>w.ScanEvent.MSOrder == MSOrderType.MS1) //if we remove, we get 1 spec per file
            //    .GroupBy(g => g.Header.FileID))
            //{
            //    // Group by the scan event of the MS1 spectrum to avoid mixing up different polarities or scan ranges
            //    foreach (var grp in spectrumDescriptorsGroupedByFileId.GroupBy(g => g.ScanEvent))
            //    {
            //        int fileId = spectrumDescriptorsGroupedByFileId.Key;

            //        // Flatten the spectrum tree to a collection of spectrum descriptors. 
            //        var spectrumDescriptors = grp.ToList();

            #endregion

            foreach (var spectrumDescriptorsGroupedByFileId in m_spectrumDescriptors.GroupBy(g => g.Header.FileID))
            {
                // Group spectra into spectrum trees. (Meaning relations between MSOrders?
                var spectrumTrees = SpectrumDescriptorTreeNode.BuildSpectralTrees(spectrumDescriptorsGroupedByFileId.OfType<ISpectrumDescriptor>().ToList());
                // Group spectrum trees by the scan event of the MS1 spectrum to avoid mixing up different polarities or scan ranges
                foreach (var spectrumTreesGroupedByMs1ScanEvents in spectrumTrees
                    .Where(w => w.SpectrumDescriptor.ScanEvent.MSOrder == MSOrderType.MS1)
                    .GroupBy(g => g.SpectrumDescriptor.ScanEvent))
                {
                    int fileId = spectrumDescriptorsGroupedByFileId.Key;
                    // Flatten the spectrum tree to a collection of spectrum descriptors. Meaning corresponding MS2 are in there?
                    var spectrumDescriptors = spectrumTreesGroupedByMs1ScanEvents
                        .SelectMany(sm => sm.AllTreeNodes.Select(s => s.SpectrumDescriptor))
                        .OfType<SpectrumDescriptor>()
                        .ToList();

                    // Export spectra to temporary *.mzML file. Only one file has this fileId
                    var fileToExport = m_workflowInputFiles.Where(w => w.FileID == fileId).ToList().First().PhysicalFileName;
                    SendAndLogMessage("Assigning fileID_{0} to input file {1}", fileId, fileToExport);
                    var spectrumExportFileName = ExportSpectraToMzMl(fileId, spectrumDescriptors);

                    //store path of exported mzML file
                    exportedList.Add(spectrumExportFileName);

                    //call this so that wrong progress gets overwritten fast
                    ReportTotalProgress((double)m_currentStep / m_numSteps);
                }
            }
            m_currentStep += 1;
            ReportTotalProgress((double)m_currentStep / m_numSteps);


            //After all files are exported, run pipeline. SendResults in RunPipeline, due to availability of filenames
            //Pipeline should only be run once for all supplied files
            var featureIonToPeak = RunOpenMsPipeline(exportedList);


            foreach (var spectrumDescriptorsGroupedByFileId in m_spectrumDescriptors.GroupBy(g => g.Header.FileID))
            {
                // Group spectra into spectrum trees.
                var spectrumTrees = SpectrumDescriptorTreeNode.BuildSpectralTrees(spectrumDescriptorsGroupedByFileId.OfType<ISpectrumDescriptor>().ToList());

                // Group spectrum trees by the scan event of the MS1 spectrum to avoid mixing up different polarities or scan ranges
                foreach (var spectrumTreesGroupedByMs1ScanEvents in spectrumTrees
                    .Where(w => w.SpectrumDescriptor.ScanEvent.MSOrder == MSOrderType.MS1)
                    .GroupBy(g => g.SpectrumDescriptor.ScanEvent))
                {
                    int fileId = spectrumDescriptorsGroupedByFileId.Key;

                    // Flatten the spectrum tree to a collection of spectrum descriptors. 
                    var spectrumDescriptors = spectrumTreesGroupedByMs1ScanEvents
                        .SelectMany(sm => sm.AllTreeNodes.Select(s => s.SpectrumDescriptor))
                        .OfType<SpectrumDescriptor>()
                        .ToList();

	                var fileFeatures = featureIonToPeak.Where(w => w.Key.FileID == fileId).ToDictionary(k => k.Key, v => v.Value);

					RebuildAndPersistCompoundIonTraces(fileId, spectrumDescriptors, fileFeatures);
					AssignAndPersistMassSpectra(spectrumDescriptors, fileFeatures);
                }
            }


			var dict = new Dictionary<ulong, Centroid>();

			var doc = new XmlDocument();
			doc.Load(m_consensusXML.get_name());

			//now go over consensus elements, add elements to peak directories depending on map 
			XmlNodeList consensusElements = doc.GetElementsByTagName("consensusElement");
			foreach (XmlElement consensusElement in consensusElements)
			{
				XmlNode centroidNode = consensusElement.SelectSingleNode("centroid");
				double mz = Convert.ToDouble(centroidNode.Attributes["mz"].Value);
				double rt = Convert.ToDouble(centroidNode.Attributes["rt"].Value) / 60.0; //changed to minute!

				var centroid = new Centroid()
				{
					mass = mz,
					rt = rt
				};

				var groupedElements = consensusElement.SelectSingleNode("groupedElementList");

				foreach (XmlNode element in groupedElements.SelectNodes("element"))
				{
					var id = Convert.ToUInt64(element.Attributes["id"].Value);
					dict.Add(id, centroid);
				}

			}

			// Add database indecies
			AddDatabaseIndices();


            //copy files to result folder if corresponding setting checked
            //besides config files, all have unique names. For configs, lets just keep the first finished one as representative
            if (do_save.Value)
            {
                foreach (var file in Directory.GetFiles(NodeScratchDirectory))
                    if (!File.Exists(Path.Combine(OutputDirectory, Path.GetFileName(file))))
                    {
                        File.Copy(file, Path.Combine(OutputDirectory, Path.GetFileName(file)));
                    }
            }
            // Send in memory results to all child nodes
			SendResults(new UnknownFeatureConsolidationProvider(dict, ProcessingNodeNumber, DisplayName, EntityDataService));

			// Fire Finish event
			FireProcessingFinishedEvent(new ResultsArguments());

            ReportTotalProgress(1.0);
        }

        /// <summary>
        /// Exports the correspoding spectra to a new created mzML.
        /// </summary>
        /// <param name="spectrumDescriptorsGroupByFileId">The spectrum descriptors grouped by file identifier.</param>
        /// <returns>The file name of the new created mzML file, containing the exported spectra.</returns>
        /// <exception cref="Thermo.Magellan.Exceptions.MagellanProcessingException"></exception>
        private string ExportSpectraToMzMl(int fileId, IEnumerable<ISpectrumDescriptor> spectrumDescriptorsGroupByFileId)
        {
            var timer = Stopwatch.StartNew();

            // Get the unique spectrum identifier from each spectrum descriptor
            var spectrumIds = spectrumDescriptorsGroupByFileId
                .OrderBy(o => o.Header.RetentionTimeCenter)
                .Select(s => s.Header.SpectrumID)
                .ToList();

            SendAndLogTemporaryMessage(MessageLevel.Debug,"Start export of {0} spectra with input file id {1} ...", spectrumIds.Count, fileId);

            var exporter = new mzML
            {
                SoftwareName = "Compound Discoverer",
                SoftwareVersion = new Version(FileVersionInfo.GetVersionInfo(Assembly.GetEntryAssembly().Location).FileVersion)
            };

            // Use node specific scratch directory to store the temporary mzML file 
            string spectrumExportFileName = Path.Combine(NodeScratchDirectory, Guid.NewGuid().ToString().Replace('-', '_') + String.Format("[FileID_{0}].mzML", fileId));

            bool exportFileIsOpen = exporter.Open(spectrumExportFileName, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.Read);

            if (exportFileIsOpen == false)
            {
                throw new MagellanProcessingException(String.Format("Cannot create or open mzML file: {0}", spectrumExportFileName));
            }

            // Retrieve spectra in bunches from the spectrum cache and export themto the new created mzML file.			
            var spectra = new MassSpectrumCollection(1000);

            foreach (var spectrum in ProcessingServices.SpectrumProcessingService.ReadSpectraFromCache(spectrumIds))
            {
                spectra.Add(spectrum);

                if (spectra.Count == 1000)
                {
                    exporter.ExportMassSpectra(spectra);
                    spectra.Clear();
                }
            }

            exporter.ExportMassSpectra(spectra);

            exporter.Close();

            SendAndLogMessage("Exporting {0} spectra with input file id {1} took {2}.", spectrumIds.Count, fileId, StringHelper.GetDisplayString(timer.Elapsed));

            return spectrumExportFileName;
        }

		/// <summary>
		/// Creates database indices.
		/// </summary>
		private void AddDatabaseIndices()
		{
			EntityDataService.CreateIndex<UnknownFeatureIonInstanceItem>();
			EntityDataService.CreateIndex<ChromatogramPeakItem>();
			EntityDataService.CreateIndex<XicTraceItem>();
			EntityDataService.CreateIndex<MassSpectrumItem>();
		}

        /// <summary>
        /// Executes the pipeline.
        /// </summary>
        /// <param name="pipelineParameterFileName">The name of the file which  settings path.</param>		
        /// <exception cref="Thermo.Magellan.Exceptions.MagellanProcessingException"></exception>
        private IDictionary<UnknownFeatureIonInstanceItem, List<ChromatogramPeakItem>> RunOpenMsPipeline(List<string> spectrumExportFileNames)
        {
            //check that entries in list of filenames  is ok
            foreach (var fn in spectrumExportFileNames)
            {
                ArgumentHelper.AssertStringNotNullOrWhitespace(fn, "spectrumExportFileName");
            }
            var timer = Stopwatch.StartNew();
            SendAndLogMessage("Starting OpenMS pipeline to process spectra ...");


            //initialise variables
            string masserror = MassTolerance.ToString(); //MassError obtained from workflow option
            masserror = masserror.Substring(0, masserror.Length - 4); //remove ' ppm' part (ppm is enforced)            

            //list of input and output files of specific OpenMS tools
            string[] invars = new string[m_numFiles];
            string[] outvars = new string[m_numFiles];
            string ini_path = ""; //path to configuration files with parameters for the OpenMS Tool

            //create Lists of possible OpenMS files
            m_consensusXML = new ConsensusXMLFile("");
            List<featureXMLFile> origFeatures = new List<featureXMLFile>(m_numFiles);
            List<featureXMLFile> alignedFeatures = new List<featureXMLFile>(m_numFiles);

            //Add path of Open MS installation here
            var openMSdir = Path.Combine(ServerConfiguration.ToolsDirectory, "OpenMS-2.0/");


            //MetaboliteFinder, do once for each exported file
            var execPath = Path.Combine(openMSdir, @"bin/FeatureFinderMetabo.exe");
            for (int i = 0; i < m_numFiles; i++)
            {
                invars[i] = spectrumExportFileNames[i];
                outvars[i] = Path.Combine(Path.GetDirectoryName(invars[i]),
                                      Path.GetFileNameWithoutExtension(invars[i])) + ".featureXML";
                origFeatures.Add(new featureXMLFile(outvars[i]));
                //set ITEM parameters in ini
                Dictionary<string, string> ffm_parameters = new Dictionary<string, string> {
                            {"in", invars[i]},
                            {"out", outvars[i]},
                            {"mass_error_ppm", masserror},
                            {"noise_threshold_int", NoiseThreshold.ToString()},
                            {"trace_termination_outliers", "2"}}; //personal preference after looking at some mzML data for Thermo instruments (vs 3; 5)
                

                ini_path = Path.Combine(NodeScratchDirectory, @"FeatureFinderMetaboDefault.ini");
                create_default_ini(execPath, ini_path);
                WriteItem(ini_path, ffm_parameters);
                SendAndLogMessage("Starting FeatureFinderMetabo for file [{0}]", invars[i]);
                RunTool(execPath, ini_path);
                m_currentStep += 1;
                ReportTotalProgress((double)m_currentStep / m_numSteps);
            }



            //if only one file, convert featureXML (unaligned) to consensus, no alignment or linking will occur
            if (m_numFiles == 1)
            {
                invars[0] = origFeatures[0].get_name();
                outvars[0] = Path.Combine(Path.GetDirectoryName(invars[0]),
                    Path.GetFileNameWithoutExtension(invars[0])) +
                    ".consensusXML";
                m_consensusXML = new ConsensusXMLFile(outvars[0]);

                execPath = Path.Combine(openMSdir, @"bin/FileConverter.exe");
                Dictionary<string, string> convert_parameters = new Dictionary<string, string> {
                            {"in", invars[0]}, //as only one file, outvar was assigned the result from FFM
                            {"in_type", "featureXML"},
                            {"out", outvars[0]},
                            {"out_type", "consensusXML"}};
                ini_path = Path.Combine(NodeScratchDirectory, @"FileConverterDefault.ini");
                create_default_ini(execPath, ini_path);
                WriteItem(ini_path, convert_parameters);
                RunTool(execPath, ini_path);
                //not really worth own progress
            }
            else if (m_numFiles > 1)
            {
                if (do_map_alignment.Value)
                {
                    execPath = Path.Combine(openMSdir, @"bin/MapAlignerPoseClustering.exe");
                    for (int i = 0; i < m_numFiles; i++)
                    {
                        invars[i] = origFeatures[i].get_name(); // current invars will be featureXML
                        outvars[i] = Path.Combine(Path.GetDirectoryName(invars[i]),
                                  Path.GetFileNameWithoutExtension(invars[i])) + ".aligned.featureXML";
                        alignedFeatures.Add(new featureXMLFile(outvars[i]));
                    }

                    Dictionary<string, string> map_parameters = new Dictionary<string, string> {
                    {"max_num_peaks_considered", "10000"},
                    {"ignore_charge", "true"}};
                    ini_path = Path.Combine(NodeScratchDirectory, @"MapAlignerPoseClusteringDefault.ini");
                    create_default_ini(execPath, ini_path);
                    WriteItem(ini_path, map_parameters);
                    replaceItemList(invars, ini_path, "in");
                    replaceItemList(outvars, ini_path, "out");
                    write_MZ_RT_thresholds(ini_path);
                    SendAndLogMessage("Starting MapAlignerPoseClustering");
                    RunTool(execPath, ini_path);
                    m_currentStep += m_numFiles;
                    ReportTotalProgress((double)m_currentStep / m_numSteps);
                }

                //FeatureLinkerUnlabeledQT
                // outvars might be original featureXML, might be aligned.featureXML
                for (int i = 0; i < m_numFiles; i++){
                    if (do_map_alignment.Value){
                        invars[i] = alignedFeatures[i].get_name();
                    }
                    else{
                        invars[i] = origFeatures[i].get_name();
                    }
                }
                //save as consensus.consensusXML, filenames are stored inside, file should normally be accessed from inside CD
                outvars[0] = Path.Combine(Path.GetDirectoryName(invars[0]),
                    "featureXML_consensus.consensusXML");
                m_consensusXML = new ConsensusXMLFile(outvars[0]);

                execPath = Path.Combine(openMSdir, @"bin/FeatureLinkerUnlabeledQT.exe");
                Dictionary<string, string> fl_unlabeled_parameters = new Dictionary<string, string> {
                        {"ignore_charge", "true"},
                        {"out", outvars[0]}};
                ini_path = Path.Combine(NodeScratchDirectory, @"FeatureLinkerUnlabeledQTDefault.ini");
                create_default_ini(execPath, ini_path);
                WriteItem(ini_path, fl_unlabeled_parameters);
                replaceItemList(invars, ini_path, "in");
                write_MZ_RT_thresholds(ini_path);
                SendAndLogMessage("FeatureLinkerUnlabeledQT");
                RunTool(execPath, ini_path);
                m_currentStep += m_numFiles;
                ReportTotalProgress((double)m_currentStep / m_numSteps);
            }

            //decharging of consensus file(s). do in all cases (ppl can just set maxcharge 1 if time issue
            if (true)
            {
                //first we apply file filter: do it always, but lower limit 1 file
                {
                    invars[0] = m_consensusXML.get_name();
                    outvars[0] = Path.Combine(Path.GetDirectoryName(invars[0]),
                        Path.GetFileNameWithoutExtension(invars[0])) +
                        "_filtered.consensusXML";
                    execPath = Path.Combine(openMSdir, @"bin/FileFilter.exe");
                    Dictionary<string, string> filter_parameters = new Dictionary<string, string> {
                            {"in", invars[0]},
                            {"out", outvars[0]},
                            {"size", Math.Min(FileFilter_min_samples.Value, m_numFiles).ToString() + ":"}};
                    ini_path = Path.Combine(NodeScratchDirectory, @"FileFilterDefault.ini");
                    create_default_ini(execPath, ini_path);
                    WriteItem(ini_path, filter_parameters);
                    RunTool(execPath, ini_path);
                }
                //in all cases, we get a consensusXML here, but Decharger requires featureXML
                //(We would do conversion even for featureXML to consensus to feature to remove hulls from information for Decharger)
                //(in theory also possible to ignore convex hull if hull similarity set to 0 in Decharger)
                {
                    invars[0] = outvars[0];
                    outvars[0] = Path.Combine(Path.GetDirectoryName(invars[0]),
                        Path.GetFileNameWithoutExtension(invars[0])) +
                        "_decharged_in.featureXML";
                    execPath = Path.Combine(openMSdir, @"bin/FileConverter.exe");
                    Dictionary<string, string> convert_parameters = new Dictionary<string, string> {
                            {"in", invars[0]},
                            {"in_type", "consensusXML"},
                            {"out", outvars[0]},
                            {"out_type", "featureXML"}};
                    ini_path = Path.Combine(NodeScratchDirectory, @"FileConverterForDechargerDefault.ini");
                    create_default_ini(execPath, ini_path);
                    WriteItem(ini_path, convert_parameters);
                    RunTool(execPath, ini_path);
                }

                {
                    //is featureXml. now decharge
                    //output will be featurexml with features (== consensus) annotated with adduct and consensusfile with pair information
                    //approach: First do for charge 1..1. Most (~90%) ESI ions are of charge 1 so if would do 1..2 and had conflicting edges, I would prefer charge 1 solutions
                    //next: if maxcharge > 1, do charge 1..2. We already assigned many charge 1s, but might be case that e.g. H- 2H pair but no Na. Again, more likely than charge 3 
                    for (int tmp_max_charge = 1; tmp_max_charge <= Decharger_max_charge.Value; tmp_max_charge++){
                        invars[0] = outvars[0]; //from converted in first iteration, else from decharger
                        String infolder = Path.GetDirectoryName(invars[0]);
                        String infile = Path.GetFileNameWithoutExtension(invars[0]);
                        String out_cm = Path.Combine(infolder, infile) +
                                                "_decharged_cm_" + tmp_max_charge.ToString() + ".consensusXML";
                        String out_fm = Path.Combine(infolder, infile) +
                                                "_decharged_fm_" + tmp_max_charge.ToString() + ".featureXML";
                        String out_pairs = Path.Combine(infolder, infile) +
                                                "_decharged_outpairs_" + tmp_max_charge.ToString() + ".consensusXML";
                        execPath = Path.Combine(openMSdir, @"bin/Decharger.exe");
                        Dictionary<string, string> decharge_parameters = new Dictionary<string, string> {
                            {"in", invars[0]},
                            {"out_cm", out_cm},
                            {"out_fm", out_fm},
                            {"outpairs", out_pairs},
                            {"charge_min", "1"},
                            {"charge_max", tmp_max_charge.ToString()},
                            {"charge_span_max", tmp_max_charge.ToString()},
                            {"retention_max_diff", Decharger_rt_max_diff.ToString()},
                            {"retention_max_diff_local", Decharger_rt_max_diff.ToString()},
                            {"mass_max_diff", Decharger_mass_max_diff.ToString()},
                            {"out_type", "consensusXML"}};
                        ini_path = Path.Combine(NodeScratchDirectory, @"DechargerDefault.ini");
                        create_default_ini(execPath, ini_path);
                        WriteItem(ini_path, decharge_parameters);
                        char[] delimiter = { ';' };
                        string[] adducts = MultilineStringParameter.MultilineToSingleLine(Decharger_potential_adducts.Value, ';').Split(delimiter);
                        replaceItemList(adducts, ini_path, "potential_adducts");
                        RunTool(execPath, ini_path);
                        outvars[0] = out_fm;
                        m_decharge_cm = new ConsensusXMLFile(out_cm);
                        m_decharge_fm = new featureXMLFile(out_fm); //only interested in last decharged
                    }
                }
            }


            //part for mappings
            //for each input file, read features, read aligned features (same order assumed)
            if ((m_numFiles > 1) && do_map_alignment.Value)
            {
                //read consensusXML(after alignment)
                XmlDocument consensus_doc = new XmlDocument();
                consensus_doc.Load(m_consensusXML.get_name());
                XmlNodeList consensus_list = consensus_doc.GetElementsByTagName("element");
                //create dictionary of elements, in which we overwrite the original RT. 
                //Because of objects, affects XmlDocument consensus_doc which we then save into new file
                Dictionary<string, XmlElement> consensus_dict = new Dictionary<string, XmlElement>(consensus_list.Count);
                foreach (XmlElement element in consensus_list)
                {
                    consensus_dict[element.Attributes["id"].Value] = element;
                }
                //The consensus contains ids from all featureXmls, thus we have to look into all featureXml
                for (int file_id = 0; file_id < m_numFiles; file_id++)
                {
                    XmlDocument orig_feat_xml = new XmlDocument();
                    orig_feat_xml.Load(origFeatures[file_id].get_name());
                    XmlNodeList orig_featurelist = orig_feat_xml.GetElementsByTagName("feature");
                    foreach (XmlElement feature in orig_featurelist)
                    {
                        var id = feature.Attributes["id"].Value.Substring(2);
                        var rt = feature.SelectNodes("position")[0].InnerText;
                        consensus_dict[id].SetAttribute("rt", rt);
                    }
                }
                var new_consensus_file = Path.Combine(Path.GetDirectoryName(m_consensusXML.get_name()), "Consensus_orig_RT.consensusXml");
                consensus_doc.Save(new_consensus_file);
                m_consensusXML = new ConsensusXMLFile(new_consensus_file);
            }

            SendAndLogMessage("OpenMS pipeline processing took {0}.", StringHelper.GetDisplayString(timer.Elapsed));
            //m_consensusXML contains the XML with original times in the elements
            //the centroids are based on the aligned features.
            //In ImportFoundFeatures we construct IonFeatures with the original Times.
            //M_consensusXMl here only used for fileID mapping
            //Centroids are constructed after OpenMSPipeline
            return ImportFoundFeatures(m_consensusXML, origFeatures);
        }

		/// <summary>
		/// Creates the ions and peaks from featureXML.
		/// </summary>
		/// <param name="fileId">The file id.</param>
		/// <param name="origFeatures">The orig features.</param>
		/// <returns></returns>
		private Dictionary<UnknownFeatureIonInstanceItem, List<ChromatogramPeakItem>> CreateIons2Peaks(int fileId, featureXMLFile origFeatures)
		{
            var doc = new XmlDocument();

            //feature-to-consensus mapping based on m_consensusXml allows us to link all features across all samples to their
            //corresponding consensusFeature they are assigned to. This allows e.g., when iterating over features, to assign the feature
            //the consensus charge (if available). This is also a reason why we want this for the whole m_consensusXml (not just the decharged)
            var feat_to_m_cons_dict = new Dictionary<ulong, ulong>();
            var m_cons_charge_dict = new Dictionary<ulong, Int32>();
            m_cons_to_feat_dict = new Dictionary<ulong,List<ulong>>();
            doc.Load(m_consensusXML.get_name());
            foreach (XmlElement consensusElement in doc.GetElementsByTagName("consensusElement")){
                var elements = new List<ulong>();
                ulong cent_id = Convert.ToUInt64(consensusElement.Attributes["id"].Value.Substring(2));
                if (consensusElement.HasAttribute("charge")){
                    m_cons_charge_dict.Add(cent_id, Convert.ToInt32(consensusElement.Attributes["charge"].Value));                    
                }
                foreach (XmlNode element in consensusElement.GetElementsByTagName("element")){
                    var fid = Convert.ToUInt64(element.Attributes["id"].Value);//already only number here
                    elements.Add(fid);
                    feat_to_m_cons_dict.Add(fid, cent_id);
                }
                m_cons_to_feat_dict.Add(cent_id, elements);
            }



            //extract dc_adducts and charges of dc_candidates
            var cm_adduct_dict = new Dictionary<ulong, Tuple<Int32,String>>();
            doc.Load(m_decharge_fm.get_name());
            foreach (XmlElement dc_consensusFeature in doc.GetElementsByTagName("feature")){
                ulong cent_fid = Convert.ToUInt64(dc_consensusFeature.Attributes["id"].Value.Substring(2));
                var charge = Convert.ToInt32(dc_consensusFeature.SelectSingleNode(@"./charge").InnerText);//best way to get feature charge; after conversion to featureXml, each consensus has at least charge 0.
                var adducts = "unknown"; //default unless decharging determined adducts (and overwriting charge? no, charge should be overwritten by decharging tool)
                //XmlNodeList userParams = consensusFeature.GetElementsByTagName("UserParam");
                foreach (XmlNode userParam in dc_consensusFeature.SelectNodes(@"./UserParam")){
                    if (userParam.Attributes["name"].Value == "dc_charge_adducts"){
                        adducts = userParam.Attributes["value"].Value;
                    }
                }
                cm_adduct_dict.Add(cent_fid, new Tuple<Int32, String>(charge, adducts));
            }

            //First use featureCharge if available
            //then overwrite with consensusCharge (if available) as more confident majority vote
            //then overwrite with decharged consensus as most confident

			XmlDocument origFeaturesDom = new XmlDocument();//
			origFeaturesDom.Load(origFeatures.get_name());
			//XmlNodeList orig_featurelist = origFeaturesDom.SelectNodes(@"//feature");
			var dict = new Dictionary<UnknownFeatureIonInstanceItem, List<ChromatogramPeakItem>>();
            foreach (XmlElement feature in origFeaturesDom.SelectNodes(@"//feature")){
                ulong fid = Convert.ToUInt64(feature.Attributes["id"].Value.Substring(2));
                
				var rtNode = feature.SelectSingleNode(@"./position[@dim=0]"); // RT
				var rt = Double.Parse(rtNode.InnerText) / 60d;
				var massNode = feature.SelectSingleNode(@"./position[@dim=1]"); // Mass
				var mass = Double.Parse(massNode.InnerText);
                //First use featureCharge (at least 0)
                var chargeNode = feature.SelectSingleNode(@"./charge");
                var charge = Int32.Parse(chargeNode.InnerText);
                var adducts = "unknown";
                //then overwrite with consensusCharge as more confident majority vote; 
                //all features should map to a consensusFeature (even if just singleton)
                //m_cons_charge_dict, i.e., consensus features with majority charge is a subset of all consensus features
                if (m_cons_charge_dict.ContainsKey(feat_to_m_cons_dict[fid])){
                    charge = m_cons_charge_dict[feat_to_m_cons_dict[fid]];
                }
                //then overwrite with decharged consensus as most confident (if available)
                //Currently, decharger should not change non-zero charges
                //feat_to_m_cons_dict[fid] should return something for all features
                //question now is whether this consensus feature could be decharged (cm_adduct_dict)
                //decharging is done after #sample filtering (i think does not have to be considered for feat_to_m_cons mapping)
                if (cm_adduct_dict.ContainsKey(feat_to_m_cons_dict[fid])){//i.e. did consensusFeature link enough samples, and could it be decharged?
                    charge = cm_adduct_dict[feat_to_m_cons_dict[fid]].Item1;
                    adducts = cm_adduct_dict[feat_to_m_cons_dict[fid]].Item2;                
                }

				var unknownCompoundIonInstanceItem = new UnknownFeatureIonInstanceItem(){
					                                     ID = EntityDataService.NextId<UnknownFeatureIonInstanceItem>(),
														 FileID = fileId,
														 Mass =  mass,
														 RetentionTime = rt,
														 FeatureID = fid.ToString(),
														 Charge = charge,
                                                         IonDescription = adducts                                                        
				                                     };

				var peaks = new List<ChromatogramPeakItem>();
				XmlNodeList hulls = feature.SelectNodes(@"./convexhull");

				foreach (XmlNode hull in hulls)
				{
					var nrAttrib = hull.Attributes["nr"];
					var nr = int.Parse(nrAttrib.Value);
                    //DID THE CASE OF UserParam CHANGE BETWEEN VERSIONS?
					var intensityNode = feature.SelectSingleNode(@"./UserParam[@name='masstrace_intensity_" + nr + "']");
					var intensityAttrib = intensityNode.Attributes["value"];

					var chromatogramPeakItem = new ChromatogramPeakItem()
					                           {
												   ID = EntityDataService.NextId<ChromatogramPeakItem>(),
						                           Mass = 0, //TODO? Mass if available?
												   ApexRT = rt,
												   LeftRT = Double.MaxValue,
												   RightRT = Double.MinValue,
												   Area = Double.Parse(intensityAttrib.Value),
												   IsotopeNumber = nr,
					                           };
				

					var pts = hull.SelectNodes(@"./pt");
					foreach (XmlNode pt in pts)
					{
						chromatogramPeakItem.Mass += Double.Parse(pt.Attributes["y"].Value);
						chromatogramPeakItem.LeftRT = Math.Min(chromatogramPeakItem.LeftRT, Double.Parse(pt.Attributes["x"].Value) / 60d);
						chromatogramPeakItem.RightRT = Math.Max(chromatogramPeakItem.RightRT, Double.Parse(pt.Attributes["x"].Value) / 60d);
					}
					
					chromatogramPeakItem.Mass /= pts.Count;

					chromatogramPeakItem.PeakModel = new DefaultPeakModel
					                                 {
						                                 LeftRT = chromatogramPeakItem.LeftRT,
														 RightRT = chromatogramPeakItem.RightRT,
														 ApexRT = chromatogramPeakItem.ApexRT
					                                 };

					peaks.Add(chromatogramPeakItem);
				}
				dict.Add(unknownCompoundIonInstanceItem, peaks);

				unknownCompoundIonInstanceItem.Area = peaks.Sum(s => s.Area);
				unknownCompoundIonInstanceItem.NumberOfMatchedIsotopes = peaks.Count;

			}
			return dict;
		}
	
        /// <summary>
        /// Imports the found features from the result file.
        /// </summary>
        /// <param name="pipelineParameterFileName">The name of the file which contains the pipeline settings.</param>
        /// <param name="fileId">The file identifier of the spectrum source file.</param>
        /// <returns>A dictionary containing a list of detected isotope peaks for each identified compound ion </returns>
        /// <exception cref="Thermo.Magellan.Exceptions.MagellanProcessingException"></exception>
        private Dictionary<UnknownFeatureIonInstanceItem, List<ChromatogramPeakItem>> ImportFoundFeatures(ConsensusXMLFile consensusXml, List<featureXMLFile> featureXmls)
        {
            //initialise stuff
            var timer = Stopwatch.StartNew();
            SendAndLogTemporaryMessage("Importing OpenMS results ...");
            RegisterEntityObjectTypes();

            //Mapping between element order in consensus file and related Thermo FileId. 
            //mapList size should always equal num_files
            int[] id_map = new int[m_numFiles];


			#region CreateMap for openms-file-id to cd-file-id
            //read in the consensus file
            XmlDocument doc = new XmlDocument();
            consensusXml.get_name();
            doc.Load(consensusXml.get_name());
            //Get mapping of each consensus element to FileId
            if (m_numFiles  == 1)
            {
                //if only one file then we extract the corresponding FileId directly from InputFiles
                id_map[0] = EntityDataService.CreateEntityItemReader().ReadAll<WorkflowInputFile>().ToList().First().FileID;
            }
            else
            {
                //read map section in consensusXml to determine which File at which position
                XmlNodeList mapList = doc.GetElementsByTagName("map");
                foreach (XmlElement map in mapList)
                {
                    string name = map.Attributes["name"].Value;
                    //more than one Input File/nonempty, which include the [FileID_...] string
                    var fileidStart = name.IndexOf("[FileID_");
                    var fileidLen = name.IndexOf("].") - name.IndexOf("[FileID_");
                    name = name.Substring(fileidStart, fileidLen).Split(new Char[] { '_' })[1];
                    id_map[Convert.ToInt32(map.Attributes["id"].Value)] = Convert.ToInt32(name);
                }
            }
			#endregion

			// Create Features
            var featureIonToPeaks = featureXmls.SelectMany((s,i) => CreateIons2Peaks(id_map[i], s)).ToDictionary(k=>k.Key,v=>v.Value);;

            var compoundToFeatureIon = CreateCompounds2Ions(featureIonToPeaks.Keys);
            
            // Insert items
			EntityDataService.InsertItems(featureIonToPeaks.Keys);
			EntityDataService.InsertItems(featureIonToPeaks.Values.SelectMany(s=>s));

            // Persists connections between ions and isotope peaks 
			EntityDataService.ConnectItems( featureIonToPeaks.Select(s=>Tuple.Create(s.Key,s.Value.AsEnumerable())) );

            //experimental compound
            EntityDataService.InsertItems(compoundToFeatureIon.Keys);
            EntityDataService.InsertItems(compoundToFeatureIon.Values.SelectMany(s => s));
            EntityDataService.ConnectItems(compoundToFeatureIon.Select(s => Tuple.Create(s.Key, s.Value.AsEnumerable())));


            // Get workflow input file and connect all components to the input file
			// Not done because one compound connected to n files


			var workflowInputFiles = EntityDataService.CreateEntityItemReader().ReadAll<WorkflowInputFile>().ToDictionary(k=>k.FileID,v=>v);
			EntityDataService.ConnectItems(featureIonToPeaks.Keys.Select(s=>Tuple.Create(s,workflowInputFiles[s.FileID])));
                        
            SendAndLogMessage("Importing OpenMS results took {0}.", StringHelper.GetDisplayString(timer.Elapsed));
            m_currentStep += m_numFiles;
            ReportTotalProgress((double)m_currentStep / m_numSteps);
	        return featureIonToPeaks;
        }

        
        private Dictionary<UnknownCompoundInstanceItem, List<UnknownFeatureIonInstanceItem>> CreateCompounds2Ions(Dictionary<UnknownFeatureIonInstanceItem, List<ChromatogramPeakItem>>.KeyCollection featureIons)
        {
            var dict = new Dictionary<UnknownCompoundInstanceItem, List<UnknownFeatureIonInstanceItem>>();
            var id_to_ion = new Dictionary<ulong, UnknownFeatureIonInstanceItem>();
            foreach (UnknownFeatureIonInstanceItem featureIon in featureIons){
                id_to_ion.Add(Convert.ToUInt64(featureIon.FeatureID), featureIon);
            }
            var m_dc_to_cons_dict = new Dictionary<ulong, List<ulong>>();
            var doc = new XmlDocument();
            doc.Load(m_decharge_cm.get_name());
            foreach (XmlElement consensusElement in doc.GetElementsByTagName("consensusElement")){
                //might read mass?
                var elements = new List<ulong>();
                ulong cent_id = Convert.ToUInt64(consensusElement.Attributes["id"].Value.Substring(2));

                var unknownCompoundInstanceItem = new UnknownCompoundInstanceItem()
                {
                    ID = EntityDataService.NextId<UnknownCompoundInstanceItem>(),
                    NumberOfAdducts = elements.Count
                };
                var ions = new List<UnknownFeatureIonInstanceItem>();
                foreach (XmlNode element in consensusElement.GetElementsByTagName("element")){
                    var fid = Convert.ToUInt64(element.Attributes["id"].Value);//already only number here
                    //elements.Add(fid);//fid=id of consensusXml consensus element
                    List <ulong> featureIonsIds =  m_cons_to_feat_dict[fid];
                    foreach (var id in featureIonsIds) {
                        UnknownFeatureIonInstanceItem featureIon = id_to_ion[id];
                        ions.Add(featureIon);
                        //connection compound<->ion?
                    }
                }
                //m_dc_to_cons_dict.Add(cent_id, elements);
                dict.Add(unknownCompoundInstanceItem, ions);
            }
            return dict;
        }

		/// <summary>
		/// Stores information about the used entity object types and connections.
		/// </summary>
		private void RegisterEntityObjectTypes()
		{
			// register items
			EntityDataService.RegisterEntity<UnknownFeatureIonInstanceItem>(ProcessingNodeNumber);
            EntityDataService.RegisterEntity<UnknownCompoundInstanceItem>(ProcessingNodeNumber);

			EntityDataService.RegisterEntity<XicTraceItem>(ProcessingNodeNumber);
			EntityDataService.RegisterEntity<ChromatogramPeakItem>(ProcessingNodeNumber);
			EntityDataService.RegisterEntity<MassSpectrumItem>(ProcessingNodeNumber);
			EntityDataService.RegisterEntity<RetentionTimeRasterItem>(ProcessingNodeNumber);

			// register basic connections                        
            EntityDataService.RegisterEntityConnection<UnknownFeatureIonInstanceItem, WorkflowInputFile>(ProcessingNodeNumber);
            EntityDataService.RegisterEntityConnection<UnknownFeatureIonInstanceItem, ChromatogramPeakItem>(ProcessingNodeNumber);

            EntityDataService.RegisterEntityConnection<UnknownCompoundInstanceItem, UnknownFeatureIonInstanceItem>(ProcessingNodeNumber);

            EntityDataService.RegisterEntityConnection<ChromatogramPeakItem, MassSpectrumItem>(ProcessingNodeNumber);
			EntityDataService.RegisterEntityConnection<UnknownFeatureIonInstanceItem, XicTraceItem>(ProcessingNodeNumber);
			EntityDataService.RegisterEntityConnection<XicTraceItem, RetentionTimeRasterItem>(ProcessingNodeNumber);
		}


    }


}

