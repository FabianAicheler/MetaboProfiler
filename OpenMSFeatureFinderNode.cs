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
//using Thermo.Magellan.BL.ReportEntityData; //for own table
//using Thermo.Metabolism.DataObjects.EntityDataObjects;


using OpenMS.OpenMSFile;
using Thermo.Metabolism.Processing.Services.Interfaces;
using Thermo.Metabolism.DataObjects.Constants;

namespace OpenMS.AdapterNodes
{
	#region Node Setup

    [ProcessingNode("{96E83A50-E4E4-4CD8-B2D2-E9B2FB7C2743}",
		Category = CDProcessingNodeCategories.UnknownCompounds,
        DisplayName = "MetaboProfiler",
		MainVersion = 1,
		MinorVersion = 001,
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
            DefaultValue = "1000")]
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

        [BooleanParameter(Category = "3. Output",
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
                            {"trace_termination_outliers", "2"}};
                

                ini_path = Path.Combine(NodeScratchDirectory, @"FeatureFinderMetaboDefault.ini");

                //SendAndLogMessage("Before default ini");
                create_default_ini(execPath, ini_path);
                //SendAndLogMessage("created default ini, before writeitem");
                WriteItem(ini_path, ffm_parameters);
                //SendAndLogMessage("after writeitem");

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
                    writeItemList(invars, ini_path, "in");
                    writeItemList(outvars, ini_path, "out");
                    write_MZ_RT_thresholds(ini_path);
                    SendAndLogMessage("Starting MapAlignerPoseClustering");
                    RunTool(execPath, ini_path);
                    m_currentStep += m_numFiles;
                    ReportTotalProgress((double)m_currentStep / m_numSteps);
                }

                //FeatureLinkerUnlabeledQT

                // outvars might be original featureXML, might be aligned.featureXML
                for (int i = 0; i < m_numFiles; i++)
                {
                    if (do_map_alignment.Value)
                    {
                        invars[i] = alignedFeatures[i].get_name();
                    }
                    else
                    {
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
                writeItemList(invars, ini_path, "in");
                write_MZ_RT_thresholds(ini_path);
                SendAndLogMessage("FeatureLinkerUnlabeledQT");
                RunTool(execPath, ini_path);
                m_currentStep += m_numFiles;
                ReportTotalProgress((double)m_currentStep / m_numSteps);
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

			XmlDocument origFeaturesDom = new XmlDocument();//
			origFeaturesDom.Load(origFeatures.get_name());

			XmlNodeList orig_featurelist = origFeaturesDom.SelectNodes(@"//feature");

			var dict = new Dictionary<UnknownFeatureIonInstanceItem, List<ChromatogramPeakItem>>();

			foreach (XmlElement feature in orig_featurelist)
			{

				var rtNode = feature.SelectSingleNode(@"./position[@dim=0]"); // RT
				var rt = Double.Parse(rtNode.InnerText) / 60d;
				var massNode = feature.SelectSingleNode(@"./position[@dim=1]"); // Mass
				var mass = Double.Parse(massNode.InnerText);

				var chargeNode = feature.SelectSingleNode(@"./charge"); // Charge
				var charge = Convert.ToInt32(chargeNode.InnerText);


				var id = feature.Attributes["id"].Value.Substring(2);

				var unknownCompoundIonInstanceItem = new UnknownFeatureIonInstanceItem()
				                                     {
					                                     ID = EntityDataService.NextId<UnknownFeatureIonInstanceItem>(),
														 FileID = fileId,
														 Mass =  mass,
														 RetentionTime = rt,
														 FeatureID = id,
														 Charge = charge
				                                     };

				var peaks = new List<ChromatogramPeakItem>();
				XmlNodeList hulls = feature.SelectNodes(@"./convexhull");

				foreach (XmlNode hull in hulls)
				{
					var nrAttrib = hull.Attributes["nr"];
					var nr = int.Parse(nrAttrib.Value);
					var intensityNode = feature.SelectSingleNode(@"./userParam[@name='masstrace_intensity_" + nr + "']");
					var intensityAttrib = intensityNode.Attributes["value"];

					var chromatogramPeakItem = new ChromatogramPeakItem()
					                           {
												   ID = EntityDataService.NextId<ChromatogramPeakItem>(),
						                           Mass = 0,
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

            // Insert items
			EntityDataService.InsertItems(featureIonToPeaks.Keys);
			EntityDataService.InsertItems(featureIonToPeaks.Values.SelectMany(s=>s));

            // Persists connections between ions and isotope peaks 
			EntityDataService.ConnectItems( featureIonToPeaks.Select(s=>Tuple.Create(s.Key,s.Value.AsEnumerable())) );

            // Get workflow input file and connect all components to the input file
			// Not done because one compound connected to n files


			var workflowInputFiles = EntityDataService.CreateEntityItemReader().ReadAll<WorkflowInputFile>().ToDictionary(k=>k.FileID,v=>v);
			EntityDataService.ConnectItems(featureIonToPeaks.Keys.Select(s=>Tuple.Create(s,workflowInputFiles[s.FileID])));
                        
            SendAndLogMessage("Importing OpenMS results took {0}.", StringHelper.GetDisplayString(timer.Elapsed));
            m_currentStep += m_numFiles;
            ReportTotalProgress((double)m_currentStep / m_numSteps);
	        return featureIonToPeaks;
        }

		/// <summary>
		/// Stores information about the used entity object types and connections.
		/// </summary>
		private void RegisterEntityObjectTypes()
		{
			// register items
			EntityDataService.RegisterEntity<UnknownFeatureIonInstanceItem>(ProcessingNodeNumber);
			EntityDataService.RegisterEntity<XicTraceItem>(ProcessingNodeNumber);
			EntityDataService.RegisterEntity<ChromatogramPeakItem>(ProcessingNodeNumber);
			EntityDataService.RegisterEntity<MassSpectrumItem>(ProcessingNodeNumber);
			EntityDataService.RegisterEntity<RetentionTimeRasterItem>(ProcessingNodeNumber);

			// register basic connections                        
            EntityDataService.RegisterEntityConnection<UnknownFeatureIonInstanceItem, WorkflowInputFile>(ProcessingNodeNumber);
            EntityDataService.RegisterEntityConnection<UnknownFeatureIonInstanceItem, ChromatogramPeakItem>(ProcessingNodeNumber);
            EntityDataService.RegisterEntityConnection<ChromatogramPeakItem, MassSpectrumItem>(ProcessingNodeNumber);
			EntityDataService.RegisterEntityConnection<UnknownFeatureIonInstanceItem, XicTraceItem>(ProcessingNodeNumber);
			EntityDataService.RegisterEntityConnection<XicTraceItem, RetentionTimeRasterItem>(ProcessingNodeNumber);
		}


    }


}

