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
		MinorVersion = 0,
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
    public class OpenMSFeatureFinderNode : ProcessingNode<UnknownFeatureConsolidationProvider, ConsensusXMLFile>,
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
                            {"noise_threshold_int", NoiseThreshold.ToString()}};

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

        private void create_default_ini(string execPath, string ini_path)
        {
            var timer = Stopwatch.StartNew();

                        
            var ini_loc = String.Format("\"{0}\"", ini_path);
            //SendAndLogMessage(execPath + NodeScratchDirectory + ini_loc);


            var process = new Process
            {
                StartInfo =
                {
                    FileName = execPath,
                    WorkingDirectory = NodeScratchDirectory,
                    Arguments = " -write_ini " + String.Format("\"{0}\"", ini_path) ,
                    UseShellExecute = false,
                    RedirectStandardOutput = false,
                    CreateNoWindow = false
                }
            };

            try
            {
                var openMS_shared_dir = Path.Combine(ServerConfiguration.ToolsDirectory, "OpenMS-2.0/share/OpenMS");
                process.StartInfo.EnvironmentVariables["OPENMS_DATA_PATH"] = openMS_shared_dir;
                process.Start();

                try
                {
                    //SendAndLogMessage("in try");
                    process.Refresh();
                    //process.PriorityClass = ProcessPriorityClass.BelowNormal;//causes exception
                    process.WaitForExit();
                    //SendAndLogMessage("end try");
                }
                catch (InvalidOperationException ex)
                {
                    //SendAndLogMessage("in invalidoperation exception");
                    NodeLogger.ErrorFormat(ex, "The following exception is raised during the execution of \"{0}\":", execPath);
                    throw;
                }

                if (process.ExitCode != 0)
                {
                    throw new MagellanProcessingException(
                        String.Format(
                            "The exit code of {0} was {1}. (The expected exit code is 0)",
                            Path.GetFileName(process.StartInfo.FileName), process.ExitCode));
                }
            }
            catch (System.Threading.ThreadAbortException)
            {
                //SendAndLogMessage("in abort throw");
                throw;
            }
            catch (Exception ex)
            {
                //SendAndLogMessage("in abort catch");
                NodeLogger.ErrorFormat(ex, "The following exception is raised during the execution of \"{0}\":", execPath);
                throw;
            }
            finally
            {
                if (!process.HasExited)
                {
                    NodeLogger.WarnFormat(
                        "The process [{0}] isn't finished correctly -> force the process to exit now", process.StartInfo.FileName);
                    process.Kill();
                }
            }
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

		/// <summary>
		/// Creates and persists XIC traces for all compound ion items.
		/// </summary>
		private void RebuildAndPersistCompoundIonTraces(
			int fileID,
			IEnumerable<SpectrumDescriptor> spectrumDescriptors,
			Dictionary<UnknownFeatureIonInstanceItem, List<ChromatogramPeakItem>> ionInstanceToPeaksMap)
		{
			SendAndLogTemporaryMessage("Re-creating XIC traces...");
			var time = Stopwatch.StartNew();

			// init XICPattern builder
			var xicPatternBuilder = new Func<List<ChromatogramPeakItem>, XICPattern>(
				peaks =>
				{
					var masks = peaks.Select(
						peak => new XICMask(
							peak.IsotopeNumber,
							peak.Mass,
							MassTolerance.Value)
						).ToList();

					return new XICPattern(masks);
				});

			// make XIC patterns
			var xicPatterns = ionInstanceToPeaksMap.ToDictionary(item => item.Key, item => xicPatternBuilder(item.Value));

			// init XIC tracer
			var tracer = new XICTraceGenerator<UnknownFeatureIonInstanceItem>(xicPatterns);

			// get sprectrum IDs
			var spectrumIds = spectrumDescriptors
				.Where(s => s.ScanEvent.MSOrder == MSOrderType.MS1)
				.OrderBy(o => o.Header.RetentionTimeRange.LowerLimit)
				.Select(s => s.Header.SpectrumID)
				.ToList();

			// add spectrum to tracer
			foreach (var spectrum in ProcessingServices.SpectrumProcessingService.ReadSpectraFromCache(spectrumIds))
			{
				tracer.AddSpectrum(spectrum);
			}

			// make trace items
			var ionInstanceToTraceMap = new Dictionary<UnknownFeatureIonInstanceItem, XicTraceItem>();
			foreach (var item in ionInstanceToPeaksMap)
			{
				// get trace
				var trace = tracer.GetXICTrace(item.Key, useFullRange: true, useFullRaster: false);

				// make XicTraceItem
				ionInstanceToTraceMap.Add(
					item.Key,
					new XicTraceItem
					{
						ID = EntityDataService.NextId<XicTraceItem>(),
						FileID = fileID,
						Trace = new TraceData(trace),
					});
			}

			// make raster
			var rasterItem = MakeRetentionTimeRasterItem(tracer, fileID);

			// persist traces 
			EntityDataService.InsertItems(ionInstanceToTraceMap.Values);
			EntityDataService.ConnectItems(ionInstanceToTraceMap.Select(s => Tuple.Create(s.Key, s.Value)));

			// persist raster
			EntityDataService.InsertItems(new[] { rasterItem });
			EntityDataService.ConnectItems(ionInstanceToTraceMap.Select(s => Tuple.Create(s.Value, rasterItem)));

			time.Stop();
			SendAndLogVerboseMessage("Re-creating and persisting {0} XIC traces took {1:F2} s.", ionInstanceToTraceMap.Values.Count, time.Elapsed.TotalSeconds);
			m_currentStep += 4;
			ReportTotalProgress((double)m_currentStep / m_numSteps);
		}

		
		/// <summary>
		/// Creates RetentionTimeRasterItem from XICTraceGenerator.
		/// </summary>
		private RetentionTimeRasterItem MakeRetentionTimeRasterItem(XICTraceGenerator<UnknownFeatureIonInstanceItem> tracer, int fileID)
		{
			// get raster points
			var raster = tracer.RetentionTimeRaster;

			// get raster info
			var info = tracer.RetentionTimeRasterInfo;

			// make RetentionTimeRasterItem
			return new RetentionTimeRasterItem
			{
				ID = EntityDataService.NextId<RetentionTimeRasterItem>(),
				FileID = fileID,
				MSOrder = info.MSOrder,
				Polarity = info.Polarity,
				IonizationSource = info.IonizationSource,
				MassAnalyzer = info.MassAnalyzer,
				MassRange = info.MassRange,
				ResolutionAtMass200 = info.ResolutionAtMass200,
				ScanRate = info.ScanRate,
				ScanType = info.ScanType,
				ActivationTypes = info.ActivationTypes,
				ActivationEnergies = info.ActivationEnergies,
				IsolationWindow = info.IsolationWindow,
				IsolationMass = info.IsolationMass,
				IsolationWidth = info.IsolationWidth,
				IsolationOffset = info.IsolationOffset,
				IsMultiplexed = info.IsMultiplexed,
				Trace = new TraceData(raster),
			};
		}

		
        #region MassSpectra
        /// <summary>
		/// Assigns to each detected chromatogram peak the nearest MS1 spectrum and all related data dependent spectra and persists the spectra afterwards.
		/// </summary>
		/// <param name="spectrumDescriptors">The spectrum descriptors group by file identifier.</param>
		/// <param name="compoundIon2IsotopePeaksDictionary">The detected peaks for each compound ion as dictionary.</param>
		private void AssignAndPersistMassSpectra(IEnumerable<SpectrumDescriptor> spectrumDescriptors, IEnumerable<KeyValuePair<UnknownFeatureIonInstanceItem, List<ChromatogramPeakItem>>> compoundIon2IsotopePeaksDictionary)
		{
			var time = Stopwatch.StartNew();
			SendAndLogTemporaryMessage("Assigning MS1 spectra...");

	        var defaultCharge = 1;
			if (spectrumDescriptors.First().ScanEvent.Polarity == PolarityType.Negative)
			{
				defaultCharge = -1;
			}

			var orderedSpectrumDescriptors = spectrumDescriptors
				.OrderBy(o => o.Header.RetentionTimeCenter)
				.ToList();

			if (orderedSpectrumDescriptors.Any(a => a.ScanEvent.MSOrder == MSOrderType.MS1) == false)
			{
				SendAndLogErrorMessage("Exception, MS1 spectra not available (check spectrum selector node).");
				return;
			}

			// Create peak details for each detected peak using the charge of the related compound ion
			var detectedPeakDetails = compoundIon2IsotopePeaksDictionary.SelectMany(
				sm => sm.Value.Select(
					s => new DetectedPeakDetails(s)
					{
						Charge = sm.Key.Charge == 0 ? defaultCharge : sm.Key.Charge 
					}))
					.ToList();

			DetectedPeakDetailsHelper.AssignMassSpectraToPeakApexes(orderedSpectrumDescriptors, detectedPeakDetails);

			SendAndLogMessage("Assigning MS1 spectra to chromatogram peak apexes finished after {0}", StringHelper.GetDisplayString(time.Elapsed));
			time.Restart();

			BuildSpectralTrees(orderedSpectrumDescriptors, detectedPeakDetails);

			SendAndLogTemporaryMessage("Persisting assigned spectra...");


			// get spectrum ids of distinct spectra
			var distinctSpectrumIds = detectedPeakDetails.SelectMany(s => s.AssignedSpectrumDescriptors)
													 .Select(s => s.Header.SpectrumID)
													 .Distinct()
													 .ToList();

			// Divide spectrum ids into parts to reduce the memory foot print. Therefore it is necessary to interrupt the spectra reading,
			// because otherwise a database locked exception will be thrown when storing the spectra			
			foreach (var spectrumIdsPartition in distinctSpectrumIds
				.Partition(ServerConfiguration.ProcessingPacketSize))
			{
				// Retrieve mass spectra and create MassSpectrumItem's
				var distinctSpectra =
					ProcessingServices.SpectrumProcessingService.ReadSpectraFromCache(spectrumIdsPartition.ToList())
									  .Select(
										  s => new MassSpectrumItem
										  {
											  ID = s.Header.SpectrumID,
											  FileID = s.Header.FileID,
											  Spectrum = s
										  })
									  .ToList();

				// Persist mass spectra
				PersistMassSpectra(distinctSpectra);
			}

			// Persists peak <-> mass spectrum connections

			var peaksToMassSpectrumConnectionList = new List<EntityConnectionItemList<ChromatogramPeakItem, MassSpectrumItem>>(detectedPeakDetails.Count);

			// Get connections between spectrum and chromatographic peak
			foreach (var item in detectedPeakDetails
				.Where(w => w.AssignedSpectrumDescriptors.Any()))
			{
				var connection = new EntityConnectionItemList<ChromatogramPeakItem, MassSpectrumItem>(item.Peak);

				peaksToMassSpectrumConnectionList.Add(connection);

				foreach (var spectrumDescriptor in item.AssignedSpectrumDescriptors)
				{
					connection.AddConnection(new MassSpectrumItem
					{
						ID = spectrumDescriptor.Header.SpectrumID,
						FileID = spectrumDescriptor.Header.FileID,
						// Omit mass spectrum here to reduce the memory footprint (only the IDs are required to persist the connections)
					});
				}
			}

			// Persists peak <-> mass spectrum connections
			EntityDataService.ConnectItems(peaksToMassSpectrumConnectionList);

			SendAndLogMessage("Persisting spectra finished after {0}", StringHelper.GetDisplayString(time.Elapsed));
			m_currentStep += 1;
            ReportTotalProgress((double)m_currentStep / m_numSteps);
			time.Stop();
		}

		/// <summary>
		/// Builds and assigns the spectral trees for each detected chromatogram peak
		/// </summary>
		/// <remarks>
		/// For each detected peak, spectral trees are generated from the MS1 scans including all matching data dependent scans eluting in that retention time range. 
		/// Matching means that the precursor mass of the MS2 scan must match the mass used to create the chromatogram trace within the given mass tolerance. Finally all 
		/// spectral tree with matching data dependent scans are assigned to the detected peak.
		/// </remarks>
		private void BuildSpectralTrees(IEnumerable<SpectrumDescriptor> spectrumDescriptors, IEnumerable<DetectedPeakDetails> peakDetails)
		{
			SendAndLogTemporaryMessage("Building spectral trees...");
			var timer = Stopwatch.StartNew();

			DetectedPeakDetailsHelper.AssignSpectrumTreesToPeakDetails(spectrumDescriptors.OfType<ISpectrumDescriptor>(), peakDetails, MassTolerance.Value);

			timer.Stop();
			SendAndLogMessage("Building spectral trees takes {0:F2} s.", timer.Elapsed.TotalSeconds);

		}

		/// <summary>
		/// Writes all mass spectra to the result database file.		
		/// </summary>
		/// <param name="massSpectrumItems">The mass spectra to persist.</param>
		private void PersistMassSpectra(IEnumerable<MassSpectrumItem> massSpectrumItems)
		{
			var unionEntityDataPersistenceService = ProcessingServices.Get<IUnionEntityDataPersistenceService>();
			unionEntityDataPersistenceService.InsertItems(massSpectrumItems);
		}
        #endregion



        #region stuff_for_tools
        private void WriteItem(string ini_path, Dictionary<string, string> parameters)
        {
            XmlDocument doc = new XmlDocument();
            doc.Load(ini_path);
            XmlNodeList nlist = doc.GetElementsByTagName("ITEM");
            foreach (XmlNode item in nlist)
            {
                foreach (string param in parameters.Keys)
                {
                    if (item.Attributes["name"].Value == param)
                    {
                        item.Attributes["value"].Value = parameters[param];
                    }
                }
            }
            //doc.Save(Path.Combine(NodeScratchDirectory, "ToolParameters.xml"));
            doc.Save(ini_path);
        }

        //Write ITEMLISTs, used for input or output file lists
        private static void writeItemList(string[] vars, string ini_path, string mode)
        {
            XmlDocument doc = new XmlDocument();
            doc.Load(ini_path);
            XmlNodeList itemlist = doc.GetElementsByTagName("ITEMLIST");
            foreach (XmlNode item in itemlist)
            {
                //mode: in or out?
                if (item.Attributes["name"].Value == mode)
                {
                    foreach (var fn in vars)
                    {
                        //We add LISTITEMS to until then empty ITEMLISTS
                        var listitem = doc.CreateElement("LISTITEM");
                        XmlAttribute newAttribute = doc.CreateAttribute("value");
                        newAttribute.Value = fn;
                        listitem.SetAttributeNode(newAttribute);
                        item.AppendChild(listitem);
                    }
                }
            }
            doc.Save(ini_path);
        }

        //Write mz and rt parameters. different function than WriteItem due to specific structure in considered tools
        private void write_MZ_RT_thresholds(string ini_path)
        {
            XmlDocument doc = new XmlDocument();
            doc.Load(ini_path);
            XmlNodeList nlist = doc.GetElementsByTagName("ITEM");
            foreach (XmlNode item in nlist)
            {
                if ((item.ParentNode.Attributes["name"].Value == "distance_MZ") && (item.Attributes["name"].Value == "max_difference"))
                {
                    var mzthresh = MZThreshold.ToString();
                    item.Attributes["value"].Value = mzthresh;
                }
                else if ((item.ParentNode.Attributes["name"].Value == "distance_MZ") && (item.Attributes["name"].Value == "unit"))
                {
                    //always use ppm
                    item.Attributes["value"].Value = "ppm";
                }
                else if ((item.ParentNode.Attributes["name"].Value == "distance_RT") && (item.Attributes["name"].Value == "max_difference"))
                {
                    item.Attributes["value"].Value = (RTThreshold.Value * 60).ToString(); //need to convert minute(CD) to seconds(OpenMS)!
                }
            }
            doc.Save(ini_path);
        }

        //execute specific OpenMS Tool (execPath) with specified Ini (ParamPath)        
        private void RunTool(string execPath, string ParamPath)
        {

            var timer = Stopwatch.StartNew();

            var process = new Process
            {
                StartInfo =
                {
                    FileName = execPath,
                    WorkingDirectory = NodeScratchDirectory,
                    //WorkingDirectory = FileHelper.GetShortFileName(NodeScratchDirectory), 

                    Arguments = " -ini " + String.Format("\"{0}\"",ParamPath),
                    UseShellExecute = false,
                    RedirectStandardOutput = true,
                    CreateNoWindow = false,
                    //WindowStyle = ProcessWindowStyle.Hidden
                }
            };

            var openMS_shared_dir = Path.Combine(ServerConfiguration.ToolsDirectory, "OpenMS-2.0/share/OpenMS");
            process.StartInfo.EnvironmentVariables["OPENMS_DATA_PATH"] = openMS_shared_dir;

            SendAndLogTemporaryMessage("Starting process [{0}] in working directory [{1}] with arguments [{2}]",
                            process.StartInfo.FileName, process.StartInfo.WorkingDirectory, process.StartInfo.Arguments);

            WriteLogMessage(MessageLevel.Debug,
                            "Starting process [{0}] in working directory [{1}] with arguments [{2}]",
                            process.StartInfo.FileName, process.StartInfo.WorkingDirectory, process.StartInfo.Arguments);
                        
            try
            {
                process.Start();

                try
                {
                    //process.PriorityClass = ProcessPriorityClass.BelowNormal;

                    string current_work = "";
                    while (process.HasExited == false)
                    {
                        var output = process.StandardOutput.ReadLine();


                        // move on if no new announcement. 
                        if (String.IsNullOrEmpty(output))
                        {
                            continue;
                        }

                        //store all results (for now?) of OpenMS Tool output
                        WriteLogMessage(MessageLevel.Debug, output);

                        // Parse the output and report progress using the method SendAndLogTemporaryMessage
                        if (output.Contains(@"Progress of 'loading mzML file':"))
                        {
                            current_work = "Progress of 'loading mzML file':";
                        }
                        else if (output.Contains("Progress of 'loading chromatograms':"))
                        {
                            current_work = "Progress of 'loading chromatograms':";
                        }
                        else if (output.Contains("Progress of 'mass trace detection':"))
                        {
                            current_work = "Progress of 'mass trace detection':";
                        }
                        else if (output.Contains("Progress of 'elution peak detection':"))
                        {
                            current_work = "Progress of 'elution peak detection':";
                        }
                        else if (output.Contains("Progress of 'assembling mass traces to features':"))
                        {
                            current_work = "Progress of 'assembling mass traces to features':";
                        }
                        else if (output.Contains("Progress of 'Aligning input maps':"))
                        {
                            current_work = "Progress of 'Aligning input maps':";
                        }
                        else if (output.Contains("Progress of 'linking features':"))
                        {
                            current_work = "Progress of 'linking features':";
                        }
                        else if (output.Contains("%"))
                        {
                            SendAndLogTemporaryMessage("{0} {1}", current_work, output);
                        }

                    }


                    // Note: The child process waits until everything is read from the standard output -> A Deadlock could arise here
                    using (var reader = new StringReader(process.StandardOutput.ReadToEnd()))
                    {
                        string output;

                        while ((output = reader.ReadLine()) != null)
                        {
                            WriteLogMessage(MessageLevel.Debug, output);

                            if (String.IsNullOrEmpty(output) == false)
                            {
                                SendAndLogMessage(output, false);
                            }
                        }
                    }

                    process.WaitForExit();
                }
                catch (InvalidOperationException ex)
                {
                    NodeLogger.ErrorFormat(ex, "The following exception is raised during the execution of \"{0}\":", execPath);
                    throw;
                }


                if (process.ExitCode != 0)
                {
                    throw new MagellanProcessingException(
                        String.Format(
                            "The exit code of {0} was {1}. (The expected exit code is 0)",
                            Path.GetFileName(process.StartInfo.FileName), process.ExitCode));
                }
            }
            catch (System.Threading.ThreadAbortException)
            {
                throw;
            }
            catch (Exception ex)
            {
                NodeLogger.ErrorFormat(ex, "The following exception is raised during the execution of \"{0}\":", execPath);
                throw;
            }
            finally
            {
                if (!process.HasExited)
                {
                    NodeLogger.WarnFormat(
                        "The process [{0}] isn't finished correctly -> force the process to exit now", process.StartInfo.FileName);
                    process.Kill();
                }
            }

            SendAndLogMessage("{0} tool processing took {1}.", execPath, StringHelper.GetDisplayString(timer.Elapsed));

        }
        #endregion

    }


}

