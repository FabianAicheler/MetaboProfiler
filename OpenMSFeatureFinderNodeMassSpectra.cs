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

    public partial class OpenMSMetaboProfilerNode : ProcessingNode<UnknownFeatureConsolidationProvider, ConsensusXMLFile>,
        IResultsSink<MassSpectrumCollection>
	{        		

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

			DetectedPeakDetailsHelper.AssignSpectrumTreesToPeakDetails(spectrumDescriptors.OfType<ISpectrumDescriptor>(), peakDetails);

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

    }

}

