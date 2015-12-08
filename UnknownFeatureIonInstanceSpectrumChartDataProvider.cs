using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Thermo.CD.DataReviewModule.SpectrumChartDataProvider;
using Thermo.Magellan.EntityDataFramework;
using Thermo.Magellan.MassSpec;
using Thermo.Magellan.Utilities;
using Thermo.Metabolism.DataObjects;
using Thermo.Metabolism.DataObjects.EntityDataObjects;

namespace OpenMS.AdapterNodes
{
    [SpectrumChartDataProviderFactoryExport(typeof(UnknownFeatureIonInstanceItem))]
    public class ExpectedCompoundSpectrumChartDataProviderFactory : ISpectrumChartDataProviderFactory
    {
        /// <summary>
        /// Creates the a <see cref="ISpectrumChartDataProvider" /> instance for entities of type <see cref="UnknownFeatureIonInstanceItem"/>.
        /// </summary>
        /// <param name="entityDataService">The entity data service to be used to retrieve the spectrum data.</param>
        /// <param name="entityItemData">A sequence of entity item data for which the spectrum chart data should be provided for.</param>
        /// <returns>
        /// A new created <see cref="ISpectrumChartDataProvider" /> instance.
        /// </returns>
        public ISpectrumChartDataProvider Create(IEntityDataService entityDataService, IEnumerable<EntityItemData> entityItemData)
        {
            return new UnknownFeatureIonInstanceSpectrumChartDataProvider(entityDataService, entityItemData);
        }
    }

    public class UnknownFeatureIonInstanceSpectrumChartDataProvider : SpectrumChartDataProvider
    {
        private readonly string m_extraHeaderLine;
        private readonly IList<UnknownFeatureIonInstanceItem> m_unknownFeatureIonInstanceItems;
        private readonly Dictionary<object[], IList<ChromatogramPeakItem>> m_chromatogramPeakItemsMap;
        private readonly IEntityDataService m_entityDataService;
        private readonly Dictionary<int, string> m_fileNames = new Dictionary<int, string>();

        public UnknownFeatureIonInstanceSpectrumChartDataProvider(IEntityDataService entityDataService, IEnumerable<EntityItemData> entityItemData)
        {
            ArgumentHelper.AssertNotNull(entityItemData, "entityItemData");

            m_entityDataService = entityDataService;

            var entityReader = entityDataService.CreateEntityItemReader();
            var compoundIonInstanceItem = entityReader.Read<UnknownFeatureIonInstanceItem>(entityItemData.First().GetEntityIds());

            // make spectrum header
            m_extraHeaderLine = String.Format(
                            "Ion: {0}, m/z: {1:F5}, Area: {2:F0}",
                            compoundIonInstanceItem.IonDescription,
                            compoundIonInstanceItem.Mass,
                            compoundIonInstanceItem.Area);

            // set ion
            m_unknownFeatureIonInstanceItems = new[] { compoundIonInstanceItem };

            // get reader
            entityReader = entityDataService.CreateEntityItemReader();
            m_fileNames = entityReader.ReadAll<WorkflowInputFile>().ToDictionary(file => file.FileID, file => Path.GetFileNameWithoutExtension(file.FileName));

            // load related chromatographic peak items
            var peakItems = entityReader.ReadFlat<UnknownFeatureIonInstanceItem, ChromatogramPeakItem>(compoundIonInstanceItem).Item2;
            m_chromatogramPeakItemsMap = new Dictionary<object[], IList<ChromatogramPeakItem>>(IdArrayComparer.Instance) { { compoundIonInstanceItem.GetIDs(), peakItems } };

            // load related spectrum tree
            SpectralTreeNodes = LoadSpectralTree();
        }

        /// <summary>
        /// Loads spectral tree for current compound.
        /// </summary>
        private IEnumerable<MassSpectrumItemTreeNode> LoadSpectralTree()
        {
            // get spectra for each ion
            if ((m_unknownFeatureIonInstanceItems != null) && (m_unknownFeatureIonInstanceItems.Any()))
            {
                // check path
                IList<string> shortestConnectingPath;
                if (m_entityDataService.TryGetShortestConnection<UnknownFeatureIonInstanceItem, MassSpectrumItem>(out shortestConnectingPath))
                {
                    // init container
                    var spectra = new List<MassSpectrumItem>();

                    // init reader
                    var readerSettings = FlatConnectedReaderSettings.Create(shortestConnectingPath);
                    var entityReader = m_entityDataService.CreateEntityItemReader();

                    // get spectra for each ion
                    foreach (var ionInstanceItem in m_unknownFeatureIonInstanceItems)
                    {
                        // get spectra
                        var connectedSpectra = entityReader.ReadFlat<UnknownFeatureIonInstanceItem, MassSpectrumItem>(
                            ionInstanceItem, readerSettingsT2: readerSettings);

                        // store spectra
                        if (connectedSpectra != null)
                        {
                            spectra.AddRange(connectedSpectra.Item2);
                        }
                    }

                    // use unique spectra only
                    var uniqueSpectra = spectra.GroupBy(g => g.Spectrum.Header.SpectrumID).Select(s => s.First()).ToList();

                    // build tree
                    return MassSpectrumItemTreeNode.BuildSpectralTrees(uniqueSpectra);
                }
            }

            return Enumerable.Empty<MassSpectrumItemTreeNode>();
        }

        public override SpectrumChartData CreateSpectrumDetails(MassSpectrumItem massSpectrumItem, MassSpectrum referenceSpectrum = null)
        {
            ArgumentHelper.AssertNotNull(massSpectrumItem, "massSpectrumItem");

            // clone given spectrum
            var spectrum = massSpectrumItem.Spectrum.Clone();
            if (spectrum == null)
            {
                return null;
            }

            //// get ions for respective polarity
            //var ions = spectrum.ScanEvent.Polarity == PolarityType.Negative
            //	? m_unknownFeatureIonInstanceItems.Where(w => w.Charge < 0)
            //	: m_unknownFeatureIonInstanceItems.Where(w => w.Charge > 0);

            // annotate nearest centroids
            foreach (var ion in m_unknownFeatureIonInstanceItems)
            {
                // annotate isotopes
                foreach (var peak in m_chromatogramPeakItemsMap[ion.GetIDs()])
                {
                    var centroid = spectrum.PeakCentroids.FindClosestPeak(peak.Mass);
                    if (centroid != null)
                    {
                        centroid.DisplayPriority = 2;
                    }
                }
            }

            // create spectrum chart data
            var massRange = Range.Create(spectrum.Header.LowPosition, spectrum.Header.HighPosition);
            if (spectrum.ScanEvent.MSOrder == MSOrderType.MS1)
            {
                var peaks = m_chromatogramPeakItemsMap.SelectMany(s => s.Value).ToList();
                massRange = Range.Create(Math.Max(0, peaks.Min(m => m.Mass)) - 4, peaks.Max(m => m.Mass) + 5);
            }

            return new SpectrumChartData
            {
                MassRange = massRange,
                SpectrumDistanceDetails = null,
                Spectrum = spectrum,
                SpectrumHeaderText = CreateSpectrumChartHeader(spectrum, m_extraHeaderLine, m_fileNames[spectrum.Header.FileID])
            };
        }

        public override MassSpectrumItem GetInitalSelectedSpectrum(MSOrderType msOrder)
        {
            return SelectSpectralTreeNode(m_unknownFeatureIonInstanceItems.First().RetentionTime, msOrder);
        }
    }
}