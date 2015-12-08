using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Linq;
using System.Threading;
using Thermo.CD.DataReviewModule.ChromatogramChartDataProvider;
using Thermo.Discoverer.Infrastructure.CommonControls.ChromatogramCharts;
//using Thermo.Discoverer.Infrastructure.WinControls;
using Thermo.Magellan.EntityDataFramework;
using Thermo.Magellan.Services.Interfaces;
using Thermo.Metabolism.DataObjects;
using Thermo.Metabolism.DataObjects.EntityDataObjects;

namespace OpenMS.AdapterNodes
{
    /// <summary>
    /// ChromatogramChartDataProviderFactory for UnknownFeatureIonInstanceItems
    /// </summary>
    [ChromatogramChartDataProviderFactory(typeof(UnknownFeatureIonInstanceItem))]
    public class UnknownFeatureIonInstanceItemFactory : IChromatogramChartDataProviderFactory
    {


        [Import(typeof(IReportFileInformationService))]
        private IReportFileInformationService ReportFileInformationService { get; set; }

        /// <summary>
        ///  Creates a new chromatogram trace provider for each entity item data using the specified creation arguments.
        /// </summary>
        /// <param name="args">A <see cref="ChromatogramChartDataProviderCreatorArgs"/> instance containing the additional information.</param>
        /// <param name="cancellationToken">The token to monitor for cancellation requests.</param>
        /// <param name="entityItemData">The entity item data for which to create chromatogram data providers</param>
        /// <returns>A list of <see cref="IChromatogramChartDataProvider"/> instances.</returns>
        public IReadOnlyList<IChromatogramChartDataProvider> Create(
            ChromatogramChartDataProviderCreatorArgs args,
            IReadOnlyList<EntityItemData> entityItemData,
            CancellationToken cancellationToken)
        {
            return
                ChromatogramChartDataProvider.RetrieveTracesAndPeaks<UnknownFeatureIonInstanceItem, ChromatogramPeakItem>(
                    args.EntityDataService,
                    entityItemData).Select(
                        s =>
                            new ChromatogramChartDataProvider<UnknownFeatureIonInstanceItem>(
                                s.Item1,
                                PrepareCurveToolTip,
                                PreparePointToolTip,
                                s.Item2.Values.SelectMany(s2 => s2).SumPoints(),
                                s.Item3,
                                s.Item2.Keys.First().Item1,
                                s.Item2.Keys.First().Item2,
                                ReportFileInformationService)
                            {
                                CurveColor = System.Windows.Media.Colors.Orange,
                                CurveId = 0
                            }).ToList();
        }

        /// <summary>
        /// Prepares the curve tooltip.
        /// </summary>
        public string PrepareCurveToolTip(
            UnknownFeatureIonInstanceItem unknownCompoundInstanceItem,
            RetentionTimeRasterItem retentionTimeRasterItem,
            WorkflowInputFile workflowInputFile,
            string studyFileID)
        {
            return string.Format(
                "MW: {0:F5} [Da]\nFile: {1}",
                unknownCompoundInstanceItem.MolecularWeight,
                workflowInputFile.FileName);
        }

        /// <summary>
        /// Prepares the point tooltip.
        /// </summary>
        protected string PreparePointToolTip(
            TracePoint tracePoint,
            UnknownFeatureIonInstanceItem unknownCompoundInstanceItem,
            RetentionTimeRasterItem retentionTimeRasterItem,
            WorkflowInputFile workflowInputFile,
            string studyFileID)
        {
            return string.Format(
                "MW: {0:F5} [Da]\nArea: {1:F0}\nRT: {2:F2} [min]\nIntensity: {3:F0} [count]\nFile: {4}",
                unknownCompoundInstanceItem.MolecularWeight,
                unknownCompoundInstanceItem.Area,
                tracePoint.Time,
                tracePoint.Intensity,
                workflowInputFile.FileName);
        }
    }
}
