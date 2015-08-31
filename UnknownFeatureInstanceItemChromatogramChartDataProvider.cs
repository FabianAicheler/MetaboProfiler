using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Thermo.CD.DataReviewModule.ChromatogramChartDataProvider;
using Thermo.Discoverer.Infrastructure.CommonControls.ChromatogramCharts;
using Thermo.Magellan.EntityDataFramework;
using Thermo.Magellan.Utilities;
using Thermo.Metabolism.DataObjects;
using Thermo.Metabolism.DataObjects.EntityDataObjects;

namespace OpenMS.AdapterNodes
{

    public class UnknownFeatureInstanceItemChromatogramChartDataProvider : ChromatogramChartDataProvider
    {
        private readonly UnknownFeatureIonInstanceItem m_unknownCompoundInstanceItem;

        /// <summary>
        /// Initializes a new instance of the UnknownCompoundInstanceItemChromatogramChartDataProvider class.
        /// </summary>
        public UnknownFeatureInstanceItemChromatogramChartDataProvider(
            UnknownFeatureIonInstanceItem unknownCompoundInstanceItem,
            IEnumerable<TracePoint> tracePoints,
            IEnumerable<ChromatogramPeakItem> chromatogramPeakItems,
            WorkflowInputFile workflowInputFile)
            : base(
                tracePoints,
                chromatogramPeakItems,
                unknownCompoundInstanceItem,
                workflowInputFile)
        {
            ArgumentHelper.AssertNotNull(unknownCompoundInstanceItem, "unknownCompoundInstanceItem");
            m_unknownCompoundInstanceItem = unknownCompoundInstanceItem;
        }


	    /// <summary>
		/// ChromatogramChartDataProviderFactory for UnknownFeatureIonInstanceItems
	    /// </summary>
	    [ChromatogramChartDataProviderFactory(typeof(UnknownFeatureIonInstanceItem))]
	    public class Factory : IChromatogramChartDataProviderFactory
	    {
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
				return RetrieveTracesAndPeaks<UnknownFeatureIonInstanceItem, ChromatogramPeakItem>(args.EntityDataService, entityItemData).Select(
				    s =>
				    new UnknownFeatureInstanceItemChromatogramChartDataProvider(s.Item1, s.Item2.Values.SelectMany(s2 => s2).SumPoints(), s.Item3, s.Item2.Keys.First())
				    {
					    CurveColor = System.Windows.Media.Colors.Orange,
					    CurveId = 0
				    }).ToList();
		    }
	    }


	    /// <summary>
		/// Prepares the curve tooltip.
		/// </summary>
		public override string PrepareCurveToolTip()
		{
			return string.Format(
				"MW: {0:F5} [Da]\nFile: {1}",
				m_unknownCompoundInstanceItem.MolecularWeight,
				WorkflowInputFile.FileName);
		}

		/// <summary>
		/// Prepares the point tooltip.
		/// </summary>
		protected override string PreparePointToolTip(TracePoint tracePoint)
		{
			return string.Format(
				"MW: {0:F5} [Da]\nArea: {1:F0}\nRT: {2:F2} [min]\nIntensity: {3:F0} [count]\nFile: {4}",
				m_unknownCompoundInstanceItem.MolecularWeight,
				m_unknownCompoundInstanceItem.Area,
				tracePoint.Time,
				tracePoint.Intensity,
				WorkflowInputFile.FileName);
		}
    }
}
