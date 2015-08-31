using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using OpenMS.OpenMSFile;
using Thermo.Magellan.EntityDataFramework;
using Thermo.Magellan.MassSpec;
using Thermo.Metabolism.DataObjects;
using Thermo.Metabolism.DataObjects.Constants;
using Thermo.Metabolism.DataObjects.EntityDataObjects;
using Thermo.Metabolism.Processing.Common;

namespace OpenMS.AdapterNodes
{
	public struct Centroid
	{
		public double mass;
		public double rt;
	}

    #region Implement logic for Consolidation of UnknownFeatureInstanceItems
    public class UnknownFeatureConsolidationProvider : PeakConsolidationProvider<UnknownFeatureIonInstanceItem>
    {

	    private Dictionary<ulong, Centroid> m_dict;


		public UnknownFeatureConsolidationProvider(Dictionary<ulong, Centroid> consensusDict, int processingNodeNumber, string processingNodeName, IEntityDataService entityDataService)
            : base(processingNodeNumber, processingNodeName, entityDataService)
        {
			m_dict = consensusDict;
        }

        public override IEnumerable<ConsolidatedComponentPeak> RetrieveComponentPeaks()
        {
            // init container
            var componentPeaks = new List<ConsolidatedComponentPeak>();

			

	        // get entity reader
            var entityReader = EntityDataService.CreateEntityItemReader();

            // read all the peaks
            foreach (var ion in entityReader.ReadAll<UnknownFeatureIonInstanceItem>())
            {
                // 1 ion has at least 1 peak, monoisotopic was used for Ion information
                // (UnknownCompound..:process each ion individually)
                // make ConsolidatedComponentPeak
	            var fid = Convert.ToUInt64(ion.FeatureID);

                componentPeaks.Add(new ConsolidatedComponentPeak
                {
                    Mass = m_dict[fid].mass,
                    RetentionTime = m_dict[fid].rt,
                    Area = ion.Area,
                    IonDescription = "unknown",
                    FileID = ion.FileID,
                    IdentifyingNodeNumber = ProcessingNodeNumber,
                    RelatedItemIDs = ion.GetIDs()
                });
            }

            return componentPeaks;
        }


		/// <summary>
		/// Compares individual items with their respective items from control file.
		/// </summary>
		/// <param name="items">Items to compare.</param>
		/// <param name="sampleToControlMaxFold">Maximum sample to control ratio.</param>
		/// <param name="controlToSampleMaxFold">Maximum control to sample ratio.</param>
		/// <param name="massTolerance">Mass tolerance used for peak consolidation.</param>
		/// <param name="inputFiles">Input files information sorted in the final view order.</param>
	    protected override Dictionary<UnknownFeatureIonInstanceItem, Tuple<InControlStatus, double?>> CompareComponentItems(
                            List<UnknownFeatureIonInstanceItem> items,
                            double sampleToControlMaxFold,
                            double controlToSampleMaxFold,
                            MassTolerance massTolerance,
                            List<InputFileInfo> inputFiles)
        {

			// init results dict
			var results = new Dictionary<UnknownFeatureIonInstanceItem, Tuple<InControlStatus, double?>>();

			// make input files map
			var inputFilesMap = inputFiles.ToDictionary(key => key.FileID);

			// get all control items
			var controlItems = items.Where(w => inputFilesMap[w.FileID].IsReference).ToList();

			// compare all items
			foreach (var item in items)
			{
				// init status and ratio
				InControlStatus status;
				double? ratio = null;

				// get input file info
				var inputFile = inputFilesMap[item.FileID];

				// control item itself but NOT found
				if (inputFile.IsReference && item.Area.Equals(0))
				{
					status = InControlStatus.NotInControlSelf;
					results.Add(item, Tuple.Create(status, ratio));
					continue;
				}

				// control item itself
				if (inputFile.IsReference)
				{
					status = InControlStatus.InControlSelf;
					results.Add(item, Tuple.Create(status, ratio));
					continue;
				}

				// no control file assigned
				if (inputFile.HasReference == false)
				{
					status = InControlStatus.NoControlAssigned;
					results.Add(item, Tuple.Create(status, ratio));
					continue;
				}

				// get corresponding control item
				double maxDelta = massTolerance.GetToleranceInU(item.MolecularWeight);
				var controlItem = controlItems.FirstOrDefault(w => w.FileID == inputFile.ReferenceFileID && Math.Abs(w.MolecularWeight - item.MolecularWeight) <= maxDelta);

				// not found in control
				if (controlItem == null || controlItem.Area.Equals(0))
				{
					status = InControlStatus.NotInControl;
					results.Add(item, Tuple.Create(status, ratio));
					continue;
				}

				// calc sample to control area ratio
				ratio = item.Area / controlItem.Area;

				// get in-control status
				bool inControl = (sampleToControlMaxFold.Equals(0) || ratio <= sampleToControlMaxFold) &&
								 (controlToSampleMaxFold.Equals(0) || 1.0 / ratio <= controlToSampleMaxFold);

				status = (inControl) ? InControlStatus.InControl : InControlStatus.Outside;

				// store result
				results.Add(item, Tuple.Create(status, ratio));
			}

			return results;
        }
    }
    #endregion
}