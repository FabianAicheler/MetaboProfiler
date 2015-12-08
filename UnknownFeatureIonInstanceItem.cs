//-----------------------------------------------------------------------------
// Copyright (c) 2013, Thermo Fisher Scientific
// All rights reserved
//-----------------------------------------------------------------------------

using System;
using Thermo.Magellan.BL.Data;
using Thermo.Magellan.EntityDataFramework;
using Thermo.Magellan.MassSpec;
using Thermo.Metabolism.DataObjects.Constants;
using Thermo.Metabolism.DataObjects.EntityDataObjects;

namespace OpenMS.AdapterNodes
{
	/// <summary>
	/// Represents an unknown feature ion instance.
	/// </summary>

    [EntityExport("04700C26-3238-427A-9F91-6BC00D743D81",
          EntityName = "FeatureIonInstanceItem",
          TableName = "FeatureIonInstanceItems",
          DisplayName = "FeatureIon",
          Description = "OpenMS metabolite feature",
          Visibility = GridVisibility.Visible,
          VisiblePosition = 410)]
    
	[PredefinedEntityProperty(PredefinedEntityPropertyNames.Checkable)]
    
	public class UnknownFeatureIonInstanceItem : DynamicEntity, ICompoundIonInstanceItem, ITooltip
    {
		/// <summary>
		/// Gets or sets the workflow ID.
		/// </summary>		
		[EntityProperty(
			DisplayName = "WorkflowID",
			DataPurpose = CDEntityDataPurpose.WorkflowID)]
		[EntityId(1)]
		public int WorkflowID { get; set; }

		/// <summary>
		/// Gets or sets the assigned abstract ion ID.
		/// </summary>		
		[EntityProperty(
			DisplayName = "AbstractIonID",
			DataPurpose = CDEntityDataPurpose.ID)]
		[EntityId(2)]
		public int AbstractIonID { get; set; }

		/// <summary>
		/// Gets or sets the item ID.
		/// </summary>		
		[EntityProperty(
			DisplayName = "ID",
			DataPurpose = CDEntityDataPurpose.ID)]
		[EntityId(3)]
		public int ID { get; set; }

		/// <summary>
		/// Gets or sets the ion description.
		/// </summary>
		[EntityProperty(
			DisplayName = "Ion",
			Description = "Ion description",
			DataPurpose = CDEntityDataPurpose.IonDescription)]
		[GridDisplayOptions(
			VisiblePosition = 100)]
		public string IonDescription { get; set; }

		/// <summary>
		/// Gets or sets the ion charge.
		/// </summary>		
		[EntityProperty(
			DisplayName = "Charge",
			Description = "Ion charge",
			FormatString = "0",
			DataPurpose = CDEntityDataPurpose.Charge)]
		[GridDisplayOptions(
			VisiblePosition = 200,
			TextHAlign = GridCellHAlign.Center)]
		public int Charge { get; set; }

		/// <summary>
		/// Gets or sets the neutral molecular weight.
		/// </summary>		
		[EntityProperty(
			DisplayName = "Molecular Weight",
			Description = "Neutral mass in Da calculated from the left most isotope",
			FormatString = "0.00000",
			DataPurpose = CDEntityDataPurpose.MolecularWeight)]
		[GridDisplayOptions(
			VisiblePosition = 300,
			TextHAlign = GridCellHAlign.Right)]
		[PlottingOptions(
			PlotType = PlotType.Numeric)]
		public double MolecularWeight { get; set; }

		/// <summary>
		/// Gets or sets the ion mass.
		/// </summary>		
		[EntityProperty(
			DisplayName = "m/z",
			Description = "Measured ion mass",
			FormatString = "0.00000",
			DataPurpose = CDEntityDataPurpose.MassOverCharge)]
		[GridDisplayOptions(
			VisiblePosition = 400,
			TextHAlign = GridCellHAlign.Right)]
		[PlottingOptions(
			PlotType = PlotType.Numeric)]
		public double Mass { get; set; }

		/// <summary>
		/// Gets or sets the retention time in minutes.
		/// </summary> 				
		[EntityProperty(
			DisplayName = "RT [min]",
			Description = "Retention time in minutes",
			FormatString = "0.000",
			DataPurpose = CDEntityDataPurpose.RetentionTime)]
		[GridDisplayOptions(
			VisiblePosition = 500,
			TextHAlign = GridCellHAlign.Right)]
		[PlottingOptions(
			PlotType = PlotType.Numeric)]
		public double RetentionTime { get; set; }

        /// <summary>
        /// Gets or sets the peak FWHM in minutes.
        /// </summary> 				
        [EntityProperty(
            DisplayName = "FWHM [min]",
            Description = "Peak width in its half-height in minutes",
            FormatString = "0.000",
            DataPurpose = CDEntityDataPurpose.FWHM)]
        [GridDisplayOptions(
            DataVisibility = GridVisibility.Hidden,
            VisiblePosition = 600,
            TextHAlign = GridCellHAlign.Right)]
        [PlottingOptions(
            PlotType = PlotType.Numeric)]
        public double? FWHM { get; set; }

		/// <summary>
		///	Gets or sets the number of theoretical isotopes matched in measured spectrum.
		/// </summary>				
		[EntityProperty(
			DisplayName = "# MI",
			Description = "Number of matched isotopes",
			FormatString = "0",
			DataPurpose = CDEntityDataPurpose.NumberOfMatchedIsotopes)]
		[GridDisplayOptions(
			VisiblePosition = 600,
			TextHAlign = GridCellHAlign.Right)]
		[PlottingOptions(
			PlotType = PlotType.Numeric)]
		public int NumberOfMatchedIsotopes { get; set; }

		/// <summary>
		/// Gets or sets the peak intensity.
		/// </summary>		
		[EntityProperty(
			DisplayName = "Intensity",
			Description = "Max apex intensity of all related peaks per input file",
			FormatString = "0",
			DataPurpose = CDEntityDataPurpose.IntensityMax)]
		[GridDisplayOptions(
			VisiblePosition = 700,
			ColumnWidth = 100,
			TextHAlign = GridCellHAlign.Right,
			DataVisibility = GridVisibility.Hidden)]
		[PlottingOptions(
			PlotType = PlotType.Numeric)]
		public double Intensity { get; set; }

		/// <summary>
		/// Gets or sets the peak area.
		/// </summary>		
		[EntityProperty(
			DisplayName = "Area",
			Description = "Summed area of all related peaks per input file",
			FormatString = "0",
			DataPurpose = CDEntityDataPurpose .AreaSum)]
		[GridDisplayOptions(
			VisiblePosition = 800,
			ColumnWidth = 100,
			TextHAlign = GridCellHAlign.Right)]
		[PlottingOptions(
			PlotType = PlotType.Numeric)]
		public double Area { get; set; }

		/// <summary>
		/// Gets or sets the source input file id.
		/// </summary>		
		[EntityProperty(
			DisplayName = "File ID",
			Description = "Input file ID",
			DataPurpose = CDEntityDataPurpose.SpectrumFileID)]
		[GridDisplayOptions(
			VisiblePosition = 1100,
			GridCellControlGuid = "ECF3E1A3-0A57-458E-8C9C-83B5B1476242",
			TextHAlign = GridCellHAlign.Center)]
		[PlottingOptions(
			PlotType = PlotType.Numeric)]
		public int FileID { get; set; }

		/// <summary>
		/// Gets or sets the feature ID.
		/// </summary>		
		[EntityProperty(
			DisplayName = "FeatureID",
			DataPurpose = CDEntityDataPurpose.ID)]
		[GridDisplayOptions(
			VisiblePosition = 1200,
			TextHAlign = GridCellHAlign.Center)]
		public string FeatureID { get; set; }

		/// <summary>
		/// Gets or sets the node number which has identified the original item.
		/// </summary>		
		[EntityProperty(
			DisplayName = "Processing Node No",
			Description = "Processing node number as in workflow",
			DataPurpose = CDEntityDataPurpose.ProcessingNodeNumber)]
		[GridDisplayOptions(
			TextHAlign = GridCellHAlign.Center,
			DataVisibility = GridVisibility.Hidden)]
		[PlottingOptions(
			PlotType = PlotType.Venn)]
		public int IdentifyingNodeNumber { get; set; }

		/// <summary>
		/// Gets or sets the isotope pattern.
		/// This property is currently only to fulfill the ICompoundIonInstanceItem
		/// interface but its value is always null and it is not persisted.
		/// </summary>
		public IsotopePattern IsotopePattern { get; set; }

		/// <summary>
		/// Returns a string that represents this instance.
		/// </summary>
		public override string ToString()
		{
			return string.Format("ID:{0} {1}, MZ:{2} RT:{3:F2}", ID, IonDescription, Mass, RetentionTime);
		}

		/// <summary>
		/// Gets the tooltip string.
		/// </summary>
		public string GetTooltip()
		{
			return String.Format(
				"MZ: {0:F5}\nRT: {1:F3} min\nArea: {2:F0}\n# Isotopes: {3}",
				Mass,
				RetentionTime,
				Area,
				NumberOfMatchedIsotopes);
		}

    }
}
