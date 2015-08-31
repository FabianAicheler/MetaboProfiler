using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Thermo.Magellan.Utilities;
using Thermo.Metabolism.DataObjects;

namespace OpenMS.AdapterNodes
{
    static class TracePointSumClass
    {
        /// <summary>
        /// Sums the intensity of trace points, if the <see cref="TracePoint.SpectrumID"/> is the same.
        /// This method can be used to sum traces having the same raster.
        /// </summary>
        /// <param name="items">An enumeration of trace points.</param>
        public static IEnumerable<TracePoint> SumPoints(this IEnumerable<TracePoint> items)
        {

            // ReSharper disable PossibleMultipleEnumeration
            ArgumentHelper.AssertNotNull(items, "items");
            return items
                  .GroupBy(g => g.SpectrumID)
                  .Select(s => new TracePoint(s.Key, s.First().Time, s.Sum(sum => sum.Intensity), s.Max(m => m.MaxNoise)));
            // ReSharper restore PossibleMultipleEnumeration
        }
    }

}
