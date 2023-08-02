﻿using System;
using System.Globalization;
using System.Text.RegularExpressions;
using Raven.Client;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Documents.Queries.Spatial;
using Spatial4n.Context.Nts;
using Spatial4n.Shapes;

namespace Raven.Server.Documents.Indexes.Static.Spatial
{
    /// <summary>
    /// Reads and writes shape strings
    /// </summary>
    internal sealed class ShapeStringReadWriter
    {
        private static readonly WktSanitizer WktSanitizer = new WktSanitizer();

        private readonly SpatialOptions _options;
        private readonly NtsSpatialContext _context;
        private readonly ShapeStringConverter _shapeStringConverter;

        public ShapeStringReadWriter(SpatialOptions options, NtsSpatialContext context)
        {
            _options = options;
            _context = context;
            _shapeStringConverter = new ShapeStringConverter(options);
        }

        public IShape ReadShape(string shape, SpatialUnits? unitOverride = null)
        {
            shape = _shapeStringConverter.ConvertToWKT(shape);
            shape = WktSanitizer.Sanitize(shape);

            // Circle translation should be done last, before passing to NtsShapeReadWriter
            if (_options.Type == SpatialFieldType.Geography)
                shape = TranslateCircleRadius(shape, unitOverride ?? _options.Units);

#pragma warning disable 612
            return _context.ReadShape(shape);
#pragma warning restore 612
        }

        public string WriteShape(IShape shape)
        {
#pragma warning disable 612
            return _context.ToString(shape);
#pragma warning restore 612
        }

        public static double TranslateCircleRadius(double radius, SpatialUnits units)
        {
            if (units == SpatialUnits.Miles)
                radius *= Constants.Documents.Indexing.Spatial.MilesToKm;

            return (radius / Constants.Documents.Indexing.Spatial.EarthMeanRadiusKm) * RadiansToDegrees;
        }

        public static double TranslateDegreesToRadius(double degrees, SpatialUnits units, SpatialOptions options)
        {
            if (options.Type == SpatialFieldType.Cartesian)
                return degrees;

            var radius = (degrees * Constants.Documents.Indexing.Spatial.EarthMeanRadiusKm) / RadiansToDegrees;
            if (units == SpatialUnits.Miles)
                radius /= Constants.Documents.Indexing.Spatial.MilesToKm;

            return radius;
        }

        private static string TranslateCircleRadius(string shapeWkt, SpatialUnits units)
        {
            var match = CircleShape.Match(shapeWkt);
            if (match.Success == false)
                return shapeWkt;

            var radCapture = match.Groups[3];
            var radius = double.Parse(radCapture.Value, CultureInfo.InvariantCulture);

            radius = TranslateCircleRadius(radius, units);

            return shapeWkt.Substring(0, radCapture.Index) + radius.ToString("F6", CultureInfo.InvariantCulture) +
                   shapeWkt.Substring(radCapture.Index + radCapture.Length);
        }

        private const double DegreesToRadians = Math.PI / 180;
        private const double RadiansToDegrees = 1 / DegreesToRadians;

        private static readonly Regex CircleShape =
            new Regex(@"Circle \s* \( \s* ([+-]?(?:\d+\.?\d*|\d*\.?\d+)) \s+ ([+-]?(?:\d+\.?\d*|\d*\.?\d+)) \s+ d=([+-]?(?:\d+\.?\d*|\d*\.?\d+)) \s* \)",
                      RegexOptions.IgnorePatternWhitespace | RegexOptions.Compiled | RegexOptions.IgnoreCase);
    }
}
