﻿using System;
using Spatial4n.Shapes;

namespace Raven.Server.Documents.Indexes.Spatial
{
    internal sealed class Coordinates
    {
        public readonly double Latitude;
        public readonly double Longitude;

        public Coordinates(IPoint point)
        {
            if (point == null)
                throw new ArgumentNullException(nameof(point));

            Latitude = point.Y;
            Longitude = point.X;
        }

        public Coordinates(double latitude, double longitude)
        {
            Latitude = latitude;
            Longitude = longitude;
        }
    }
}
