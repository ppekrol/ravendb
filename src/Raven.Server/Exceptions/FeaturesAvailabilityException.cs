﻿using System;
using System.Diagnostics.CodeAnalysis;
using Raven.Server.Config;
using Raven.Server.Utils;
using Raven.Server.Utils.Features;

namespace Raven.Server.Exceptions
{
    internal sealed class FeaturesAvailabilityException : Exception
    {
     
        public FeaturesAvailabilityException()
        {
        }

        public FeaturesAvailabilityException(string message) : base(message)
        {
        }

        public FeaturesAvailabilityException(string message, Exception inner) : base(message, inner)
        {
        }

        [DoesNotReturn]
        public static void Throw(Feature feature)
        {
            throw new FeaturesAvailabilityException(
                $"Can not use '{feature.GetDescription()}', as this is an experimental feature and the server does not support experimental features. " +
                $"Please enable experimental features by changing '{RavenConfiguration.GetKey(x => x.Core.FeaturesAvailability)}' configuration value to '{nameof(FeaturesAvailability.Experimental)}'.");
        }

        [DoesNotReturn]
        public static void Throw(string message)
        {
            throw new FeaturesAvailabilityException(message);
        }
    }
}
