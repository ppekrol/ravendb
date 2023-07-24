using System;

namespace Sparrow.Server.Exceptions;

public class CGroupException : Exception
{
    public CGroupException(string message)
        : base(message)
    {

    }
}
