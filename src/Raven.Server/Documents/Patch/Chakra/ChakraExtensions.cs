using Microsoft.Scripting.JavaScript;

namespace Raven.Server.Documents.Patch.Chakra
{
    public static class ChakraExtensions
    {
        public static void AssertNoExceptions(this JavaScriptEngine engine)
        {
            if (engine.HasException)
                throw new ChakraException(engine.GetAndClearException().ToString());
        }
    }
}