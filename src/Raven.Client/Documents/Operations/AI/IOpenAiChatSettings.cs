namespace Raven.Client.Documents.Operations.AI;

// The settings the OpenAI-family chat providers read, for the connectors that can use either chat API
// (OpenAI, Azure OpenAI and Ollama). Each of them declares these properties itself; nothing else inherits them.
internal interface IOpenAiChatSettings : IAiSettings
{
    double? Temperature { get; }

    string ReasoningEffort { get; }
}
