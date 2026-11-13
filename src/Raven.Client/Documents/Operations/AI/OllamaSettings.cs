using System;
using System.Collections.Generic;
using Sparrow.Extensions;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI;

/// <summary>
/// The configuration for the Ollama API client.
/// </summary>
public sealed class OllamaSettings : AbstractAiSettings, IOpenAiChatSettings
{
    /// <summary>
    /// Initializes a new instance of <see cref="OllamaSettings"/> with the specified URI and model.
    /// </summary>
    /// <param name="uri">The URI of the Ollama API.</param>
    /// <param name="model">The model to be used.</param>
    public OllamaSettings(string uri, string model)
    {
        Uri = uri;
        Model = model;
    }

    public OllamaSettings()
    {
        // deserialization
    }

    /// <summary>
    /// The URI of the Ollama API.
    /// </summary>
    public string Uri { get; set; }

    /// <summary>
    /// The model that should be used.
    /// </summary>
    public string Model { get; set; }

    /// <summary>
    /// Controls whether thinking models engage their reasoning process before responding.
    /// When true, thinking models will perform their internal reasoning process (uses more tokens, slower, better quality for complex tasks).
    /// When false, thinking models skip the reasoning process and respond directly (fewer tokens, faster, may reduce quality for complex reasoning).
    /// When null, the parameter is not sent (backwards compatible).
    /// Disable thinking for speed/efficiency in simple tasks, enable for complex tasks requiring reasoning.
    /// On Ollama's <c>/v1/chat/completions</c>, <c>think: false</c> may not stop reasoning; setting
    /// <see cref="ReasoningEffort"/> to <c>none</c> does (observed on Ollama 0.40.16).
    /// </summary>
    public bool? Think { get; set; } = null;

    /// <summary>
    /// Controls randomness of the model output. Range typically [0.0, 2.0].
    /// Higher values (e.g., 1.0+) make output more creative and diverse; lower values (e.g., 0.2) make it more deterministic.
    /// When null, the parameter is not sent.
    /// </summary>
    public double? Temperature { get; set; } = null;

    /// <summary>
    /// Optional. Local Ollama deployments usually have no API key. When set, it is sent as a Bearer token
    /// on chat requests (for an Ollama behind an authenticating proxy); embeddings requests do not send it.
    /// </summary>
    public string ApiKey { get; set; }

    /// <summary>
    /// The reasoning effort for models that support it. When not set, the model's default is used.
    /// Sent as <c>reasoning_effort</c> on Chat Completions and as <c>reasoning.effort</c> on the Responses API.
    /// </summary>
    public string ReasoningEffort { get; set; }

    /// <summary>
    /// The API that chat requests are sent to. When not set, Chat Completions is used, as before this setting existed.
    /// </summary>
    public OpenAiApiMode? ApiMode { get; set; }

    internal OpenAiApiMode GetApiMode() => ApiMode ?? OpenAiApiMode.ChatCompletions;

    public override void ValidateFields(List<string> errors)
    {
        base.ValidateFields(errors);

        if (string.IsNullOrWhiteSpace(Uri))
            errors.Add($"Value of `{nameof(Uri)}` field cannot be empty.");

        if (string.IsNullOrWhiteSpace(Model))
            errors.Add($"Value of `{nameof(Model)}` field cannot be empty.");
        
        if (Temperature is < 0d)
            errors.Add($"Value of `{nameof(Temperature)}` field must be non-negative.");
    }

    public override AiSettingsCompareDifferences Compare(AbstractAiSettings other)
    {
        if (other is not OllamaSettings ollamaSettings)
            return AiSettingsCompareDifferences.All;

        var differences = AiSettingsCompareDifferences.None;

        if (Model != ollamaSettings.Model)
            differences |= AiSettingsCompareDifferences.ModelArchitecture;

        if (Uri != ollamaSettings.Uri)
            differences |= AiSettingsCompareDifferences.EndpointConfiguration;

        if (Think != ollamaSettings.Think)
            differences |= AiSettingsCompareDifferences.EndpointConfiguration;
        
        if (Temperature.HasValue != ollamaSettings.Temperature.HasValue ||
            (Temperature.HasValue && ollamaSettings.Temperature.HasValue && Temperature.Value.AlmostEquals(ollamaSettings.Temperature.Value) == false))
            differences |= AiSettingsCompareDifferences.EndpointConfiguration;

        if (ApiKey != ollamaSettings.ApiKey)
            differences |= AiSettingsCompareDifferences.AuthenticationSettings;

        if (ReasoningEffort != ollamaSettings.ReasoningEffort || ApiMode != ollamaSettings.ApiMode)
            differences |= AiSettingsCompareDifferences.EndpointConfiguration;

        return differences;
    }

    /// <summary>
    /// Serializes the settings to JSON structure.
    /// </summary>
    public override DynamicJsonValue ToJson()
    {
        var json = base.ToJson();
        json[nameof(Model)] = Model;
        json[nameof(Uri)] = Uri;
        json[nameof(Think)] = Think;
        json[nameof(Temperature)] = Temperature;

        // Written only when set, so a connection string without them is stored with the same keys as before.
        if (string.IsNullOrWhiteSpace(ApiKey) == false)
            json[nameof(ApiKey)] = ApiKey;

        if (string.IsNullOrWhiteSpace(ReasoningEffort) == false)
            json[nameof(ReasoningEffort)] = ReasoningEffort;

        if (ApiMode.HasValue)
            json[nameof(ApiMode)] = ApiMode.Value;

        return json;
    }

    /// <summary>
    /// The endpoint URI for the Ollama service.
    /// </summary>
    public string Endpoint => Uri;
    public Uri GetBaseEndpointUri() => new Uri(Endpoint);

    // Ollama has no prompt cache setting, so it always follows the default.
    bool? IAiSettings.EnablePromptCache => null;
}
