using System;
using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI;

/// <summary>
/// Configuration for Azure OpenAI deployments.
/// </summary>
public sealed class AzureOpenAiSettings : OpenAiBaseSettings, IOpenAiChatSettings
{
    public AzureOpenAiSettings(string apiKey, string endpoint, string model, string deploymentName, int? dimensions = null, double? temperature = null) : base(apiKey, endpoint, model, dimensions, temperature)
    {
        DeploymentName = deploymentName ?? throw new ArgumentNullException(nameof(deploymentName));
    }

    public AzureOpenAiSettings()
    {
        // deserialization
    }

    /// <summary>Azure OpenAI deployment name.
    /// <see href="https://learn.microsoft.com/azure/cognitive-services/openai/how-to/create-resource">Learn more</see>
    /// </summary>
    public string DeploymentName { get; set; }

    /// <summary>
    /// The reasoning effort for models that support it. Sent as <c>reasoning.effort</c> on the Responses API;
    /// not sent on Chat Completions. When not set, the model's default is used.
    /// </summary>
    public string ReasoningEffort { get; set; }

    /// <summary>
    /// The API that chat requests are sent to. When not set, <see cref="OpenAiApiMode.Responses"/> is used, at <c>openai/v1/responses</c>.
    /// </summary>
    public OpenAiApiMode? ApiMode { get; set; }

    internal OpenAiApiMode GetApiMode() => ApiMode ?? OpenAiApiMode.Responses;

    public override void ValidateFields(List<string> errors)
    {
        base.ValidateFields(errors);

        if (string.IsNullOrWhiteSpace(DeploymentName))
            errors.Add($"Value for `{nameof(DeploymentName)}` field cannot be empty.");

        if (string.IsNullOrWhiteSpace(Endpoint))
            errors.Add($"Value of `{nameof(Endpoint)}` field cannot be empty.");
    }

    public override AiSettingsCompareDifferences Compare(AbstractAiSettings other)
    {
        if (other is not AzureOpenAiSettings azureSettings)
            return AiSettingsCompareDifferences.All;

        var differences = base.Compare(other);

        if (DeploymentName != azureSettings.DeploymentName)
            differences |= AiSettingsCompareDifferences.DeploymentConfiguration;

        if (ReasoningEffort != azureSettings.ReasoningEffort || ApiMode != azureSettings.ApiMode)
            differences |= AiSettingsCompareDifferences.EndpointConfiguration;

        return differences;
    }

    public override DynamicJsonValue ToJson()
    {
        var json = base.ToJson();
        json[nameof(DeploymentName)] = DeploymentName;

        if (string.IsNullOrWhiteSpace(ReasoningEffort) == false)
            json[nameof(ReasoningEffort)] = ReasoningEffort;

        if (ApiMode.HasValue)
            json[nameof(ApiMode)] = ApiMode.Value;

        return json;
    }
}
