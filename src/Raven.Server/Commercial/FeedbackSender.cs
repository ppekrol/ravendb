﻿using System;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Server.Documents.Studio;

namespace Raven.Server.Commercial
{
    internal sealed class FeedbackSender
    {
        public async Task SendFeedback(FeedbackForm feedback)
        {
            var response = await ApiHttpClient.Instance.PostAsync("api/v1/feedback",
                    new StringContent(JsonConvert.SerializeObject(feedback), Encoding.UTF8, "application/json"))
                .ConfigureAwait(false);

            var responseString = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
            if (response.IsSuccessStatusCode == false)
            {
                throw new InvalidOperationException(responseString);
            }
        }

    }
}