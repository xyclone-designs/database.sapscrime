using Newtonsoft.Json.Linq;

using SQLite;

using System.Collections.Generic;
using System.IO;
using System.Linq;

using XycloneDesigns.Apis.General.Tables;
using XycloneDesigns.Apis.SAPS.Tables;

namespace Database.SAPSCrime
{
	internal partial class Program
	{
		static IEnumerable<District> JsonDistricts()
		{
			foreach (string filepath in Directory.EnumerateFiles(DirectoryInputJsons).Where(_ => _.EndsWith("districts.json")))
			{
				using FileStream filestream = File.OpenRead(filepath);
				using StreamReader streamreader = new(filestream);

				string json = streamreader.ReadToEnd();
				JObject jobject = JObject.Parse(json);

				if (jobject.GetValue("results") is not JArray jarray)
					continue;

				foreach (JToken jtoken in jarray)
					yield return new District
					{
						Code = jtoken["code"]?.ToObject<string?>(),
						Name = jtoken["text"]?.ToObject<string?>(),
					};
			}
		}
		static IEnumerable<Municipality> JsonMunicipalities()
		{
			foreach (string filepath in Directory.EnumerateFiles(DirectoryInputJsons).Where(_ => _.EndsWith("municiapilities.json")))
			{
				using FileStream filestream = File.OpenRead(filepath);
				using StreamReader streamreader = new(filestream);

				string json = streamreader.ReadToEnd();
				JObject jobject = JObject.Parse(json);

				if (jobject.GetValue("results") is not JArray jarray)
					continue;

				foreach (JToken jtoken in jarray)
					yield return new Municipality
					{
						GeoCode = jtoken["code"]?.ToObject<string?>(),
						Name = jtoken["text"]?.ToObject<string?>(),
					};
			}
		}
	}
}