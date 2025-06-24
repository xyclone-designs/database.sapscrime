using Database.SAPSCrime.Inputs.CSVs;

using ICSharpCode.SharpZipLib.GZip;

using Newtonsoft.Json.Linq;

using SQLite;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using XycloneDesigns.Apis.General.Tables;
using XycloneDesigns.Apis.SAPS.Tables;

namespace Database.SAPSCrime
{
	internal partial class Program
	{
		//static readonly string DirectoryCurrent = Directory.GetCurrentDirectory();
		static readonly string DirectoryCurrent = Directory.GetParent(Directory.GetCurrentDirectory())?.Parent?.Parent?.FullName!;

		static readonly string DirectoryTemp = Path.Combine(DirectoryCurrent, ".temp");
		static readonly string DirectoryInput = Path.Combine(DirectoryCurrent, ".inputs");
		static readonly string DirectoryInputDatabases = Path.Combine(DirectoryInput, "databases");
		static readonly string DirectoryInputData = Path.Combine(DirectoryInput, "data");
		static readonly string DirectoryInputData_EasternCape = Path.Combine(DirectoryInputData, "Eastern Cape");
		static readonly string DirectoryInputData_FreeState = Path.Combine(DirectoryInputData, "Free State");
		static readonly string DirectoryInputData_KwaZuluNatal = Path.Combine(DirectoryInputData, "KwaZulu-Natal");
		static readonly string DirectoryInputData_Gauteng = Path.Combine(DirectoryInputData, "Gauteng");
		static readonly string DirectoryInputData_Limpopo = Path.Combine(DirectoryInputData, "Limpopo");
		static readonly string DirectoryInputData_Mpumalanga = Path.Combine(DirectoryInputData, "Mpumalanga");
		static readonly string DirectoryInputData_NorthWest = Path.Combine(DirectoryInputData, "North West");
		static readonly string DirectoryInputData_NorthernCape = Path.Combine(DirectoryInputData, "Northern Cape");
		static readonly string DirectoryInputData_WesternCape = Path.Combine(DirectoryInputData, "Western Cape");

		static readonly string DirectoryOutputs = Path.Combine(DirectoryCurrent, ".outputs");

		static void Main(string[] args) 
		{
			_CleaningPre();

			string sqlconnectionpath_municipalities = Path.Combine(DirectoryInputDatabases, "municipalities.db");
			string sqlconnectionpath_provinces = Path.Combine(DirectoryInputDatabases, "provinces.db");

			string sqlconnectionpath = Path.Combine(DirectoryOutputs, "sapscrime.db");

			SQLiteConnection sqliteconnection = _SQLiteConnection(sqlconnectionpath);
			SQLiteConnection sqliteconnection_municipalities = new (sqlconnectionpath_municipalities);
			SQLiteConnection sqliteconnection_provinces = new (sqlconnectionpath_provinces);
			JArray apifiles = [];
			StreamWriters streamwriters = [];
			string[] csvfilepaths =
			[
				.. Directory.EnumerateDirectories(DirectoryInputData_EasternCape).SelectMany(_ => Directory.EnumerateFiles(_)),
				.. Directory.EnumerateDirectories(DirectoryInputData_FreeState).SelectMany(_ => Directory.EnumerateFiles(_)),
				.. Directory.EnumerateDirectories(DirectoryInputData_KwaZuluNatal).SelectMany(_ => Directory.EnumerateFiles(_)),
				.. Directory.EnumerateDirectories(DirectoryInputData_Gauteng).SelectMany(_ => Directory.EnumerateFiles(_)),
				.. Directory.EnumerateDirectories(DirectoryInputData_Limpopo).SelectMany(_ => Directory.EnumerateFiles(_)),
				.. Directory.EnumerateDirectories(DirectoryInputData_Mpumalanga).SelectMany(_ => Directory.EnumerateFiles(_)),
				.. Directory.EnumerateDirectories(DirectoryInputData_NorthWest).SelectMany(_ => Directory.EnumerateFiles(_)),
				.. Directory.EnumerateDirectories(DirectoryInputData_NorthernCape).SelectMany(_ => Directory.EnumerateFiles(_)),
				.. Directory.EnumerateDirectories(DirectoryInputData_WesternCape).SelectMany(_ => Directory.EnumerateFiles(_)),
			];

			foreach (string csvfilepath in csvfilepaths)
			{
				string[] names = csvfilepath.Split('\\', '.')[^4..^1];
				string policestationname = names[2], municipalitygeocode = names[1], provincename = names[0];

				using FileStream csvfilestream = File.OpenRead(csvfilepath);
				using StreamReader csvstreamreader = new (csvfilestream);

				Municipality? municipality = sqliteconnection_municipalities
					.Table<Municipality>()
					.AsEnumerable()
					.FirstOrDefault(_ => string.Equals(_.GeoCode, municipalitygeocode, StringComparison.OrdinalIgnoreCase));
				Province? province = sqliteconnection_provinces
					.Table<Province>()
					.AsEnumerable()
					.FirstOrDefault(_ => string.Equals(_.Name, provincename, StringComparison.OrdinalIgnoreCase));

				PoliceStation policestation = sqliteconnection.InsertAndReturn(new PoliceStation
				{
					Name = policestationname,
					PkMunicipality = municipality?.Pk,
					PkProvince = province?.Pk,
				});

				List<CSVRow> csvrows = [];

				if (csvstreamreader.ReadLine() is not null)
					while (csvstreamreader.ReadLine() is string line)
						csvrows.Add(new CSVRow(line));

				sqliteconnection.InsertAll(
					objects: csvrows
					.Select(_ => _.Category)
					.Where(_ =>
					{
						return sqliteconnection
							.Table<Category>()
							.Any(__ => _ == __.Name) is false;

					}).Select(_ => new Category { Name = _ }));
				sqliteconnection.Commit();

				IEnumerable<Record> records = sqliteconnection.InsertAllAndReturn(
					objs: csvrows.GroupBy(_ => _.Year).Select(_ => new Record
					{
						Year = _.Key,
						PkMunicipality = municipality?.Pk,
						PkPoliceStation = policestation.Pk,
						PkProvince = province?.Pk,
						List_PkCategoryValue = string.Join(',', _.Select(_ =>
						{
							Category category = sqliteconnection
								.Table<Category>()
								.First(__ => __.Name == _.Category);

							return string.Format("{0}:{1}", category.Pk, _.Value);
						})),
					}));

				policestation.List_PkRecord = string.Join(',', records.Select(_ => _.Pk));

				sqliteconnection.Update(policestation);
				sqliteconnection.Commit();
			}

			sqliteconnection.Close();

			FileInfo fileinfo = new(sqlconnectionpath);

			string fileinfozipfile = fileinfo.ZipFile().Split('\\').Last();
			string fileinfogzipfile = fileinfo.GZipFile().Split('\\').Last();

			apifiles.Add(fileinfozipfile, "SAPS crime stats");
			apifiles.Add(fileinfogzipfile, "SAPS crime stats");

			fileinfo.Delete();

			string apifilesjson = apifiles.ToString();
			string apifilespath = Path.Combine(DirectoryOutputs, "index.json");

			using FileStream apifilesfilestream = File.OpenWrite(apifilespath);
			using StreamWriter apifilesstreamwriter = new(apifilesfilestream);

			apifilesstreamwriter.Write(apifilesjson);
			apifilesstreamwriter.Close();
			apifilesfilestream.Close();

			_CleaningPost();
		}

		static void _CleaningPre()
		{
			Console.WriteLine("Pre Cleaning...");

			if (Directory.Exists(DirectoryOutputs)) Directory.Delete(DirectoryOutputs, true);
			if (Directory.Exists(DirectoryTemp)) Directory.Delete(DirectoryTemp, true);

			Console.WriteLine("Creating Directories...");

			Directory.CreateDirectory(DirectoryOutputs);
			Directory.CreateDirectory(DirectoryTemp);
		}
		static void _CleaningPost()
		{
			Console.WriteLine("Cleaning Up...");

			Directory.Delete(DirectoryTemp, true);
		}
		static SQLiteConnection _SQLiteConnection(string path)
		{
			SQLiteConnection sqliteconnection = new(path);

			sqliteconnection.CreateTable<Category>();
			sqliteconnection.CreateTable<PoliceStation>();
			sqliteconnection.CreateTable<Record>();

			return sqliteconnection;
		}
	}

	public static class Extensions
	{
		public static void Add(this JArray jarray, string filename, string name)
		{
			string description = filename.Split('.').Last() switch
			{
				"zip" => string.Format("zipped {0} ", name),
				"gz" => string.Format("g-zipped {0} ", name),
				
				_ => string.Empty
			};

			jarray.Add(new JObject
			{
				{ "DateCreated", DateTime.Now.ToString("dd-MM-yyyy") },
				{ "DateEdited", DateTime.Now.ToString("dd-MM-yyyy") },
				{ "Name", filename },
				{ "Url", string.Format("https://raw.githubusercontent.com/xyclone-designs/database.sapscrime/refs/heads/main/.output/{0}", filename) },
				{ "Description", string.Format("individual {0}database", description) }
			});
		}
	}
}