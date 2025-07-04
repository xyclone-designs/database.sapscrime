using Database.SAPSCrime.Inputs.CSVs;

using ICSharpCode.SharpZipLib.GZip;

using Newtonsoft.Json.Linq;

using SQLite;

using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
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
		static readonly string DirectoryInputData_EasternCape = Path.Combine(DirectoryInputData, "eastern-cape");
		static readonly string DirectoryInputData_FreeState = Path.Combine(DirectoryInputData, "free-state");
		static readonly string DirectoryInputData_KwaZuluNatal = Path.Combine(DirectoryInputData, "kwazulu-natal");
		static readonly string DirectoryInputData_Gauteng = Path.Combine(DirectoryInputData, "gauteng");
		static readonly string DirectoryInputData_Limpopo = Path.Combine(DirectoryInputData, "limpopo");
		static readonly string DirectoryInputData_Mpumalanga = Path.Combine(DirectoryInputData, "mpumalanga");
		static readonly string DirectoryInputData_NorthWest = Path.Combine(DirectoryInputData, "north-west");
		static readonly string DirectoryInputData_NorthernCape = Path.Combine(DirectoryInputData, "northern-cape");
		static readonly string DirectoryInputData_WesternCape = Path.Combine(DirectoryInputData, "western-cape");
		static readonly string DirectoryInputJsons = Path.Combine(DirectoryInput, "jsons");

		static readonly string DirectoryOutputs = Path.Combine(DirectoryCurrent, ".outputs");

		static IEnumerable<string> AllDirectories(params string[] paths)
		{
			foreach (string path in paths)
			{
				foreach (string dir in Directory.EnumerateDirectories(path))
					foreach (string _dir in AllDirectories(dir))
						yield return _dir;

				yield return path;
			}
		}

		static void Main(string[] args) 
		{
			_CleaningPre();

			using FileStream filestream_districts = File.OpenRead(Path.Combine(DirectoryInputDatabases, "districts.db.zip"));
			using FileStream filestream_municipalities = File.OpenRead(Path.Combine(DirectoryInputDatabases, "municipalities.db.zip"));
			using FileStream filestream_provinces = File.OpenRead(Path.Combine(DirectoryInputDatabases, "provinces.db.zip"));

			new ZipArchive(filestream_districts).ExtractToDirectory(DirectoryOutputs);
			new ZipArchive(filestream_municipalities).ExtractToDirectory(DirectoryOutputs);
			new ZipArchive(filestream_provinces).ExtractToDirectory(DirectoryTemp);

			string sqlconnectionpath = Path.Combine(DirectoryOutputs, "sapscrime.db");
			string sqlconnectionpath_districts = Path.Combine(DirectoryOutputs, "districts.db");
			string sqlconnectionpath_municipalities = Path.Combine(DirectoryOutputs, "municipalities.db");
			string sqlconnectionpath_provinces = Path.Combine(DirectoryTemp, "provinces.db");

			JArray apifiles = [];
			SQLiteConnection sqliteconnection = _SQLiteConnection(sqlconnectionpath);
			SQLiteConnection sqliteconnection_districts = new(sqlconnectionpath_districts);
			SQLiteConnection sqliteconnection_municipalities = new(sqlconnectionpath_municipalities);
			SQLiteConnection sqliteconnection_provinces = new(sqlconnectionpath_provinces);

			List<District> districts = JsonDistricts()
				.Where(_ =>
				{
					return sqliteconnection_districts
						.TableEnumerable<District>()
						.Any(__ =>
						{
							return string.Equals(_.Code, __.Code, StringComparison.OrdinalIgnoreCase);

						}) is false;

				}).DistinctBy(_ => _.Code).ToList(); sqliteconnection_districts.InsertAll(districts, out int _).Commit();
			List<Municipality> municipalities = JsonMunicipalities()
				.Where(_ =>
				{
					return sqliteconnection_municipalities
						.TableEnumerable<Municipality>()
						.Any(__ =>
						{
							return string.Equals(_.GeoCode, __.GeoCode, StringComparison.OrdinalIgnoreCase);

						}) is false;

				}).DistinctBy(_ => _.GeoCode).ToList(); sqliteconnection_municipalities.InsertAll(municipalities, out int _).Commit();

			foreach (string csvfilepath in AllDirectories(
			[
				.. Directory.EnumerateDirectories(DirectoryInputData_EasternCape),
				.. Directory.EnumerateDirectories(DirectoryInputData_FreeState),
				.. Directory.EnumerateDirectories(DirectoryInputData_KwaZuluNatal),
				.. Directory.EnumerateDirectories(DirectoryInputData_Gauteng),
				.. Directory.EnumerateDirectories(DirectoryInputData_Limpopo),
				.. Directory.EnumerateDirectories(DirectoryInputData_Mpumalanga),
				.. Directory.EnumerateDirectories(DirectoryInputData_NorthWest),
				.. Directory.EnumerateDirectories(DirectoryInputData_NorthernCape),
				.. Directory.EnumerateDirectories(DirectoryInputData_WesternCape),

			]).SelectMany(_ => Directory.EnumerateFiles(_)))
			{
				string[] names = csvfilepath.Split('\\', '.')[^5..^1];
				string 
					policestationname = names[3], 
					municipalityname = names[2], 
					districtname = names[1], 
					provincename = names[0];

				string cleanmunicipalityname = CleanMuniciapility(municipalityname); 
				string cleandistrictname = CleanDistrict(districtname); 
				string cleanprovincename = CleanProvince(provincename);

				Console.WriteLine("Province: {0}, District: {1}, Municipality: {2}, Station: {3}", cleanprovincename, cleandistrictname, cleanmunicipalityname, policestationname);

				using FileStream csvfilestream = File.OpenRead(csvfilepath);
				using StreamReader csvstreamreader = new (csvfilestream);

				District district = sqliteconnection_districts
					.Table<District>()
					.AsEnumerable()
					.FirstOrDefault(_ => cleandistrictname.Equals(_.Name, StringComparison.OrdinalIgnoreCase)) ??
					sqliteconnection_districts.InsertAndReturn(new District
					{
						Name = cleandistrictname
					});
				Municipality municipality = sqliteconnection_municipalities
					.Table<Municipality>()
					.AsEnumerable()
					.FirstOrDefault(_ => string.Equals(_.Name, cleanmunicipalityname, StringComparison.OrdinalIgnoreCase)) ??
					sqliteconnection_municipalities.InsertAndReturn(new Municipality
					{
						Name = cleanmunicipalityname
					});
				Province province = sqliteconnection_provinces
					.Table<Province>()
					.AsEnumerable()
					.First(_ => string.Equals(_.Name, cleanprovincename, StringComparison.OrdinalIgnoreCase));

				PoliceStation policestation = sqliteconnection.InsertAndReturn(new PoliceStation
				{
					Name = policestationname,
					PkDistrict = district.Pk,
					PkMunicipality = municipality.Pk,
					PkProvince = province.Pk,
				});

				List<CSVRow> csvrows = [];

				if (csvstreamreader.ReadLine() is not null)
					while (csvstreamreader.ReadLine() is string line)
						csvrows.Add(new CSVRow(line));

				sqliteconnection.InsertAll(csvrows
					.Select(_ => _.Category)
					.Where(_ =>
					{
						return sqliteconnection
							.Table<Category>()
							.Any(__ => _ == __.Name) is false;

					}).Select(_ => new Category { Name = _ }), out int _).Commit();

				sqliteconnection.InsertAll(
					objects : csvrows.GroupBy(_ => _.Year).Select(_ => new Record
					{
						Year = _.Key,
						PkPoliceStation = policestation.Pk,
						List_PkCategoryValue = string.Join(',', _.Select(_ =>
						{
							Category category = sqliteconnection
								.Table<Category>()
								.First(__ => __.Name == _.Category);

							return string.Format("{0}:{1}", category.Pk, _.Value);
						})),

					}), out int _).Commit();
			}

			sqliteconnection.Close();
			sqliteconnection_districts.Close();
			sqliteconnection_municipalities.Close();
			sqliteconnection_provinces.Close();

			FileInfo fileinfo = new(sqlconnectionpath);
			FileInfo fileinfo_districts = new(sqlconnectionpath_districts);
			FileInfo fileinfo_municipalities = new(sqlconnectionpath_municipalities);

			string fileinfozipfile = fileinfo.ZipFile().Split('\\').Last();
			string fileinfogzipfile = fileinfo.GZipFile().Split('\\').Last();

			string fileinfo_districtszipfile = fileinfo_districts.ZipFile().Split('\\').Last();
			string fileinfo_districtsgzipfile = fileinfo_districts.GZipFile().Split('\\').Last();

			string fileinfo_municipalitieszipfile = fileinfo_municipalities.ZipFile().Split('\\').Last();
			string fileinfo_municipalitiesgzipfile = fileinfo_municipalities.GZipFile().Split('\\').Last();

			apifiles.Add(fileinfozipfile, "SAPS crime stats");
			apifiles.Add(fileinfogzipfile, "SAPS crime stats");

			fileinfo.Delete();
			fileinfo_districts.Delete();
			fileinfo_municipalities.Delete();

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