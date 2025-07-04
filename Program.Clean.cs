using System.Globalization;

namespace Database.SAPSCrime
{
	internal partial class Program
	{
		static string CleanDistrict(string districtname)
		{
			string _districtname = districtname
				.ToLower()
				.Replace('-', ' ')
				.Replace("z f mgcawu", "zf mgcawu");

			return CultureInfo.CurrentCulture.TextInfo.ToTitleCase(_districtname);
		}
		static string CleanMuniciapility(string municipality)
		{
			string _municipality = municipality
				.ToLower()
				.Replace('-', ' ')
				.Replace("greater thubatsefetakgomo", "greater tubatse")
				.Replace("dr js moroka", "dr j.s. moroka")
				.Replace("jb marks", "j b marks")
				.Replace("kagisanomolopo", "kagisano-molopo")
				.Replace("sol plaatjie", "sol plaatje")
				.Replace("ga segonyana", "ga-segonyana")
				.Replace("Khâ\u0094\u009cÃ³i Ma", "khai-ma")
				.Replace("kai !garib", "!kai! garib")
				.Replace("city of cape town", "cape town")
				.Replace("dr pixley ka isaka seme", "pixley ka seme (mp)")
				.Replace("chief albert luthuli", "albert luthuli")
				.Replace("mbombelaumjindi", "mbombela")
				.Replace("modimollemookgophong", "modimolle-mookgopong")
				.Replace("ba phalaborwa", "ba-phalaborwa")
				.Replace("lepele nkumpi", "lepelle-nkumpi")
				.Replace("nqutu", "nquthu")
				.Replace("the msunduzi", "msunduzi")
				.Replace("hlabisa big five", "big five hlabisa")
				.Replace("maluti a phofung", "maluti-a-phofung")
				.Replace("ngquza hill", "ngquza hills")
				.Replace("maletswaigariep", "maletswai")
				.Replace("kou kamma", "kou-kamma")
				.Replace("dr ab xuma", "dr. a.b. xuma")
				.Replace("blue crane routie", "blue crane route")
				.Replace("winnie madikizela mandela local municipality", "winnie madikizela-mandela");

			return CultureInfo.CurrentCulture.TextInfo.ToTitleCase(_municipality);
		}
		static string CleanProvince(string provincename)
		{
			return provincename
				.Replace('-', ' ')
				.Replace("kwazulu natal", "kwazulu-natal");
		}
	}
}