
namespace Database.SAPSCrime.Inputs.CSVs
{
    public class CSVRow
    {
        public CSVRow(string line)
        {
			string[] linesplit = line.Split(',');

			Line = line;
			Year = int.Parse(linesplit[0]);			
			Category = linesplit[1];			
			Value = int.Parse(linesplit[2]);			
		}

        public string Line { get; set; }
        public int LineNumber { get; set; }
        public int Year { get; set; }
        public string Category { get; set; }
        public int Value { get; set; }
	}
}