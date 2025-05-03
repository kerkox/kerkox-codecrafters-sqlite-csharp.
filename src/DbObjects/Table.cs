using System.Text.RegularExpressions;

namespace codecrafters_sqlite.DbObjects;

public class Table
{
    public string Name { get; set; }
    public byte RootPage { get; set; }
    public string Sql { get; set; }
    public string Type { get; set; }
    public string TblName { get; set; }
    public long PageOffset { get; set; } = 0;
    public long NumCells { get; set; } = 0;
    public int NumColumns { get; set; } = 0;
    public List<string> ColumnNames { get; set; } = new List<string>();

    public Table(string type, string name, byte rootPage, string tblName, string sql)
    {
        Name = name;
        RootPage = rootPage;
        Type = type;
        TblName = tblName;
        Sql = sql;
        LoadColumnNames();
    }
    
    public void LoadColumnNames()
    {
        var regexColumns = @"(?<=\(\s*|,\s*)([\w]+|""[\w\s]+"")";
        var regex = new Regex(regexColumns, RegexOptions.IgnoreCase | RegexOptions.Multiline);
        if (string.IsNullOrEmpty(Sql)) return;
        var matches = regex.Matches(Sql);
        foreach (Match match in matches)
        {
            if (match.Groups.Count > 1)
            {
                ColumnNames.Add(match.Groups[1].Value.Replace("\"", "").ToLowerInvariant());
            }
        }
        NumColumns = ColumnNames.Count;
    }

    public int GetColumnIndex(string columnName)
    {
        return ColumnNames.IndexOf(Regex.Replace(columnName, "\"|\'", "").ToLowerInvariant());
    }
    
}