namespace codecrafters_sqlite;

public class Table
{
    public string Name { get; set; }
    public byte RootPage { get; set; }
    public string Sql { get; set; }
    public string Type { get; set; }
    public string TblName { get; set; }
    public long PageOffset { get; set; } = 0;
    public long NumCells { get; set; } = 0;

    public Table(string type, string name, byte rootPage, string tblName, string sql)
    {
        Name = name;
        RootPage = rootPage;
        Type = type;
        TblName = tblName;
        Sql = sql;
    }
    
}