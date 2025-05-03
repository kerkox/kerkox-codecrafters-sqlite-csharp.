namespace codecrafters_sqlite.DbObjects;

public class TableIndex : Table
{
    public TableIndex(string type, string name, byte rootPage, string tblName, string sql) : base(type, name, rootPage, tblName, sql)
    {
    }
}