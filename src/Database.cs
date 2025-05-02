using System.Text;
using codecrafters_sqlite.Btrees;

namespace codecrafters_sqlite;

public class Database : IDisposable
{
    private string DbPath { get; }
    public Encoding DbEncoding { get; }
    public long DbPageSize { get; }
    public RootPage RootPage { get; set;  }
    private readonly FileStream _databaseStream;
    

    public Database(string dbPath)
    {
        DbPath = dbPath;
        _databaseStream = File.OpenRead(DbPath);
        
        DbEncoding = Helpers.GetDbEncoding(_databaseStream);
        
        _databaseStream.Seek(16, SeekOrigin.Begin); // Skip the first 16 bytes
        byte[] pageSizeBytes = new byte[2];
        _databaseStream.ReadExactly(pageSizeBytes, 0, 2);
        DbPageSize = Helpers.ReadUInt16BigEndian(pageSizeBytes);
        RootPage = LoadRootPage();
    }
    
    public List<List<object>>  GetFieldValuesFromTableWithBTree(string tableName, string[] fieldNames, Dictionary<string, string> filters)
    {
        var table = RootPage.Tables.FirstOrDefault(t => t.Name.Equals(tableName, StringComparison.OrdinalIgnoreCase));
        if (table == null) return new List<List<object>>();
        var navigator = new BTreeNavigator(table.RootPage, DbPageSize, DbEncoding, table);
        var fieldValues = navigator.ReadData(_databaseStream, fieldNames, filters);
        return fieldValues;
    }
    
    public int GetNumberOfRowsFromTable(string tableName)
    {
        var table = RootPage.Tables.FirstOrDefault(t => t.Name.Equals(tableName, StringComparison.OrdinalIgnoreCase));
        if (table == null) return 0;
        var navigator = new BTreeNavigator(table.RootPage, DbPageSize, DbEncoding, table);
        var numRows = navigator.GetNumberOfRows(_databaseStream);
        return numRows;
    }

    private RootPage LoadRootPage()
    {
        const long pageOffset = 100;
        const int pageNumber = 1;
        var rootPage = new RootPage(pageOffset, DbEncoding, DbPageSize, pageNumber);
        rootPage.ReadHeader(_databaseStream);
        rootPage.ReadSchemaCells(_databaseStream);
        return rootPage;
    }

    public void Dispose()
    {
        _databaseStream?.Dispose();
        GC.SuppressFinalize(this);
    }
}