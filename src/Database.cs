using System.Text;

namespace codecrafters_sqlite;

public class Database : IDisposable
{
    private string DbPath { get; }
    public Encoding DbEncoding { get; }
    public long DbPageSize { get; }
    public RootPage RootPage { get; set;  }
    public Dictionary<int, Page> Pages { get; } = new Dictionary<int, Page>();
    private FileStream _databaseStream;
    

    public Database(string dbPath)
    {
        DbPath = dbPath;
        _databaseStream = File.OpenRead(DbPath);
        
        DbEncoding = Helpers.GetDbEncoding(_databaseStream);
        
        _databaseStream.Seek(16, SeekOrigin.Begin); // Skip the first 16 bytes
        byte[] pageSizeBytes = new byte[2];
        Helpers.ReadExactly(_databaseStream, pageSizeBytes, 0, 2);
        DbPageSize = Helpers.ReadUInt16BigEndian(pageSizeBytes);
        RootPage = LoadRootPage();
    }
    
    public List<List<object>>  GetFieldValuesFromTable(string tableName, string[] fieldNames)
    {
        var table = RootPage.Tables.FirstOrDefault(t => t.Name.Equals(tableName, StringComparison.OrdinalIgnoreCase));
        if (table == null) return new List<List<object>>();
        LoadSpecificPage(table.RootPage);
        var page = Pages[table.RootPage];
        var fieldValues = page.GetFieldValues(fieldNames, _databaseStream);
        return fieldValues;
    }
    
    private int GetColumnIndex(string fieldName, Table table)
    {
        var columnIndex = table.Sql
            .Split(new[] { '(', ',', ')' }, StringSplitOptions.RemoveEmptyEntries)
            .Select((col, index) => new { col, index })
            .FirstOrDefault(x => x.col.Trim().Equals(fieldName, StringComparison.OrdinalIgnoreCase))?.index;
        return columnIndex ?? -1;
    }
    
    public int GetNumberOfRowsFromTable(string tableName)
    {
        var table = RootPage.Tables.FirstOrDefault(t => t.Name.Equals(tableName, StringComparison.OrdinalIgnoreCase));
        if (table == null) return 0;
        LoadSpecificPage(table.RootPage);
        var numRows = Pages[table.RootPage]?.NumCells ?? 0;
        return numRows;
    }

    private int LoadSpecificPage(int pageNumber)
    {
        if(pageNumber < 1) throw new ArgumentOutOfRangeException(nameof(pageNumber));
        var pageOffset = (long)(pageNumber - 1) * DbPageSize;
        if (pageNumber == 1) return 1;
        if (pageOffset >= _databaseStream.Length) return 0;
        var table = RootPage.Tables.FirstOrDefault(t => t.RootPage == pageNumber);
        if (table == null) return 0; // Table not found
        var page = new Page(pageOffset, DbEncoding, DbPageSize, pageNumber, table);
        page.ReadHeader(_databaseStream);
        Pages.Add(pageNumber, page);
        return 1;
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

    public IEnumerable<string> GetTableNames()
    {
        return RootPage.FoundTableNames;
    }
    
    public void Dispose()
    {
        _databaseStream?.Dispose();
        GC.SuppressFinalize(this);
    }
}