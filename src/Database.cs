using System.Text;

namespace codecrafters_sqlite;

public class Database : IDisposable
{
    private string DbPath { get; }
    public Encoding DbEncoding { get; }
    public long DbPageSize { get; }
    public List<Page> Pages { get; } = new List<Page>();
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
        LoadSpecificPage(1);
    }
    
    public int GetNumberOfRowsFromTable(string tableName)
    {
        var page1 = Pages.FirstOrDefault(p => p.PageOffset == 100);
        if (page1 == null)
        {
            LoadSpecificPage(1);
            page1 = Pages.FirstOrDefault(p => p.PageOffset == 100);
        }
        
        var table = page1?.Tables.FirstOrDefault(t => t.Name.Equals(tableName, StringComparison.OrdinalIgnoreCase));
        if (table == null) return 0;
        LoadSpecificPage(table.RootPage);
        var numRows = Pages.FirstOrDefault(p => p.PageNumber == table.RootPage)?.NumCells ?? 0;
        return numRows;
    }
    
    public void LoadSpecificPage(int pageNumber)
    {
        if(pageNumber < 1) throw new ArgumentOutOfRangeException(nameof(pageNumber));
        long pageOffset = (pageNumber == 1) ? 100 : (long)(pageNumber - 1) * DbPageSize;
        if (pageOffset < _databaseStream.Length)
        {
            var page = new Page(pageOffset, DbEncoding, DbPageSize, pageNumber);

            page.ReadHeader(_databaseStream);

            if (pageNumber == 1)
            {
                page.ReadSchemaCells(_databaseStream);
            }
            Pages.Add(page);
        }
    }
    
    private void LoadAllPages()
    {
        if(_databaseStream.Length > 100)
        {   
            var page1 = new Page(100, DbEncoding, DbPageSize, 1);
            page1.ReadHeader(_databaseStream);
            Pages.Add(page1);
        }
        
        long pageOffset = DbPageSize;
        long pageNumber = 2;
        while (pageOffset < _databaseStream.Length)
        {
            // databaseFile.Seek(pageOffset, SeekOrigin.Begin);
            var page = new Page(pageOffset, DbEncoding, DbPageSize, pageNumber);
            page.ReadHeader(_databaseStream);
            Pages.Add(page);
            pageNumber++;
            pageOffset += DbPageSize; // Move to the next page
        }
    }

    public IEnumerable<string> GetTableNames()
    {
        var page1 = Pages.FirstOrDefault(p => p.PageOffset == 100);
        if (page1 == null)
        {
            LoadSpecificPage(1);
            page1 = Pages.FirstOrDefault(p => p.PageOffset == 100);
        }
        
        return page1?.FoundTableNames ?? Enumerable.Empty<string>();
    }
    
    public void Dispose()
    {
        _databaseStream?.Dispose();
        GC.SuppressFinalize(this);
    }
    
    // private void LoadPages()
    // {
    //     using var databaseFile = File.OpenRead(DbPath);
    //     var page = new Page(DbPath, 100, DbEncoding);
    //     page.ReadCells();
    //     Pages.Add(page);
    //     long pageOffset = DbPageSize;
    //     while (pageOffset < databaseFile.Length)
    //     {
    //         // databaseFile.Seek(pageOffset, SeekOrigin.Begin);
    //         var newPage = new Page(DbPath, pageOffset, DbEncoding);
    //         newPage.ReadCells();
    //         Pages.Add(newPage);
    //         pageOffset += DbPageSize; // Move to the next page
    //     }
    // }
}