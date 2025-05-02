using System.Text;

namespace codecrafters_sqlite;

public class Page
{
    public byte PageType { get; set; }
    public ushort CellCount { get; set; }
    public long CellContentAreaStart { get; set; }
    public byte FragmentedFreeBytes { get; set; }
    public uint RightMostPointer { get; set; }
    public ushort BTreePageHeaderSize { get; set; }
    public List<string> FoundTableNames { get; } = new List<string>();
    
    public readonly long PageOffset;
    public readonly Encoding DbEncoding;
    public readonly long DbPageSize;
    public readonly string TableName;
    public Table Table { get; }
    public long PageNumber { get; set; }
    public int NumColumns { get; set; } = 0;


    public Page(long pageOffset, Encoding dbEncoding, long dbPageSize, long pageNumber, Table table)
    {
        PageOffset = pageOffset;
        DbEncoding = dbEncoding;
        DbPageSize = dbPageSize;
        PageNumber = pageNumber;
        Table = table;
        TableName = table.Name;
    }
}