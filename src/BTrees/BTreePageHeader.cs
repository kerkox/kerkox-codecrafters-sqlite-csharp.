using System.Text;

namespace codecrafters_sqlite.Btrees;

public class BTreePageHeader
{
    public byte PageType { get; set; }
    public ushort FirstFreeBlockOffset { get; set; }
    public ushort CellCount { get; set; }
    public uint CellOffsetContentStart { get; set; }
    public byte FragmentedFreeBytes { get; set; }
    public uint RightmostChildPageNumber { get; set; }
    
    public long PageOffset { get; set; }
    public Encoding DbEncoding { get; set; }
    public long DbPageSize { get; set; }
    public long PageNumber { get; set; }
    public Table Table { get; set; }
    public ushort BTreePageHeaderSize { get; set; }

    public BTreePageHeader(long pageOffset, Encoding dbEncoding, long dbPageSize, long pageNumber, Table table, Stream dbStream)
    {
        PageOffset = pageOffset;
        DbEncoding = dbEncoding;
        DbPageSize = dbPageSize;
        PageNumber = pageNumber;
        Table = table;
        ReadHeader(dbStream);
    }
    
    private void ReadHeader(Stream dbStream)
    {
        dbStream.Seek(PageOffset, SeekOrigin.Begin);
        byte[] pageHeaderBuffer = new byte[12];
        
        Helpers.ReadExactly(dbStream, pageHeaderBuffer, 0, 8);
        
        PageType = pageHeaderBuffer[0];
        
        CellCount = Helpers.ReadUInt16BigEndian(pageHeaderBuffer, 3);
        CellOffsetContentStart = Helpers.ReadUInt16BigEndian(pageHeaderBuffer, 5);

        if (CellOffsetContentStart == 0 && DbPageSize == 65536)
        {
            CellOffsetContentStart = 65536;
        }
        
        BTreePageHeaderSize = PageType switch
        {
            0x02 => 12, // Index Interior
            0x05 => 12, // Table Interior
            0x0A => 8,  // Index Leaf
            0x0D => 8,  // Table Leaf
            _ => 8 // Asumir 8 para otros tipos desconocidos? O lanzar error?
            // throw new NotSupportedException($"Unsupported page type: {PageType:X2}");
        };

        if (BTreePageHeaderSize == 12)
        {
            Helpers.ReadExactly(dbStream, pageHeaderBuffer, 8, 4);
            RightmostChildPageNumber = Helpers.ReadUInt32BigEndian(pageHeaderBuffer, 8);
        }
        else
        {
            RightmostChildPageNumber = 0;
        }
        // Console.WriteLine($"DEBUG: Read Header Page Offset={PageOffset}, Type=0x{PageType:X2}, NumCells={NumCells}, ContentStart={CellContentAreaStart}, HeaderSize={BTreePageHeaderSize}");
    }
}