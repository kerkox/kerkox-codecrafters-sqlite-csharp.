namespace codecrafters_sqlite.Btrees;

public class BTreeTableInteriorPage : BTreePage
{
    public const byte Type = (byte)PageTypes.TableInterior;
    
    public BTreeTableInteriorPage(BTreePageHeader header) : base(header)
    {
    }

    public List<uint> GetAllChildPageNumbers(Stream dbStream)
    {
        var childPages = new List<uint>();
        long cellPointerArrayStart = Header.PageOffset + Header.BTreePageHeaderSize;
        byte[] cellPointerBytes = new byte[2];
        byte[] leftChildPtrBytes = new byte[4];

        for (int i = 0; i < Header.CellCount; i++)
        {
            long pointerLocation = cellPointerArrayStart + (i * 2);
            dbStream.Seek(pointerLocation, SeekOrigin.Begin);
            dbStream.ReadExactly(cellPointerBytes, 0, 2);
            ushort relativeCellOffset = Helpers.ReadUInt16BigEndian(cellPointerBytes);
            long absoluteCellOffset = Header.PageOffset + relativeCellOffset;
            dbStream.Seek(absoluteCellOffset, SeekOrigin.Begin);
            
            dbStream.ReadExactly(leftChildPtrBytes, 0, 4);
            uint leftChildPage = Helpers.ReadUInt32BigEndian(leftChildPtrBytes);
            childPages.Add(leftChildPage);
            var (rowId, _) = dbStream.ReadVarint();
        }
        // childPages.Add(Header.RightmostChildPageNumber);
        
        return childPages;
    }

    public uint GetLeftChildPage(Stream dbStream)
    {
        // Leer desde el contentArea 

        long cellPointerArrayStart = Header.PageOffset + Header.BTreePageHeaderSize;
        byte[] cellPointerBytes = new byte[2];
        dbStream.Seek(cellPointerArrayStart, SeekOrigin.Begin);
        dbStream.ReadExactly(cellPointerBytes, 0, 2);
        ushort relativeCellOffset = Helpers.ReadUInt16BigEndian(cellPointerBytes);
        long absoluteCellOffset = Header.PageOffset + relativeCellOffset;
        
        dbStream.Seek(absoluteCellOffset, SeekOrigin.Begin);
        
        // Leer Puntero Hijo Izquierdo (4 bytes) y Clave (Varint)
        byte[] leftChildPtrBytes = new byte[4];
        dbStream.ReadExactly(leftChildPtrBytes, 0, 4);
        uint leftChildPage = Helpers.ReadUInt32BigEndian(leftChildPtrBytes);
        var (rowId, keyBytes) = dbStream.ReadVarint();
        return leftChildPage;
    }
    
}