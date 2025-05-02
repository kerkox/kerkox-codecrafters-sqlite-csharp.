namespace codecrafters_sqlite.Btrees;

public class BTreeIndexLeafPage : BTreePage
{
    public const byte Type = (byte)PageTypes.IndexLeaf;

    public BTreeIndexLeafPage(BTreePageHeader header) : base(header)
    {
    }
    
}