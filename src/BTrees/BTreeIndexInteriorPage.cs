using System.Text;

namespace codecrafters_sqlite.Btrees;

public class BTreeIndexInteriorPage : BTreePage
{
    public const byte Type = (byte)PageTypes.IndexInterior;
    

    public BTreeIndexInteriorPage(BTreePageHeader header) : base(header)
    {
    }
}