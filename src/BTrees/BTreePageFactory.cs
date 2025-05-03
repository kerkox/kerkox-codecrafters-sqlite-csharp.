using System.Text;
using codecrafters_sqlite.DbObjects;

namespace codecrafters_sqlite.Btrees;

public class BTreePageFactory
{
    // Here we define the types of B-Tree pages
    public static BTreePage CreatePage(Stream dbStream, long pageOffset, Encoding dbEncoding, long dbPageSize, long pageNumber, Table table)
    {
        var btreePage = new BTreePage(pageOffset, dbEncoding, dbPageSize, pageNumber, table, dbStream);
        return btreePage.Header.PageType switch
        {
            BTreeTableLeafPage.Type => new BTreeTableLeafPage(btreePage.Header),
            BTreeTableInteriorPage.Type => new BTreeTableInteriorPage(btreePage.Header),
            BTreeIndexLeafPage.Type => new BTreeIndexLeafPage(btreePage.Header),
            BTreeIndexInteriorPage.Type => new BTreeIndexInteriorPage(btreePage.Header),
            _ => throw new ArgumentException($"Unknown page type: {btreePage.Header.PageType}")
        };
    }
}