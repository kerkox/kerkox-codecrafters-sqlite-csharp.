using System.Text;

namespace codecrafters_sqlite.Btrees;

public class BTreePage
{
    public BTreePageHeader Header { get; set; }

    public BTreePage(BTreePageHeader header)
    {
        Header = header;
    }

    public BTreePage(long pageOffset, Encoding dbEncoding, long dbPageSize, long pageNumber, Table table, Stream dbStream)
    {
        Header = new BTreePageHeader(pageOffset, dbEncoding, dbPageSize, pageNumber, table, dbStream);
    }
    
}