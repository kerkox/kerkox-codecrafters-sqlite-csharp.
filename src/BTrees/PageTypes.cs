namespace codecrafters_sqlite.Btrees;

public enum PageTypes
{
    TableLeaf = 0x0D,
    TableInterior = 0x05,
    IndexLeaf = 0x0A,
    IndexInterior = 0x02,
}