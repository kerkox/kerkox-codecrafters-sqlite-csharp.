using System.Text;
using codecrafters_sqlite.DbObjects;

namespace codecrafters_sqlite;

public class RootPage : Page
{
    public List<Table> Tables { get; } = new List<Table>();
    public List<TableIndex> Indexes { get; } = new List<TableIndex>();
    public RootPage(long pageOffset, Encoding dbEncoding, long dbPageSize, long pageNumber) 
        : base(pageOffset, dbEncoding, dbPageSize, pageNumber, CreateTable())
    {
        
    }

    private static Table CreateTable()
    {
        return new Table("table", "sqlite_schema", 1, "sqlite_schmea", "");
    }
    
    public void ReadHeader(Stream dbStream)
    {
        dbStream.Seek(PageOffset, SeekOrigin.Begin);
        byte[] pageHeaderBuffer = new byte[12];
        
        Helpers.ReadExactly(dbStream, pageHeaderBuffer, 0, 8);
        
        PageType = pageHeaderBuffer[0];
        
        CellCount = Helpers.ReadUInt16BigEndian(pageHeaderBuffer, 3);
        CellContentAreaStart = Helpers.ReadUInt16BigEndian(pageHeaderBuffer, 5);

        if (CellContentAreaStart == 0 && DbPageSize == 65536)
        {
            CellContentAreaStart = 65536;
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
            RightMostPointer = Helpers.ReadUInt32BigEndian(pageHeaderBuffer, 8);
        }
        else
        {
            RightMostPointer = 0;
        }
        // Console.WriteLine($"DEBUG: Read Header Page Offset={PageOffset}, Type=0x{PageType:X2}, NumCells={NumCells}, ContentStart={CellContentAreaStart}, HeaderSize={BTreePageHeaderSize}");
    }

    public void ReadSchemaCells(Stream dbStream)
    {
        if (PageType != 0x0D && PageType != 0x05)
        {
            Console.WriteLine(
                $"Advertenbcia: Se intentó leer celdas de esquema en una página no válida: {PageType:X2} en offset {PageOffset}");
            return;
        }

        var cellPointerArrayStart = CellContentAreaStart;
        var cellPointerBytes = new byte[2];
        long nextCellPointerOffset = 0;

        for (var i = 0; i < CellCount; i++)
        {
            var absoluteCellOffset = cellPointerArrayStart + (i * 2) + nextCellPointerOffset;
            dbStream.Seek(absoluteCellOffset, SeekOrigin.Begin);

            try
            {
                if (PageType == 0x0D)
                {
                    var (payloadSize, payloadBytes) = dbStream.ReadVarint();
                    var (rowId, rowIdBytes) = dbStream.ReadVarint();
                    nextCellPointerOffset += payloadSize;
                }
                else
                {
                    var leftChildPtrBytes = new byte[4];
                    dbStream.ReadExactly(leftChildPtrBytes, 0, 4);
                    var (key, keyBytes) = dbStream.ReadVarint();
                    continue;
                }

                var (recordHeaderSize, recordHeaderBytes) = dbStream.ReadVarint();
                var (typeSerialCode, typeCodeBytes) = dbStream.ReadVarint();
                var (nameSerialCode, nameCodeBytes) = dbStream.ReadVarint();
                var (tblNameSerialCode, tblNameCodeBytes) = dbStream.ReadVarint();
                var (rootPageSerialCode, rootPageCodeBytes) = dbStream.ReadVarint();
                var (sqlSerialCode, sqlCodeBytes) = dbStream.ReadVarint();

                // 5. Calcular tamaños de datos
                var typeDataSize = Helpers.CalculateDataSize(typeSerialCode);
                var nameDataSize = Helpers.CalculateDataSize(nameSerialCode);
                var tblNameDataSize = Helpers.CalculateDataSize(tblNameSerialCode);
                var rootPageDataSize = Helpers.CalculateDataSize(rootPageSerialCode);
                var sqlDataSize = Helpers.CalculateDataSize(sqlSerialCode);

                // 6. Leer Cuerpo del Registro
                var typeDataBuffer = new byte[typeDataSize];
                dbStream.ReadExactly(typeDataBuffer, 0, typeDataBuffer.Length);
                var typeValue = DbEncoding.GetString(typeDataBuffer);

                var nameDataBuffer = new byte[nameDataSize];
                var tableName = "";

                dbStream.ReadExactly(nameDataBuffer, 0, nameDataBuffer.Length);
                tableName = DbEncoding.GetString(nameDataBuffer);

                var tblNameDataBuffer = new byte[tblNameDataSize];
                dbStream.ReadExactly(tblNameDataBuffer, 0, tblNameDataBuffer.Length);
                var tblNameValue = DbEncoding.GetString(tblNameDataBuffer);

                var rootPageDataBuffer = new byte[rootPageDataSize];
                dbStream.ReadExactly(rootPageDataBuffer, 0, rootPageDataBuffer.Length);

                var sqlDataBuffer = new byte[sqlDataSize];
                dbStream.ReadExactly(sqlDataBuffer, 0, sqlDataBuffer.Length);
                var sqlValue = DbEncoding.GetString(sqlDataBuffer);

                if (typeValue == "table")
                {
                    if (!tableName.StartsWith("sqlite_"))
                    {
                        FoundTableNames.Add(tableName);
                        // Console.WriteLine($"      -> Found Table: {tableName}");
                    }
                    var table = new Table(typeValue, tableName, rootPageDataBuffer[0], tblNameValue, sqlValue);
                    Tables.Add(table);
                }
                else if (typeValue == "index")
                {
                    var index = new TableIndex(typeValue, tableName, rootPageDataBuffer[0], tblNameValue, sqlValue);
                    Indexes.Add(index);
                }
            }
            catch (EndOfStreamException ex)
            {
                Console.Error.WriteLine(
                    $"Error (EOF) procesando celda {i} en offset {absoluteCellOffset}: {ex.Message}");
                break;
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(
                    $"Error inesperado procesando celda {i} en offset {absoluteCellOffset}: {ex.Message} {ex.StackTrace}");
                // Considerar detenerse: break;
            }
        }
    }
}