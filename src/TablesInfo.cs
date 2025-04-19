using System.Text;

namespace codecrafters_sqlite;

public class TablesInfo
{

    private readonly string _dbPath;
    
    public TablesInfo(string dbPath)
    {
        _dbPath = dbPath;
    }

    public void PrintTables()
    {
        using var databaseFile = File.OpenRead(_dbPath);
        var database = new Database(_dbPath);
        var tableNames = new List<string>();
        
        var page = new Page(100, database.DbEncoding, database.DbPageSize, 1);
        // var numCells, cellContentAreaStart, pageType) = GetPageInfo(databaseFile);

        long cellPointerArrayOffset = page.CellContentAreaStart;
        long nextCellPointerOffset = 0;
        
        for (var i = 0; i < page.NumCells; i++)
        {
            var cellOffset = cellPointerArrayOffset + nextCellPointerOffset + (i * 2);
            databaseFile.Seek(cellOffset, SeekOrigin.Begin);
            
            try
            {
                if (page.PageType == 0x0D)
                {
                    var (payloadSize, payloadBytes) = Helpers.ReadVarint(databaseFile);
                    var (rowId, rowIdBytes) = Helpers.ReadVarint(databaseFile);
                    nextCellPointerOffset += payloadSize;
                    // Console.WriteLine($"RowId: {rowId} Payload Size: ({rowIdBytes} bytes)");
                }
                else
                {
                    var leftChildPtrBytes = new byte[4];
                    Helpers.ReadExactly(databaseFile, leftChildPtrBytes, 0, 4);
                    var (key, keyBytes) = Helpers.ReadVarint(databaseFile);
                    // Console.WriteLine($"Key: {key} Key Bytes: ({keyBytes} bytes)");
                    continue;
                }

                var (recordHeaderSize, recordHeaderBytes) = Helpers.ReadVarint(databaseFile);
                var (typeSerialCode, typeCodeBytes) = Helpers.ReadVarint(databaseFile);
                var (nameSerialCode, nameCodeBytes) = Helpers.ReadVarint(databaseFile);
                var (tblNameSerialCode, tblNameCOdeBytes) = Helpers.ReadVarint(databaseFile);
                var (rootPageSerialCode, rootPageCodeBytes) = Helpers.ReadVarint(databaseFile);
                var (sqlSerialCode, sqlCodeBytes) = Helpers.ReadVarint(databaseFile);


                var typeDataSize = Helpers.CalculateDataSize(typeSerialCode);
                var nameDataSize = Helpers.CalculateDataSize(nameSerialCode);
                var tblNameDataSize = Helpers.CalculateDataSize(tblNameSerialCode);
                var rootPageDataSize = Helpers.CalculateDataSize(rootPageSerialCode);
                var sqlDataSize = Helpers.CalculateDataSize(sqlSerialCode);


                var typeDataBuffer = new byte[typeDataSize];
                Helpers.ReadExactly(databaseFile, typeDataBuffer, 0, typeDataBuffer.Length);
                var typeValue = database.DbEncoding.GetString(typeDataBuffer);
                // Console.WriteLine($"Type: {typeValue} ({typeDataSize} bytes)");
                
                if (typeValue == "table")
                {
                    var nameDataBuffer = new byte[nameDataSize];
                    Helpers.ReadExactly(databaseFile, nameDataBuffer, 0, nameDataBuffer.Length);
                    var tableName = database.DbEncoding.GetString(nameDataBuffer);
                    
                    var tblNameDataBuffer = new byte[tblNameDataSize];
                    Helpers.ReadExactly(databaseFile, tblNameDataBuffer, 0, tblNameDataBuffer.Length);
                    var tblNameValue = database.DbEncoding.GetString(tblNameDataBuffer);
                    
                    var rootPageDataBuffer = new byte[rootPageDataSize];
                    Helpers.ReadExactly(databaseFile, rootPageDataBuffer, 0, rootPageDataBuffer.Length);
                    var rootPageValue = database.DbEncoding.GetString(rootPageDataBuffer);
                    
                    var sqlDataBuffer = new byte[sqlDataSize];
                    Helpers.ReadExactly(databaseFile, sqlDataBuffer, 0, sqlDataBuffer.Length);
                    var sqlValue = database.DbEncoding.GetString(sqlDataBuffer);

                    if (!tableName.StartsWith("sqlite_"))
                    {
                        tableNames.Add(tableName);
                        // Console.WriteLine($"Table Name: {tableName} ({nameDataSize} bytes)");
                    }
                    else
                    {
                        // Console.WriteLine($"-> Skipping internal table: {tableName}");
                    }
                }
            }
            catch (EndOfStreamException ex)
            {
                Console.WriteLine($"Error reading cell content area: {ex.Message}");
                break;
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(
                    $"Unexpected error processing cell: {i} in offset: {cellOffset} - {ex.Message}");
            }
        }

        tableNames.Reverse();
        Console.WriteLine(string.Join(" ", tableNames));
    }
}