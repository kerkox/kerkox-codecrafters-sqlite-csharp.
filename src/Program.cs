using System.Collections.Generic;
using System.Buffers.Binary;
using System.Text;

ushort ReadUInt16BigEndian(byte[] buffer, int offset = 0)
{
    return BinaryPrimitives.ReadUInt16BigEndian(buffer.AsSpan(offset));
}

uint ReadUInt32BigEndian(byte[] buffer, int offset = 0)
{
    return BinaryPrimitives.ReadUInt32BigEndian(buffer.AsSpan(offset));
}

void ReadExactly(Stream stream, byte[] buffer, int offset, int count)
{
    int totalBytesRead = 0;
    while (totalBytesRead < count)
    {
        int bytesRead = stream.Read(buffer, offset + totalBytesRead, count - totalBytesRead);
        if (bytesRead == 0)
        {
            // End of stream reached before reading all requested bytes
            throw new EndOfStreamException($"Se esperaban {count} bytes, pero se alcanzó el final del stream después de leer {totalBytesRead} bytes.");
        }
        totalBytesRead += bytesRead;
    }
}

(long value, int bytesRead) ReadVarint(Stream stream)
{
    long value = 0;
    int bytesRead = 0;
    byte[] buffer = new byte[1];
    byte b;

    for (int i = 0; i < 9; i++) // Un Varint tiene como máximo 9 bytes
    {
        try
        {
            ReadExactly(stream, buffer, 0, 1);
        }
        catch (EndOfStreamException ex)
        {
            throw new EndOfStreamException("Stream ended while reading Varint.", ex);
        }
        
        bytesRead++;
        b = buffer[0];

        if (i == 8) // El 9no byte usa los 8 bits
        {
            value = (value << 8) | b;
            break;
        }
        else
        {
            // Toma los 7 bits inferiores
            value = (value << 7) | ((byte)(b & 0x7F));
            // Si el bit más significativo es 0, es el último byte
            if ((b & 0x80) == 0)
            {
                break;
            }
        }
    }
    return (value, bytesRead);
}


Encoding GetDbEncoding(Stream dbStream)
{
    long originalPosition = dbStream.Position; // Guardar posición actual
    try
    {
        dbStream.Seek(56, SeekOrigin.Begin); // Offset de Text Encoding
        byte[] encodingBytes = new byte[4];
        ReadExactly(dbStream, encodingBytes, 0, 4);
        uint encodingCode = ReadUInt32BigEndian(encodingBytes);
        switch (encodingCode)
        {
            case 1: return Encoding.UTF8;
            case 2: return Encoding.Unicode; // UTF-16 LE
            case 3: return Encoding.BigEndianUnicode; // UTF-16 BE
            default:
                Console.Error.WriteLine($"Advertencia: Codificación de texto no soportada ({encodingCode}). Usando UTF-8 por defecto.");
                return Encoding.UTF8; // O lanzar excepción si prefieres ser estricto
        }
    }
    finally
    {
        dbStream.Seek(originalPosition, SeekOrigin.Begin); // Restaurar posición
    }
}

long CalculateDataSize(long serialTypeCode)
{
    if (serialTypeCode >= 0 && serialTypeCode <= 4) return serialTypeCode; // NULL, INT8, INT16, INT24, INT32
    if (serialTypeCode == 5) return 6; // INT48
    if (serialTypeCode == 6) return 8; // INT64
    if (serialTypeCode == 7) return 8; // FLOAT64
    if (serialTypeCode == 8) return 0; // Constant 0
    if (serialTypeCode == 9) return 0; // Constant 1
    // 10 y 11 son reservados
    if (serialTypeCode >= 12 && serialTypeCode % 2 == 0) // BLOB
    {
        return (serialTypeCode - 12) / 2;
    }
    if (serialTypeCode >= 13 && serialTypeCode % 2 != 0) // TEXT
    {
        return (serialTypeCode - 13) / 2;
    }
    // Si llegamos aquí, es un tipo desconocido o reservado
    Console.Error.WriteLine($"Advertencia: Serial Type Code desconocido o reservado: {serialTypeCode}");
    return 0; // O lanzar excepción
}

// Parse arguments
var (path, command) = args.Length switch
{
    0 => throw new InvalidOperationException("Missing <database path> and <command>"),
    1 => throw new InvalidOperationException("Missing <command>"),
    _ => (args[0], args[1])
};

using var databaseFile = File.OpenRead(path);

// Parse command and act accordingly
if (command == ".dbinfo")
{
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    Console.WriteLine("Logs from your program will appear here!");

    // Uncomment this line to pass the first stage
    databaseFile.Seek(16, SeekOrigin.Begin); // Skip the first 16 bytes
    byte[] pageSizeBytes = new byte[2];
    ReadExactly(databaseFile, pageSizeBytes, 0, 2);
    var pageSize = ReadUInt16BigEndian(pageSizeBytes);
    Console.WriteLine($"database page size: {pageSize}");

    databaseFile.Seek(103, SeekOrigin.Begin);
    byte[] numberOfTables = new byte[2];
    ReadExactly(databaseFile, numberOfTables, 0, 2);
    Console.WriteLine($"number of tables: {ReadUInt16BigEndian(numberOfTables)}");
}
else if (command == ".tables")
{
    List<string> tableNames = new List<string>();
    Encoding dbEncoding = GetDbEncoding(databaseFile);
    
    databaseFile.Seek(16, SeekOrigin.Begin); // Skip the first 16 bytes
    byte[] pageSizeBytes = new byte[2];
    ReadExactly(databaseFile, pageSizeBytes, 0, 2);
    var pageSize = ReadUInt16BigEndian(pageSizeBytes);

    databaseFile.Seek(100, SeekOrigin.Begin);
    byte[] pageHeaderBuffer = new byte[8];
    ReadExactly(databaseFile, pageHeaderBuffer, 0, 8);

    byte pageType = pageHeaderBuffer[0];
    if (pageType != 0x0D && pageType != 0x05)
    {
        throw new NotSupportedException($"Invalid page type: {pageType:X2} for .tables command");
    }

    int pageHeaderSize = (pageType == 0x0D) ? 8 : 12;
    ushort numCells = ReadUInt16BigEndian(pageHeaderBuffer, 3);
    ushort cellContentAreaStart = ReadUInt16BigEndian(pageHeaderBuffer, 5);

    // Console.WriteLine($"Page Type: {pageType:X2} Number of Cells: {numCells}");

    long cellPointerArrayOffset = cellContentAreaStart;
    long nextCellPointerOffset = 0;
    byte[] cellPointerBytes = new byte[2];

    for (int i = 0; i < numCells; i++)
    {
        var cellOffset = cellPointerArrayOffset + nextCellPointerOffset + (i * 2);
        databaseFile.Seek(cellOffset, SeekOrigin.Begin);
        // ReadExactly(databaseFile, cellPointerBytes, 0, 2);
        // ushort cellOffset = ReadUInt16BigEndian(cellPointerBytes);

        // Console.WriteLine($"Cell {i} Offset: {cellOffset}");
        // databaseFile.Seek(cellOffset, SeekOrigin.Begin);

        try
        {
            if (pageType == 0x0D)
            {
                var (payloadSize, payloadBytes) = ReadVarint(databaseFile);
                var (rowId, rowIdBytes) = ReadVarint(databaseFile);
                nextCellPointerOffset += payloadSize;
                // Console.WriteLine($"   RowId: {rowId} Payload Size: ({rowIdBytes} bytes)");
            }
            else
            {
                byte[] leftChildPtrBytes = new byte[4];
                ReadExactly(databaseFile, leftChildPtrBytes, 0, 4);
                var (key, keyBytes) = ReadVarint(databaseFile);
                // Console.WriteLine($"   Key: {key} Key Bytes: ({keyBytes} bytes)");
                continue;
            }

            var (recordHeaderSize, recordHeaderBytes) = ReadVarint(databaseFile);
            var (typeSerialCode, typeCodeBytes) = ReadVarint(databaseFile);
            var (nameSerialCode, nameCodeBytes) = ReadVarint(databaseFile);
            var (tblNameSerialCode, tblNameCOdeBytes) = ReadVarint(databaseFile);
            var (rootPageSerialCode, rootPageCodeBytes) = ReadVarint(databaseFile);
            var (sqlSerialCode, sqlCodeBytes) = ReadVarint(databaseFile);


            long typeDataSize = CalculateDataSize(typeSerialCode);
            long nameDataSize = CalculateDataSize(nameSerialCode);
            long tblNameDataSize = CalculateDataSize(tblNameSerialCode);
            long rootPageDataSize = CalculateDataSize(rootPageSerialCode);
            long sqlDataSize = CalculateDataSize(sqlSerialCode);
            

            byte[] typeDataBuffer = new byte[typeDataSize];
            ReadExactly(databaseFile, typeDataBuffer, 0, typeDataBuffer.Length);
            string typeValue = dbEncoding.GetString(typeDataBuffer);
            // Console.WriteLine($"   Type: {typeValue} ({typeDataSize} bytes)");

            if (typeValue == "table")
            {
                byte[] nameDataBuffer = new byte[nameDataSize];
                ReadExactly(databaseFile, nameDataBuffer, 0, nameDataBuffer.Length);
                string tableName = dbEncoding.GetString(nameDataBuffer);

                if (!tableName.StartsWith("sqlite_"))
                {
                    tableNames.Add(tableName);
                    // Console.WriteLine($"   Table Name: {tableName} ({nameDataSize} bytes)");
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
            Console.Error.WriteLine($"Unexpected error processing cell: {i} in offset: {cellOffset} - {ex.Message}");
        }
    }

    tableNames.Reverse();
    Console.WriteLine(string.Join(" ", tableNames));

}
else
{
    throw new InvalidOperationException($"Invalid command: {command}");
}
