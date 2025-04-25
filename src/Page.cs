using System.Text;

namespace codecrafters_sqlite;

public class Page
{
    public byte PageType { get; set; }
    public ushort NumCells { get; set; }
    public long CellContentAreaStart { get; set; }
    public byte FragmentedFreeBytes { get; set; }
    public uint RightMostPointer { get; set; }
    public ushort BTreePageHeaderSize { get; set; }
    public List<string> FoundTableNames { get; } = new List<string>();
    
    public readonly long PageOffset;
    public readonly Encoding DbEncoding;
    public readonly long DbPageSize;
    public readonly string TableName;
    public Table Table { get; }
    public long PageNumber { get; set; }
    public int NumColumns { get; set; } = 0;


    public Page(long pageOffset, Encoding dbEncoding, long dbPageSize, long pageNumber, Table table)
    {
        PageOffset = pageOffset;
        DbEncoding = dbEncoding;
        DbPageSize = dbPageSize;
        PageNumber = pageNumber;
        Table = table;
        TableName = table.Name;
    }

    public virtual void ReadHeader(Stream dbStream)
    {
        dbStream.Seek(PageOffset, SeekOrigin.Begin);
        byte[] pageHeaderBuffer = new byte[12];
        
        Helpers.ReadExactly(dbStream, pageHeaderBuffer, 0, 8);
        
        PageType = pageHeaderBuffer[0];
        
        NumCells = Helpers.ReadUInt16BigEndian(pageHeaderBuffer, 3);
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
    
    public List<List<object>> ReadCells(Stream dbStream, Dictionary<int, string> filters)
    {
        if (PageType != 0x0D && PageType != 0x05 && PageType != 0x0A && PageType != 0x02)
        {
            Console.WriteLine(
                $"Advertencia: Se intentó leer celdas de esquema en una página no válida: {PageType:X2} en offset {PageOffset}");
            return new List<List<object>>();
        }
        long cellPointerArrayStart = PageOffset + BTreePageHeaderSize;
        byte[] cellPointerBytes = new byte[2];
        
        
        // long nextCellPointerOffset = 0;
        List<List<object>> results = new List<List<object>>();
        for (var i = 0; i < NumCells; i++)
        {
            // 1. Calcular la posición del puntero actual
            long pointerLocation = cellPointerArrayStart + (i * 2);
            
            // 2. Leer el valor del puntero (offset relativo de la celda)
            dbStream.Seek(pointerLocation, SeekOrigin.Begin);
            Helpers.ReadExactly(dbStream, cellPointerBytes, 0, 2);
            ushort relativeCellOffset = Helpers.ReadUInt16BigEndian(cellPointerBytes);
            
            // 3. Calcular el offset absoluto de la celda en el archivo
            //    ¡ESTE ES EL OFFSET DONDE COMIENZA LA CELDA!
            long absoluteCellOffset = PageOffset + relativeCellOffset;
            
            // 4. Ir al inicio de la celda
            dbStream.Seek(absoluteCellOffset, SeekOrigin.Begin);
            var rowIdValue = -1;
            try
            {
                if (PageType == 0x0D)
                {
                    var (payloadSize, payloadBytes) = Helpers.ReadVarint(dbStream);
                    var (rowId, rowIdBytes) = Helpers.ReadVarint(dbStream);
                    rowIdValue = (int)rowId;
                }
                else if (PageType == 0x05)
                {
                    // Leer Puntero Hijo Izquierdo (4 bytes) y Clave (Varint)
                    byte[] leftChildPtrBytes = new byte[4];
                    Helpers.ReadExactly(dbStream, leftChildPtrBytes, 0, 4);
                    // uint leftChildPage = Helpers.ReadUInt32BigEndian(leftChildPtrBytes);
                    var (key, keyBytes) = Helpers.ReadVarint(dbStream);
                    // Console.WriteLine($"      Interior Cell - Left Child: {leftChildPage}, Key: {key}");
                    // Saltar al siguiente puntero, no hay datos de registro aquí.
                    continue;
                }
                
                var data = ReadRecord(dbStream);
                if (rowIdValue != -1)
                {
                    data[0] = rowIdValue;
                }
                // Here we can filter the data based on the filters
                if (filters != null && filters.Count > 0)
                {
                    var addColumn = true;
                    foreach (var filter in filters)
                    {
                        var columnIndex = filter.Key;
                        var columnValue = filter.Value;
                        string dataValue = data[columnIndex].ToString() ?? string.Empty;
                        if (columnIndex < 0 || columnIndex >= data.Count || !dataValue.Contains(columnValue))
                        {
                            addColumn = false;
                        }
                    }
                    if (!addColumn)
                    {
                        continue;
                    }
                }
                results.Add(data);
                
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al leer la celda {i}: {ex.Message}");
            }
        }
        return results;
    }

    private List<object> ReadRecord(Stream dbStream)
    {
        var (totalHeaderSize, headerSizeBytesRead) = Helpers.ReadVarint(dbStream);
        var bytesReadForTypeCodes = 0L;
        var totalBytesReadInHeader = headerSizeBytesRead;
        var serialTypeCodes = new List<long>();
        while(totalBytesReadInHeader < totalHeaderSize)
        {
            var (typeSerialCode, typeCodeBytes) = Helpers.ReadVarint(dbStream);
            serialTypeCodes.Add(typeSerialCode);
            bytesReadForTypeCodes += typeCodeBytes;
            totalBytesReadInHeader += typeCodeBytes;

            if (totalBytesReadInHeader > totalHeaderSize)
            {
                throw new InvalidDataException($"Se leyeron más butes ({totalBytesReadInHeader}]) para la cabecera de los indicados ({totalHeaderSize}");
            }
            
        }
        
        var recordData = new List<object>();
        foreach (var serialTypeCode in serialTypeCodes)
        {
            var typeValue = ReadColumnData(dbStream, serialTypeCode);
            recordData.Add(typeValue);
        }
        return recordData;
    }

    private object ReadColumnData(Stream dbStream, long serialTypeCode)
    {
        switch(serialTypeCode)  
        {
            case 0:
                return new object();
            case 1:
                return (sbyte)dbStream.ReadByte();
            case 2:
            {
                var buffer = new byte[2];
                Helpers.ReadExactly(dbStream, buffer, 0, 2);
                return Helpers.ReadInt16BigEndian(buffer);
            }
            case 3:
            {
                var buffer = new byte[3];
                Helpers.ReadExactly(dbStream, buffer, 0, 3);
                return Helpers.ReadInt24BigEndian(buffer);
            }
            case 4:
            {
                var buffer = new byte[4];
                Helpers.ReadExactly(dbStream, buffer, 0, 4);
                return Helpers.ReadInt32BigEndian(buffer);
            }
            case 5:
            {
                var buffer = new byte[6];
                Helpers.ReadExactly(dbStream, buffer, 0, 6);
                return Helpers.ReadInt48BigEndian(buffer);
            }
            case 6:
            {
                var buffer = new byte[8];
                Helpers.ReadExactly(dbStream, buffer, 0, 8);
                return Helpers.ReadInt64BigEndian(buffer);
            }
            case 7:
            {
                var buffer = new byte[8];
                Helpers.ReadExactly(dbStream, buffer, 0, 8);
                if (BitConverter.IsLittleEndian) Array.Reverse(buffer);
                return BitConverter.ToDouble(buffer, 0);
            }
            case 8:
                return 0L;
            case 9:
                return 1L;
            default:
                if (serialTypeCode < 12)
                {
                    Console.WriteLine($"Not supported type {serialTypeCode}");
                    return new object();
                }

                long dataSize;
                bool isText;
                if (serialTypeCode % 2 == 0)
                {
                    dataSize = (serialTypeCode - 12) / 2;
                    isText = false;
                }
                else
                {
                    dataSize = (serialTypeCode - 13) / 2;
                    isText = true;
                }

                if (dataSize == 0)
                {
                    return isText ? (object)"" : (object)Array.Empty<byte>();
                }
                
                byte[] dataBuffer = new byte[dataSize];
                Helpers.ReadExactly(dbStream, dataBuffer, 0, dataBuffer.Length);

                if (isText)
                {
                    return DbEncoding.GetString(dataBuffer);
                }
                else
                {
                    return dataBuffer;
                }
        }
    }
    
    public List<List<object>> GetFieldValues(string[] fieldNames, Stream dbStream, Dictionary<string, string> filters)
    {
        var selectedFieldValues = new List<List<object>>();
        var filtersByIndexColumn = filters.Select(f => new { ColumnName = f.Key, ColumnValue = f.Value })
            .ToDictionary(f => Table.GetColumnIndex(f.ColumnName), f => f.ColumnValue);
        
        var rows = ReadCells(dbStream, filtersByIndexColumn);
        foreach (var row in rows)
        {
            if (row.Count == 0) continue;
            var fieldIndexes = fieldNames.Select(fieldName => Table.GetColumnIndex(fieldName)).ToList();
            if (fieldIndexes.Any(fieldIndex => (fieldIndex < 0 || fieldIndex >= row.Count))) continue;
            var selectedRow = new List<object>();
            fieldIndexes.ForEach(fieldIndex =>
            {
                var fieldValue = row[fieldIndex];
                selectedRow.Add(fieldValue);
            });
            selectedFieldValues.Add(selectedRow);
        }
        return selectedFieldValues;
    }
}