using System.Buffers.Binary;
using System.Text;

namespace codecrafters_sqlite;

public static class Helpers
{
    public static long CalculateDataSize(long serialTypeCode)
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
    
    public static Encoding GetDbEncoding(Stream dbStream)
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
    
    public static (long value, int bytesRead) ReadVarint(Stream stream)
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
    
    public static void ReadExactly(Stream stream, byte[] buffer, int offset, int count)
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
    
    public static ushort ReadUInt16BigEndian(byte[] buffer, int offset = 0)
    {
        return BinaryPrimitives.ReadUInt16BigEndian(buffer.AsSpan(offset));
    }
    
    public static uint ReadUInt32BigEndian(byte[] buffer, int offset = 0)
    {
        return BinaryPrimitives.ReadUInt32BigEndian(buffer.AsSpan(offset));
    }
}