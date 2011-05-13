#ifndef DBMS_COMMON_COMPRESSED_WRITEBUFFER_H
#define DBMS_COMMON_COMPRESSED_WRITEBUFFER_H

#include <quicklz/quicklz_level1.h>

#include <DB/IO/WriteBuffer.h>
#include <DB/IO/CompressedStream.h>


namespace DB
{

class CompressedWriteBuffer : public WriteBuffer
{
private:
	WriteBuffer & out;

	char compressed_buffer[DBMS_COMPRESSING_STREAM_BUFFER_SIZE + QUICKLZ_ADDITIONAL_SPACE];
	char scratch[QLZ_SCRATCH_COMPRESS];

	size_t compressed_bytes;

public:
	CompressedWriteBuffer(WriteBuffer & out_) : out(out_), compressed_bytes(0) {}

	void next()
	{
		size_t compressed_size = qlz_compress(
			working_buffer.begin(),
			compressed_buffer,
			pos - working_buffer.begin(),
			scratch);

		out.write(compressed_buffer, compressed_size);
		pos = working_buffer.begin();
		compressed_bytes += compressed_size;
	}

	/// Объём данных, которые были сжаты
	size_t getCompressedBytes()
	{
		nextIfAtEnd();
		return compressed_bytes;
	}

	/// Сколько несжатых байт было записано в буфер
	size_t getUncompressedBytes()
	{
		return count();
	}

	/// Сколько байт находится в буфере (ещё не сжато)
	size_t getRemainingBytes()
	{
		nextIfAtEnd();
		return pos - working_buffer.begin();
	}

	~CompressedWriteBuffer()
	{
		next();
	}
};

}

#endif
