#pragma once

#include <Poco/SharedPtr.h>

#include <DB/Core/Block.h>
#include <DB/IO/WriteBuffer.h>
#include <DB/IO/WriteBufferValidUTF8.h>
#include <DB/DataStreams/JSONRowOutputStream.h>


namespace DB
{

/** Поток для вывода данных в формате JSON.
  */
class JSONCompactRowOutputStream : public JSONRowOutputStream
{
public:
	JSONCompactRowOutputStream(WriteBuffer & ostr_, const Block & sample_);

	void writeField(const Field & field);
	void writeFieldDelimiter();
	void writeRowStartDelimiter();
	void writeRowEndDelimiter();
};

}
