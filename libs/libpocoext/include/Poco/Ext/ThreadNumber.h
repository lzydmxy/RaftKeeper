#pragma once

/** Последовательный номер потока, начиная с 1, среди тех потоков, для которых был получен этот номер.
  * Используется при логгировании.
  */
namespace Poco
{

namespace ThreadNumber
{
	unsigned get();
}

}
