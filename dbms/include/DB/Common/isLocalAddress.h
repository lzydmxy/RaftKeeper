#pragma once


namespace Poco
{
	namespace Net
	{
		class SocketAddress;
	}
}

namespace DB
{

	/** Позволяет проверить, похож ли адрес на localhost.
	 * Цель этой проверки обычно состоит в том, чтобы сделать предположение,
	 *  что при хождении на этот адрес через интернет, мы попадём на себя.
	 * Следует иметь ввиду, что эта проверка делается неточно:
	 * - адрес просто сравнивается с адресами сетевых интерфейсов;
	 * - для каждого сетевого интерфейса берётся только первый адрес;
	 * - не проверяются правила маршрутизации, которые влияют, через какой сетевой интерфейс мы пойдём на заданный адрес.
	 */
	bool isLocalAddress(const Poco::Net::SocketAddress & address);

}
