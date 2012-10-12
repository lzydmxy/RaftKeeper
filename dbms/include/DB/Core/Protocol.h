#pragma once

namespace DB
{

/** Протокол взаимодействия с сервером.
  *
  * Клиент соединяется с сервером и передаёт ему пакет Hello.
  * Если версия не устраивает, то сервер может разорвать соединение.
  * Сервер отвечает пакетом Hello.
  * Если версия не устраивает, то клиент может разорвать соединение.
  *
  * Далее в цикле.
  *
  * 1. Клиент отправляет на сервер пакет Query.
  *
  * Если запрос типа INSERT (требует передачи данных от клиента), то сервер передаёт
  *  пакет Data, содержащий пустой блок, который описывает структуру таблицы.
  * Затем клиент отправляет данные для вставки
  * - один или несколько пакетов Data.
  * Конец данных определается по отправленному пустому блоку.
  * Затем сервер отправляет клиенту пакет EndOfStream.
  * 
  * Если запрос типа SELECT или другой, то сервер передаёт набор пакетов одного из следующих видов:
  * - Data - данные результата выполнения запроса (один блок);
  * - Progress - прогресс выполнения запроса;
  * - Exception - ошибка;
  * - EndOfStream - конец передачи данных;
  *
  * Клиент должен читать пакеты до EndOfStream или Exception.
  * Также, клиент может передать на сервер пакет Cancel - отмена выполнения запроса.
  * В этом случае, сервер может прервать выполнение запроса и вернуть неполные данные;
  *  но клиент всё равно должен читать все пакеты до EndOfStream.
  *
  * 2. Между запросами, клиент может отправить Ping, и сервер должен ответить Pong.
  */

namespace Protocol
{
	/// То, что передаёт сервер.
	namespace Server
	{
		enum Enum
		{
			Hello = 0,			/// Имя, версия, ревизия.
			Data = 1,			/// Блок данных со сжатием или без.
			Exception = 2,		/// Исключение во время обработки запроса.
			Progress = 3,		/// Прогресс выполнения запроса: строк считано, байт считано.
			Pong = 4,			/// Ответ на Ping.
			EndOfStream = 5,	/// Все пакеты были переданы.
		};

		inline const char * toString(Enum packet)
		{
			static const char * data[] = { "Hello", "Data", "Exception", "Progress", "Pong", "EndOfStream" };
			return packet >= 0 && packet < 6
				? data[packet]
				: "Unknown packet";
		}
	}

	/// То, что передаёт клиент.
	namespace Client
	{
		enum Enum
		{
			Hello = 0,			/// Имя, версия, ревизия, БД по-умолчанию.
			Query = 1,			/** Идентификатор запроса, информация, до какой стадии исполнять запрос,
								  * использовать ли сжатие, текст запроса (без данных для INSERT-а).
								  */
			Data = 2,			/// Блок данных со сжатием или без.
			Cancel = 3,			/// Отменить выполнение запроса.
			Ping = 4,			/// Проверка живости соединения с сервером.
		};

		inline const char * toString(Enum packet)
		{
			static const char * data[] = { "Hello", "Query", "Data", "Cancel", "Ping" };
			return packet >= 0 && packet < 5
				? data[packet]
				: "Unknown packet";
		}
	}

	/// Использовать ли сжатие.
	namespace Compression
	{
		enum Enum
		{
			Disable = 0,
			Enable = 1,
		};
	}
}

}
