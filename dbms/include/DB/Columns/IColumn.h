#pragma once

#include <Poco/SharedPtr.h>

#include <DB/Core/Field.h>


namespace DB
{

using Poco::SharedPtr;

class IColumn;
typedef SharedPtr<IColumn> ColumnPtr;
typedef std::vector<ColumnPtr> Columns;


/** Интерфейс для хранения столбцов значений в оперативке.
  */
class IColumn
{
public:
	/** Столбец представляет собой вектор чисел или числовую константу. 
	  */
	virtual bool isNumeric() const { return false; }
	
	/** Создать пустой столбец такого же типа */
	virtual SharedPtr<IColumn> cloneEmpty() const = 0;

	/** Количество значений в столбце. */
	virtual size_t size() const = 0;

	/** Получить значение n-го элемента.
	  * Используется для преобразования из блоков в строки (например, при выводе значений в текстовый дамп)
	  */
	virtual Field operator[](size_t n) const = 0;

	/** Удалить всё кроме диапазона элементов.
	  * Используется, например, для операции LIMIT.
	  */
	virtual void cut(size_t start, size_t length) = 0;

	/** Вставить значение в конец столбца (количество значений увеличится на 1).
	  * Используется для преобразования из строк в блоки (например, при чтении значений из текстового дампа)
	  */
	virtual void insert(const Field & x) = 0;

	/** Вставить значение "по умолчанию".
	  * Используется, когда нужно увеличить размер столбца, но значение не имеет смысла.
	  * Например, для ColumnNullable, если взведён флаг null, то соответствующее значение во вложенном столбце игнорируется.
	  */
	virtual void insertDefault() = 0;

	/** Соединить столбец с одним или несколькими другими.
	  * Используется при склейке маленьких блоков.
	  */
	//virtual void merge(const Columns & columns) = 0;

	/** Оставить только значения, соответствующие фильтру.
	  * Используется для операции WHERE / HAVING.
	  */
	typedef std::vector<UInt8> Filter;
	virtual void filter(const Filter & filt) = 0;

	/** Очистить */
	virtual void clear() = 0;

	virtual ~IColumn() {}
};


}
