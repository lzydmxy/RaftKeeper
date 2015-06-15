#include <math.h>

#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/DataStreams/IBlockInputStream.h>


namespace DB
{


String IBlockInputStream::getTreeID() const
{
	std::stringstream s;
	s << getName();

	if (!children.empty())
	{
		s << "(";
		for (BlockInputStreams::const_iterator it = children.begin(); it != children.end(); ++it)
		{
			if (it != children.begin())
				s << ", ";
			s << (*it)->getTreeID();
		}
		s << ")";
	}

	return s.str();
}


size_t IBlockInputStream::checkDepth(size_t max_depth) const
{
	return checkDepthImpl(max_depth, max_depth);
}

size_t IBlockInputStream::checkDepthImpl(size_t max_depth, size_t level) const
{
	if (children.empty())
		return 0;

	if (level > max_depth)
		throw Exception("Query pipeline is too deep. Maximum: " + toString(max_depth), ErrorCodes::TOO_DEEP_PIPELINE);

	size_t res = 0;
	for (BlockInputStreams::const_iterator it = children.begin(); it != children.end(); ++it)
	{
		size_t child_depth = (*it)->checkDepth(level + 1);
		if (child_depth > res)
			res = child_depth;
	}

	return res + 1;
}

	
void IBlockInputStream::dumpTree(std::ostream & ostr, size_t indent, size_t multiplier)
{
	/// Не будем отображать в дереве обёртку потока блоков в AsynchronousBlockInputStream.
	if (getName() != "Asynchronous")
	{
		ostr << String(indent, ' ') << getName();
		if (multiplier > 1)
			ostr << " × " << multiplier;
		ostr << std::endl;
		++indent;

		/// Если поддерево повторяется несколько раз, то будем выводить его один раз с множителем.
		typedef std::map<String, size_t> Multipliers;
		Multipliers multipliers;

		for (BlockInputStreams::const_iterator it = children.begin(); it != children.end(); ++it)
			++multipliers[(*it)->getTreeID()];

		for (BlockInputStreams::iterator it = children.begin(); it != children.end(); ++it)
		{
			String id = (*it)->getTreeID();
			size_t & subtree_multiplier = multipliers[id];
			if (subtree_multiplier != 0)	/// Уже выведенные поддеревья помечаем нулём в массиве множителей.
			{
				(*it)->dumpTree(ostr, indent, subtree_multiplier);
				subtree_multiplier = 0;
			}
		}
	}
	else
	{
		for (BlockInputStreams::iterator it = children.begin(); it != children.end(); ++it)
			(*it)->dumpTree(ostr, indent, multiplier);
	}
}


BlockInputStreams IBlockInputStream::getLeaves()
{
	BlockInputStreams res;
	getLeavesImpl(res);
	return res;
}


void IBlockInputStream::getLeafRowsBytes(size_t & rows, size_t & bytes)
{
	BlockInputStreams leaves = getLeaves();
	rows = 0;
	bytes = 0;

	for (BlockInputStreams::const_iterator it = leaves.begin(); it != leaves.end(); ++it)
	{
		if (const IProfilingBlockInputStream * profiling = dynamic_cast<const IProfilingBlockInputStream *>(&**it))
		{
			const BlockStreamProfileInfo & info = profiling->getInfo();
			rows += info.rows;
			bytes += info.bytes;
		}
	}
}


void IBlockInputStream::getLeavesImpl(BlockInputStreams & res, BlockInputStreamPtr this_shared_ptr)
{
	if (children.empty())
	{
		if (this_shared_ptr)
			res.push_back(this_shared_ptr);
	}
	else
		for (BlockInputStreams::iterator it = children.begin(); it != children.end(); ++it)
			(*it)->getLeavesImpl(res, *it);
}


}

