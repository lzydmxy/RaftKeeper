//
// Path_WIN32U.h
//
// $Id: //poco/1.4/Foundation/include/Poco/Path_WIN32U.h#1 $
//
// Library: Foundation
// Package: Filesystem
// Module:  Path
//
// Definition of the PathImpl class for WIN32.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_Path_WIN32U_INCLUDED
#define Foundation_Path_WIN32U_INCLUDED


#include "Poco/Foundation.h"
#include <vector>


namespace Poco {


class Foundation_API PathImpl
{
public:
	static std::string currentImpl();
	static std::string homeImpl();
	static std::string tempImpl();
	static std::string nullImpl();
	static std::string systemImpl();
	static std::string expandImpl(const std::string& path);
	static void listRootsImpl(std::vector<std::string>& roots);
	
	enum
	{
		MAX_PATH_LEN = 32767
	};
};


} // namespace Poco


#endif // Foundation_Path_WIN32U_INCLUDED
