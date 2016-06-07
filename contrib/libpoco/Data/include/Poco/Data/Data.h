//
// Data.h
//
// $Id: //poco/Main/Data/include/Poco/Data/Data.h#4 $
//
// Library: Data
// Package: DataCore
// Module:  Data
//
// Basic definitions for the Poco Data library.
// This file must be the first file included by every other Data
// header file.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Data_Data_INCLUDED
#define Data_Data_INCLUDED


#include "Poco/Foundation.h"


//
// The following block is the standard way of creating macros which make exporting
// from a DLL simpler. All files within this DLL are compiled with the Data_EXPORTS
// symbol defined on the command line. this symbol should not be defined on any project
// that uses this DLL. This way any other project whose source files include this file see
// Data_API functions as being imported from a DLL, wheras this DLL sees symbols
// defined with this macro as being exported.
//
#if defined(_WIN32) && defined(POCO_DLL)
	#if defined(Data_EXPORTS)
		#define Data_API __declspec(dllexport)
	#else
		#define Data_API __declspec(dllimport)
	#endif
#endif


#if !defined(Data_API)
	#if !defined(POCO_NO_GCC_API_ATTRIBUTE) && defined (__GNUC__) && (__GNUC__ >= 4)
		#define Data_API __attribute__ ((visibility ("default")))
	#else
		#define Data_API
	#endif
#endif


//
// Automatically link Data library.
//
#if defined(_MSC_VER)
	#if !defined(POCO_NO_AUTOMATIC_LIBS) && !defined(Data_EXPORTS)
		#pragma comment(lib, "PocoData" POCO_LIB_SUFFIX)
	#endif
#endif


#endif // Data_Data_INCLUDED
