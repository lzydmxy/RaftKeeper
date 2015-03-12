#include <DB/IO/ReadBufferAIO.h>
#include <boost/filesystem.hpp>

#include <vector>
#include <iostream>
#include <fstream>
#include <functional>
#include <cstdlib>
#include <unistd.h>

namespace
{

void run();
void prepare(std::string & directory, std::string  & filename, std::string & buf);
void die(const std::string & msg);
void run_test(unsigned int num, const std::function<bool()> func);

bool test1(const std::string & filename);
bool test2(const std::string & filename, const std::string & buf);
bool test3(const std::string & filename, const std::string & buf);
bool test4(const std::string & filename, const std::string & buf);
bool test5(const std::string & filename);
bool test6(const std::string & filename, const std::string & buf);
bool test7(const std::string & filename, const std::string & buf);
bool test8(const std::string & filename);
bool test9(const std::string & filename, const std::string & buf);
bool test10(const std::string & filename, const std::string & buf);

void run()
{
	namespace fs = boost::filesystem;

	std::string directory;
	std::string filename;
	std::string buf;
	prepare(directory, filename, buf);

	const std::vector<std::function<bool()> > tests =
	{
		std::bind(test1, std::ref(filename)),
		std::bind(test2, std::ref(filename), std::ref(buf)),
		std::bind(test3, std::ref(filename), std::ref(buf)),
		std::bind(test4, std::ref(filename), std::ref(buf)),
		std::bind(test5, std::ref(filename)),
		std::bind(test6, std::ref(filename), std::ref(buf)),
		std::bind(test7, std::ref(filename), std::ref(buf)),
		std::bind(test8, std::ref(filename)),
		std::bind(test9, std::ref(filename), std::ref(buf)),
		std::bind(test10, std::ref(filename), std::ref(buf))
	};

	unsigned int num = 0;
	for (const auto & test : tests)
	{
		++num;
		run_test(num, test);
	}

	fs::remove_all(directory);
}

void prepare(std::string & directory, std::string  & filename, std::string & buf)
{
	static const std::string symbols = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

	char pattern[] = "/tmp/fileXXXXXX";
	char * dir = ::mkdtemp(pattern);
	if (dir == nullptr)
		die("Could not create directory");

	directory = std::string(dir);
	filename = directory + "/foo";

	size_t n = 10 * DB::ReadBufferAIO::BLOCK_SIZE;
	buf.reserve(n);

	for (size_t i = 0; i < n; ++i)
		buf += symbols[i % symbols.length()];

	std::ofstream out(filename.c_str());
	if (!out.is_open())
		die("Could not open file");

	out << buf;
}

void die(const std::string & msg)
{
	std::cout << msg << "\n";
	::exit(EXIT_FAILURE);
}

void run_test(unsigned int num, const std::function<bool()> func)
{
	bool ok;

	try
	{
		ok = func();
	}
	catch (const DB::Exception & ex)
	{
		ok = false;
		std::cout << "Caught exception " << ex.displayText() << "\n";
	}
	catch (const std::exception & ex)
	{
		ok = false;
		std::cout << "Caught exception " << ex.what() << "\n";
	}

	if (ok)
		std::cout << "Test " << num << " passed\n";
	else
		std::cout << "Test " << num << " failed\n";
}

bool test1(const std::string & filename)
{
	DB::ReadBufferAIO in(filename, 3 * DB::ReadBufferAIO::BLOCK_SIZE);
	if (in.getFileName() != filename)
		return false;
	if (in.getFD() == -1)
		return false;
	return true;
}

bool test2(const std::string & filename, const std::string & buf)
{
	std::string newbuf;
	newbuf.resize(buf.length());

	DB::ReadBufferAIO in(filename, 3 * DB::ReadBufferAIO::BLOCK_SIZE);
	size_t count = in.read(&newbuf[0], newbuf.length());
	if (count != newbuf.length())
		return false;

	return (newbuf == buf);
}

bool test3(const std::string & filename, const std::string & buf)
{
	std::string newbuf;
	newbuf.resize(buf.length());

	size_t requested = 9 * DB::ReadBufferAIO::BLOCK_SIZE;

	DB::ReadBufferAIO in(filename, 3 * DB::ReadBufferAIO::BLOCK_SIZE);
	in.setMaxBytes(requested);
	size_t count = in.read(&newbuf[0], newbuf.length());

	newbuf.resize(count);
	return (newbuf == buf.substr(0, requested));
}

bool test4(const std::string & filename, const std::string & buf)
{
	std::string newbuf;
	newbuf.resize(buf.length());

	DB::ReadBufferAIO in(filename, 3 * DB::ReadBufferAIO::BLOCK_SIZE);
	in.setMaxBytes(0);
	size_t n_read = in.read(&newbuf[0], newbuf.length());

	return n_read == 0;
}

bool test5(const std::string & filename)
{
	bool ok = false;

	try
	{
		DB::ReadBufferAIO in(filename, 3 * DB::ReadBufferAIO::BLOCK_SIZE);
		in.setMaxBytes(DB::ReadBufferAIO::BLOCK_SIZE >> 1);
	}
	catch (const DB::Exception &)
	{
		ok = true;
	}
	return ok;
}

bool test6(const std::string & filename, const std::string & buf)
{
	std::string newbuf;
	newbuf.resize(buf.length());

	DB::ReadBufferAIO in(filename, 3 * DB::ReadBufferAIO::BLOCK_SIZE);

	if (in.getPositionInFile() != 0)
		return false;

	size_t count = in.read(&newbuf[0], newbuf.length());
	if (count != newbuf.length())
		return false;

	if (static_cast<size_t>(in.getPositionInFile()) != buf.length())
		return false;

	return true;
}

bool test7(const std::string & filename, const std::string & buf)
{
	std::string newbuf;
	newbuf.resize(buf.length() - DB::ReadBufferAIO::BLOCK_SIZE);

	DB::ReadBufferAIO in(filename, 3 * DB::ReadBufferAIO::BLOCK_SIZE);
	(void) in.seek(DB::ReadBufferAIO::BLOCK_SIZE, SEEK_SET);
	size_t count = in.read(&newbuf[0], newbuf.length());
	if (count != (9 * DB::ReadBufferAIO::BLOCK_SIZE))
		return false;

	return (newbuf == buf.substr(DB::ReadBufferAIO::BLOCK_SIZE));
}

bool test8(const std::string & filename)
{
	bool ok = false;

	try
	{
		DB::ReadBufferAIO in(filename, 3 * DB::ReadBufferAIO::BLOCK_SIZE);
		(void) in.seek(DB::ReadBufferAIO::BLOCK_SIZE + 1, SEEK_CUR);
	}
	catch (const DB::Exception &)
	{
		ok = true;
	}

	return ok;
}

bool test9(const std::string & filename, const std::string & buf)
{
	bool ok = false;

	try
	{
		std::string newbuf;
		newbuf.resize(buf.length());

		DB::ReadBufferAIO in(filename, 3 * DB::ReadBufferAIO::BLOCK_SIZE);
		size_t count =  in.read(&newbuf[0], newbuf.length());
		if (count != newbuf.length())
			return false;
		in.setMaxBytes(9 * DB::ReadBufferAIO::BLOCK_SIZE);
	}
	catch (const DB::Exception &)
	{
		ok = true;
	}

	return ok;
}

bool test10(const std::string & filename, const std::string & buf)
{
	std::string newbuf;
	newbuf.resize(4 * DB::ReadBufferAIO::BLOCK_SIZE);

	DB::ReadBufferAIO in(filename, 3 * DB::ReadBufferAIO::BLOCK_SIZE);
	size_t count1 = in.read(&newbuf[0], newbuf.length());
	if (count1 != newbuf.length())
		return false;

	if (newbuf != buf.substr(0, 4 * DB::ReadBufferAIO::BLOCK_SIZE))
		return false;

	(void) in.seek(2 * DB::ReadBufferAIO::BLOCK_SIZE, SEEK_CUR);

	size_t count2 = in.read(&newbuf[0], newbuf.length());
	if (count2 != newbuf.length())
		return false;

	if (newbuf != buf.substr(6 * DB::ReadBufferAIO::BLOCK_SIZE))
		return false;

	return true;
}

}

int main()
{
	run();
	return 0;
}
