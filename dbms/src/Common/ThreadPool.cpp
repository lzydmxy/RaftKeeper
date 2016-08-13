#include <DB/Common/ThreadPool.h>


ThreadPool::ThreadPool(size_t m_size)
	: m_size(m_size)
{
	threads.reserve(m_size);
	for (size_t i = 0; i < m_size; ++i)
		threads.emplace_back([this] { worker(); });
}

void ThreadPool::schedule(Job job)
{
	{
		std::unique_lock<std::mutex> lock(mutex);
		has_free_thread.wait(lock, [this] { return active_jobs < m_size || shutdown; });
		if (shutdown)
			return;

		jobs.push(std::move(job));
		++active_jobs;
	}
	has_new_job_or_shutdown.notify_one();
}

void ThreadPool::wait()
{
	{
		std::unique_lock<std::mutex> lock(mutex);
		has_free_thread.wait(lock, [this] { return active_jobs == 0 || shutdown; });

		if (!exceptions.empty())
			std::rethrow_exception(exceptions.front());
	}
}

ThreadPool::~ThreadPool()
{
	{
		std::unique_lock<std::mutex> lock(mutex);
		shutdown = true;
	}

	has_new_job_or_shutdown.notify_all();

	for (auto & thread : threads)
		thread.join();
}

size_t ThreadPool::active() const
{
	std::unique_lock<std::mutex> lock(mutex);
	return active_jobs;
}


void ThreadPool::worker()
{
	while (true)
	{
		Job job;

		{
			std::unique_lock<std::mutex> lock(mutex);
			has_new_job_or_shutdown.wait(lock, [this] { return shutdown || !jobs.empty(); });

			if (!shutdown)
			{
				job = std::move(jobs.front());
				jobs.pop();
			}
			else
			{
				return;
			}
		}

		try
		{
			job();
		}
		catch (...)
		{
			{
				std::unique_lock<std::mutex> lock(mutex);
				exceptions.push_back(std::current_exception());
				shutdown = true;
				--active_jobs;
			}
			has_free_thread.notify_one();
			has_new_job_or_shutdown.notify_all();
			return;
		}

		{
			std::unique_lock<std::mutex> lock(mutex);
			--active_jobs;
		}

		has_free_thread.notify_one();
	}
}

