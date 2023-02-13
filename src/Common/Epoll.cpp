/**
 * Copyright 2016-2023 ClickHouse, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#if defined(OS_LINUX)

#include "Epoll.h"
#include <Common/Exception.h>
#include <unistd.h>
#include <common/logger_useful.h>

namespace RK
{

namespace ErrorCodes
{
    extern const int EPOLL_ERROR;
    extern const int LOGICAL_ERROR;
}

Epoll::Epoll() : events_count(0)
{
    epoll_fd = epoll_create1(0);
    if (epoll_fd == -1)
        throwFromErrno("Cannot open epoll descriptor", RK::ErrorCodes::EPOLL_ERROR);
}

Epoll::Epoll(Epoll && other) : epoll_fd(other.epoll_fd), events_count(other.events_count.load())
{
    other.epoll_fd = -1;
}

Epoll & Epoll::operator=(Epoll && other)
{
    epoll_fd = other.epoll_fd;
    other.epoll_fd = -1;
    events_count.store(other.events_count.load());
    return *this;
}

void Epoll::add(int fd, void * ptr)
{
    epoll_event event;
    event.events = EPOLLIN | EPOLLPRI;
    if (ptr)
        event.data.ptr = ptr;
    else
        event.data.fd = fd;

    ++events_count;

    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &event) == -1)
        throwFromErrno("Cannot add new descriptor to epoll", RK::ErrorCodes::EPOLL_ERROR);
}

void Epoll::remove(int fd)
{
    --events_count;

    if (epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr) == -1)
        throwFromErrno("Cannot remove descriptor from epoll", RK::ErrorCodes::EPOLL_ERROR);
}

size_t Epoll::getManyReady(int max_events, epoll_event * events_out, bool blocking) const
{
    if (events_count == 0)
        throw Exception("There is no events in epoll", ErrorCodes::LOGICAL_ERROR);

    int ready_size;
    int timeout = blocking ? -1 : 0;
    do
    {
        ready_size = epoll_wait(epoll_fd, events_out, max_events, timeout);

        if (ready_size == -1 && errno != EINTR)
            throwFromErrno("Error in epoll_wait", RK::ErrorCodes::EPOLL_ERROR);
    }
    while (ready_size <= 0 && (ready_size != 0 || blocking));

    return ready_size;
}

Epoll::~Epoll()
{
    if (epoll_fd != -1)
        close(epoll_fd);
}

}
#endif
