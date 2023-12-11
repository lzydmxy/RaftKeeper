/**
* Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH. and Contributors.
* SPDX-License-Identifier:	BSL-1.0
*
*/
#pragma once

#include <Poco/Net/Net.h>
#include <Poco/Net/Socket.h>

#include <Common/NIO/Notification.h>

using Poco::Net::Socket;

namespace RK
{

class SocketReactor;

/// The base class for all notifications generated by
/// the SocketReactor.
class SocketNotification : public Notification
{
public:
    explicit SocketNotification(SocketReactor * reactor_) : reactor(reactor_) { }
    virtual ~SocketNotification() override = default;

    SocketReactor & source() const;
    Socket socket() const;

private:
    void setSocket(const Socket & target);

    SocketReactor * reactor;
    Socket socket_;

    friend class SocketNotifier;
};

/// This notification is sent if a socket has become readable.
class ReadableNotification : public SocketNotification
{
public:
    explicit ReadableNotification(SocketReactor * reactor_) : SocketNotification(reactor_) { }
    ~ReadableNotification() override = default;
};

/// This notification is sent if a socket has become writable.
class WritableNotification : public SocketNotification
{
public:
    explicit WritableNotification(SocketReactor * reactor_) : SocketNotification(reactor_) { }
    ~WritableNotification() override = default;
};

/// This notification is sent if a socket has signalled an error.
class ErrorNotification : public SocketNotification
{
public:
    explicit ErrorNotification(SocketReactor * reactor_) : SocketNotification(reactor_) { }
    ~ErrorNotification() override = default;
};

/// This notification is sent if no other event has occurred
/// for a specified time.
class TimeoutNotification : public SocketNotification
{
public:
    explicit TimeoutNotification(SocketReactor * reactor_) : SocketNotification(reactor_) { }
    ~TimeoutNotification() override = default;
};

/// This notification is sent when the SocketReactor does
/// not have any sockets to react to.
class IdleNotification : public SocketNotification
{
public:
    explicit IdleNotification(SocketReactor * reactor_) : SocketNotification(reactor_) { }
    ~IdleNotification() override = default;
};

/// This notification is sent when the SocketReactor is
/// about to shut down.
class ShutdownNotification : public SocketNotification
{
public:
    explicit ShutdownNotification(SocketReactor * reactor_) : SocketNotification(reactor_) { }
    ~ShutdownNotification() override = default;
};


inline SocketReactor & SocketNotification::source() const
{
    return *reactor;
}


inline Socket SocketNotification::socket() const
{
    return socket_;
}

inline void SocketNotification::setSocket(const Socket & target)
{
    socket_ = target;
}


}
