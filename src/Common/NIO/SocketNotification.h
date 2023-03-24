/**
* Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH. and Contributors.
* SPDX-License-Identifier:	BSL-1.0
*
*/
#pragma once

#include "Poco/Net/Net.h"
#include "Poco/Net/Socket.h"
#include "Poco/Notification.h"

using Poco::Net::Socket;

namespace RK {

class SocketReactor;

class SocketNotification: public Poco::Notification
/// The base class for all notifications generated by
/// the SocketReactor.
{
public:
    explicit SocketNotification(SocketReactor* pReactor);
    /// Creates the SocketNotification for the given SocketReactor.

    virtual ~SocketNotification() override;
    /// Destroys the SocketNotification.

    SocketReactor& source() const;
    /// Returns the SocketReactor that generated the notification.

    Socket socket() const;
    /// Returns the socket that caused the notification.

private:
    void setSocket(const Socket& socket);

    SocketReactor* _pReactor;
    Socket         _socket;

    friend class SocketNotifier;
};


class ReadableNotification: public SocketNotification
/// This notification is sent if a socket has become readable.
{
public:
    ReadableNotification(SocketReactor* pReactor);
    /// Creates the ReadableNotification for the given SocketReactor.

    ~ReadableNotification() override;
    /// Destroys the ReadableNotification.
};


class WritableNotification: public SocketNotification
/// This notification is sent if a socket has become writable.
{
public:
    WritableNotification(SocketReactor* pReactor);
    /// Creates the WritableNotification for the given SocketReactor.

    ~WritableNotification() override;
    /// Destroys the WritableNotification.
};


class ErrorNotification: public SocketNotification
/// This notification is sent if a socket has signalled an error.
{
public:
    ErrorNotification(SocketReactor* pReactor);
    /// Creates the ErrorNotification for the given SocketReactor.

    ~ErrorNotification() override;
    /// Destroys the ErrorNotification.
};


class TimeoutNotification: public SocketNotification
/// This notification is sent if no other event has occurred
/// for a specified time.
{
public:
    TimeoutNotification(SocketReactor* pReactor);
    /// Creates the TimeoutNotification for the given SocketReactor.

    ~TimeoutNotification() override;
    /// Destroys the TimeoutNotification.
};


class IdleNotification: public SocketNotification
/// This notification is sent when the SocketReactor does
/// not have any sockets to react to.
{
public:
    IdleNotification(SocketReactor* pReactor);
    /// Creates the IdleNotification for the given SocketReactor.

    ~IdleNotification() override;
    /// Destroys the IdleNotification.
};


class ShutdownNotification: public SocketNotification
/// This notification is sent when the SocketReactor is
/// about to shut down.
{
public:
    ShutdownNotification(SocketReactor* pReactor);
    /// Creates the ShutdownNotification for the given SocketReactor.

    ~ShutdownNotification() override;
    /// Destroys the ShutdownNotification.
};


//
// inlines
//
inline SocketReactor& SocketNotification::source() const
{
    return *_pReactor;
}


inline Socket SocketNotification::socket() const
{
    return _socket;
}


}
