#pragma once
#include <Coordination/TestKeeperStorage.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadBuffer.h>

namespace DB
{

class TestKeeperStorageSerializer
{
public:
    void serialize(const TestKeeperStorage & storage, WriteBuffer & out) const;

    void deserialize(TestKeeperStorage & storage, ReadBuffer & in) const;
};

}
