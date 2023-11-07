#pragma once

#include <Interpreters/BlobStorageLog.h>

#include "config.h"

#if USE_AWS_S3

namespace Aws::S3
{
    class S3Error;
}

namespace DB
{

using BlobStorageLogPtr = std::shared_ptr<BlobStorageLog>;

/// Helper class tp write events to BlobStorageLog
/// Can additionally hold some context information
class BlobStorageLogWriter
{
public:
    BlobStorageLogWriter() = default;

    explicit BlobStorageLogWriter(BlobStorageLogPtr log_)
        : log(std::move(log_))
    {}

    void addEvent(
        BlobStorageLogElement::EventType event_type,
        const String & bucket,
        const String & remote_path,
        const String & local_path,
        size_t data_size,
        const Aws::S3::S3Error * error,
        BlobStorageLogElement::EvenTime time_now = {});

    bool isInitialized() const { return log != nullptr; }

    /// Optional context information
    String disk_name;
    String query_id;
    String local_path;

private:
    BlobStorageLogPtr log;
};

}

#endif
