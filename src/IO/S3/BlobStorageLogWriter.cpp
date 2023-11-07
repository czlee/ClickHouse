#include <IO/S3/BlobStorageLogWriter.h>

#if USE_AWS_S3

#include <base/getThreadId.h>
#include <IO/S3/Client.h>

namespace DB
{

void BlobStorageLogWriter::addEvent(
    BlobStorageLogElement::EventType event_type,
    const String & bucket,
    const String & remote_path,
    const String & local_path_,
    size_t data_size,
    const Aws::S3::S3Error * error,
    BlobStorageLogElement::EvenTime time_now)
{

    if (!log)
        return;

    if (!time_now.time_since_epoch().count())
        time_now = std::chrono::system_clock::now();

    BlobStorageLogElement element;

    element.event_type = event_type;

    element.query_id = query_id;
    element.thread_id = getThreadId();

    element.disk_name = disk_name;
    element.bucket = bucket;
    element.remote_path = remote_path;
    element.local_path = local_path_.empty() ? local_path : local_path_;

    if (data_size > std::numeric_limits<decltype(element.data_size)>::max())
        element.data_size = std::numeric_limits<decltype(element.data_size)>::max();
    else
        element.data_size = static_cast<decltype(element.data_size)>(data_size);

    if (error)
    {
        element.error_code = static_cast<Int32>(error->GetErrorType());
        element.error_msg = error->GetMessage();
    }

    element.event_time = time_now;

    log->add(element);
}

}

#endif
