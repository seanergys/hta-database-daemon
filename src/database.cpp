// SPDX-FileCopyrightText: 2026 SEANERGYS Partners <https://seanergys.eu>
//
// SPDX-License-Identifier: MIT

#include <hta/chrono.hpp>
#include <hta/hta.hpp>
#include <hta/types.hpp>
#include <log.hpp>
#include <seanerbus/client/connection.hpp>
#include <seanerbus/client/simple_event_loop.hpp>
#include <seanerbus/helpers/resolve.hpp>
#include <seanerbus/helpers/time_series.hpp>

#include <chrono>
#include <exception>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include <seanerbus/msg/msg.capnp.h>

#include "seanerbus/helpers/noexcept.hpp"
#include "seanerbus/msg/message.hpp"
#include "seanerbus/msg/msg.capnp.h"

seanerbus::msg::Address history_req_addr;

namespace
{

void shutdown_action(seanerbus::client::Connection& conn,
                     seanerbus::Expected<void, seanerbus::Error> err)
{
    seanerbus::resolve::unregister_resolve(conn, history_req_addr, [](auto& conn, auto res) {});
    conn.shutdown(err);
}

class HistSimpleEventLoop : public seanerbus::client::SimpleEventLoop
{
public:
    HistSimpleEventLoop(seanerbus::client::Connection& conn)
    : seanerbus::client::SimpleEventLoop(conn)
    {
    }

    void sigint_handler() override
    {
        seanerbus::logging::info("Database received SIGINT, shutting down...");

        shutdown_action(conn_, {});
    }
};

} // namespace

// Converts a single hta::Aggregate to a Dataplane Aggregate
seanerbus::time_series::Aggregate hta_aggr_to_sean_aggr(hta::Aggregate aggr)
{

    seanerbus::time_series::Aggregate res;

    res.minimum = aggr.minimum;
    res.maximum = aggr.maximum;
    res.sum = aggr.sum;
    res.count = aggr.count;
    res.integral = aggr.integral;
    res.activeTime = aggr.active_time.count();

    return res;
}

// Converts a vector of hta::Row's into a vector of timestamps and Aggregates as used by the
// Dataplane
std::pair<std::vector<int64_t>, std::vector<seanerbus::time_series::Aggregate>>
hta_rows_to_sean_aggr(std::vector<hta::Row> aggrs)
{

    auto res =
        std::make_pair(std::vector<int64_t>{}, std::vector<seanerbus::time_series::Aggregate>{});
    for (const auto& aggr : aggrs)
    {
        res.first.emplace_back(aggr.time.time_since_epoch().count());
        res.second.emplace_back(hta_aggr_to_sean_aggr(aggr.aggregate));
    }

    return res;
}

// Converts a vector of hta::TimeValues to timestamps and doubles as used by the Dataplane History
std::pair<std::vector<int64_t>, std::vector<double>>
hta_time_values_to_sean_raws(std::vector<hta::TimeValue> rows)
{
    auto res = std::make_pair(std::vector<int64_t>{}, std::vector<double>{});

    for (const auto& row : rows)
    {
        res.first.emplace_back(row.time.time_since_epoch().count());
        res.second.emplace_back(row.value);
    }

    return res;
}

void sub_handler(seanerbus::client::Connection& conn,
                 seanerbus::Expected<seanerbus::msg::Message, seanerbus::Error> res,
                 hta::Directory& dir)
{

    if (!res.ok())
    {
        seanerbus::logging::warn("Subscription to metric failed: {}", res.unwrap_error().what());
        return;
    }

    auto msg = res.unwrap_ok();
    if (msg.payload_type() == MessageTypes::TIME_VALUE_V1)
    {
        auto rd_res = seanerbus::msg::MessageReader<TimeValueV1>::from_msg(msg);

        if (!rd_res.ok())
        {
            seanerbus::logging::warn("Decoding TiemValueV1 failed: {}", res.unwrap_error().what());
            return;
        }

        auto rd = rd_res.unwrap_ok();
        // Big ole try-catch(everything) block, because we do not want the DB to crash like ever.
        // @bielert1 assured me that exceptions only signal recoverable errors.
        //
        // In case the consistency of the HTA is going awry, the HTA will explode far more violently
        try
        {
            seanerbus::logging::debug("Inserting {}: {} for address {}", rd.reader().getTime(),
                                      rd.reader().getValue(), msg.address().as_string());
            auto& metric = dir[std::string(msg.address().as_string())];

            metric.insert(hta::TimeValue{ hta::TimePoint(hta::Duration(rd.reader().getTime())),
                                          (double)rd.reader().getValue() });
            metric.flush();
        }
        catch (std::exception& e)
        {
            seanerbus::logging::warn("Inserting time-value pair failed: {}, ignoring!", e.what());
        }
    }
    else
    {
        seanerbus::logging::warn(
            "Expected a TimeValueV1 from subscription to {}, got {} instead. Ignoring!",
            (uint64_t)MessageTypes::TIME_VALUE_V1, msg.address().as_string(),
            msg.payload_type_to_str());
    }
}

// Handler for HistoryReqV1
void history_req_handler(seanerbus::client::Connection& conn,
                         seanerbus::Expected<seanerbus::msg::Message, seanerbus::Error> res,
                         hta::Directory& dir)
{

    if (!res.ok())
    {
        seanerbus::logging::warn("Failed to receive request: {}", res.unwrap_error().what());
    }

    auto msg = res.unwrap_ok();

    if (msg.payload_type() == MessageTypes::HISTORY_REQ_V1)
    {

        auto rd_res = seanerbus::msg::MessageReader<HistoryReqV1>::from_msg(msg);

        if (!rd_res.ok())
        {
            auto bd = seanerbus::msg::MessageBuilder<ErrorResV1>();

            bd.builder().setErrorCode(static_cast<uint64_t>(ErrorCodeV1::WRONG_SCHEMA));
            bd.builder().setWhat("Not a HistoryReqV1!");

            conn.respond_to(msg, bd.to_message(msg.address()));

            return;
        }

        auto rd = rd_res.unwrap_ok();
        try
        {
            auto start = hta::TimePoint(
                hta::duration_cast(std::chrono::nanoseconds(rd.reader().getStartTime())));
            auto end = hta::TimePoint(
                hta::duration_cast(std::chrono::nanoseconds(rd.reader().getEndTime())));

            auto addr = seanerbus::msg::Address(seanerbus::msg::Uuid(
                rd.reader().getAddress().getUpper(), rd.reader().getAddress().getLower()));

            if (rd.reader().getHistoryType() == HistoryType::AGGREGATE)
            {
                seanerbus::logging::debug("Data from {} ({}-{}) requested (AGGREGATED)",
                                          addr.as_string(), start.time_since_epoch().count(),
                                          end.time_since_epoch().count());

                auto& metric = dir[addr.as_string()];

                auto aggr = hta_aggr_to_sean_aggr(metric.aggregate(start, end));

                seanerbus::logging::debug(
                    "Got aggregate min:{}, max:{}, sum={}, count={}, integral={}, active_time={}",
                    aggr.minimum, aggr.maximum, aggr.sum, aggr.count, aggr.integral,
                    aggr.activeTime);

                auto hist = seanerbus::time_series::History({}, { aggr });

                seanerbus::msg::MessageBuilder<HistoryResV1> bd;

                auto history_bld = bd.builder().initHist();
                hist.to_capnp(history_bld);

                conn.respond_to(msg, bd.to_message(msg.address()));
            }
            else
            {
                seanerbus::logging::debug("Data from {} ({}-{}) requested (TIMELINE)",
                                          addr.as_string(), start.time_since_epoch().count(),
                                          end.time_since_epoch().count());

                auto& metric = dir[addr.as_string()];

                auto res = metric.retrieve_flex(
                    start, end,
                    hta::duration_cast(std::chrono::nanoseconds(rd.reader().getIntervalMax())));

                // Check if we are getting raw (hta::TimeValue in HTA speak) or aggregated
                // (hta::Row) values
                if (std::holds_alternative<std::vector<hta::TimeValue>>(res))
                {
                    auto raw_data =
                        hta_time_values_to_sean_raws(std::get<std::vector<hta::TimeValue>>(res));

                    seanerbus::logging::debug("Got raw timeline result: {} items",
                                              raw_data.first.size());

                    auto hist = seanerbus::time_series::History(raw_data.first, raw_data.second);

                    seanerbus::msg::MessageBuilder<HistoryResV1> bd;

                    auto history_bld = bd.builder().initHist();
                    hist.to_capnp(history_bld);

                    conn.respond_to(msg, bd.to_message(msg.address()));
                }
                else
                {
                    auto aggr_data = hta_rows_to_sean_aggr(std::get<std::vector<hta::Row>>(res));

                    seanerbus::logging::debug("Got aggregated timeline result: {} items",
                                              aggr_data.first.size());

                    auto hist = seanerbus::time_series::History(aggr_data.first, aggr_data.second);

                    seanerbus::msg::MessageBuilder<HistoryResV1> bd;

                    auto history_bld = bd.builder().initHist();
                    hist.to_capnp(history_bld);

                    conn.respond_to(msg, bd.to_message(msg.address()));
                }
            }
        }
        catch (std::exception& e)
        {
            seanerbus::logging::warn("Servicing HistoryReqV1 failed: {}", e.what());

            auto bd = seanerbus::msg::MessageBuilder<ErrorResV1>();

            bd.builder().setWhat(
                fmt::format("Got exception from HTA Time-Series Database: {}", e.what()));

            conn.respond_to(msg, bd.to_message(msg.address()));
        }
    }
    else
    {
        seanerbus::logging::warn("Expected HistoryResV1, got {} instead. Answering with error",
                                 msg.payload_type_to_str());

        auto bd = seanerbus::msg::MessageBuilder<ErrorResV1>();

        bd.builder().setErrorCode(static_cast<uint64_t>(ErrorCodeV1::WRONG_SCHEMA));
        bd.builder().setWhat("Not a HistoryReqV1!");

        conn.respond_to(msg, bd.to_message(msg.address()));
    }
}

int main(void)
{
    seanerbus::logging::set_log_level(seanerbus::logging::LogLevel::TRACE);

    hta::json config{ { "type", "file" },
                      { "path", "./data" },
                      { "metrics", hta::json::object() } };

    std::vector<seanerbus::msg::Address> metrics;
    std::ifstream metrics_file("metrics.list");

    std::string line;

    while (std::getline(metrics_file, line))
    {
        metrics.emplace_back(seanerbus::msg::Address(line));
    }

    if (metrics.empty())
    {
        std::cerr << "Need to define at least one metric to record\n";
        std::cerr << "To record a metric, add a line with the UUID-Address of the metric to the "
                     "file metrics.list!\n";
        return 1;
    }

    for (auto metric : metrics)
    {
        config["metrics"][metric.as_string()] = hta::json{
            { "mode", "RW" },
            { "interval_min", 40000000000 },
            { "interval_max", 400000000000000 },
            { "interval_factor", 10 },
        };
    }

    hta::Directory dir(config, true);
    seanerbus::client::Connection conn("demobus://localhost:5398", "demobus://localhost:5398");

    for (auto metric : metrics)
    {
        seanerbus::logging::debug("subscribing to {}", metric.as_string());
        conn.subscribe(metric, [&dir](seanerbus::client::Connection& conn, const auto& msg) {
            sub_handler(conn, msg, dir);
        });
    }

    history_req_addr = seanerbus::msg::Address();

    seanerbus::logging::info("Using address {} for HistoryReqV1 interface",
                             history_req_addr.as_string());

    // Register address for HistoryReqV1 req/res
    conn.register_endpoint(history_req_addr, [&dir](auto& conn, const auto& msg) {
        history_req_handler(conn, msg, dir);
    });

    // Register us a {"service" : "time_series_db"} to the naming serve, so that we can be found
    seanerbus::resolve::register_resolve(conn, { { "service", "time_series_db" } },
                                         history_req_addr, [](auto& conn, auto err) {
                                             if (!err.ok())
                                             {
                                                 shutdown_action(conn, {});
                                             }
                                         });

    seanerbus::logging::debug("Entering Main Event Loop!");
    HistSimpleEventLoop sel(conn);

    sel.run();

    if (!conn.result().ok())
    {
        seanerbus::logging::warn("Running HTA resulted in an error: {}",
                                 conn.result().get_error().what());
    }
}
