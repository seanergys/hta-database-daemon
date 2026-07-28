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
#include <seanerbus/internal/message.hpp>
#include <seanerbus/msg/helpers.hpp>

#include <chrono>
#include <exception>
#include <fstream>
#include <iostream>
#include <string>
#include <variant>
#include <vector>

#include <seanerbus/msg/msg.capnp.h>

#include "seanerbus/msg/msg.capnp.h"

// Converts a single hta::Aggregate to a Dataplane Aggregate
seanerbus::helpers::Aggregate hta_aggr_to_sean_aggr(hta::Aggregate aggr)
{

    seanerbus::helpers::Aggregate res;

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
std::pair<std::vector<int64_t>, std::vector<seanerbus::helpers::Aggregate>>
hta_rows_to_sean_aggr(std::vector<hta::Row> aggrs)
{

    auto res = std::make_pair(std::vector<int64_t>{}, std::vector<seanerbus::helpers::Aggregate>{});
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

void sub_handler(seanerbus::client::Connection& conn, const seanerbus::Message& msg,
                 hta::Directory& dir)
{
    if (seanerbus::msg::is_payload_type<TimeValueV1>(msg.payload.first))
    {
        // Big ole try-catch(everything) block, because we do not want the DB to crash like ever.
        // @bielert1 assured me that exceptions only signal recoverable errors.
        //
        // In case the consistency of the HTA is going awry, the HTA will explode far more violently
        try
        {
            capnp::MallocMessageBuilder msg_builder;

            seanerbus::msg::initMessageBuilderFromVec(msg_builder, msg.payload.second);

            ::TimeValueV1::Reader rd = msg_builder.getRoot<::TimeValueV1>();

            seanerbus::logging::debug("Inserting {}: {} for address {}", rd.getTime(),
                                      rd.getValue(), msg.address.as_string());
            auto& metric = dir[std::string(msg.address.as_string())];

            metric.insert(hta::TimeValue{ hta::TimePoint(hta::Duration(rd.getTime())),
                                          (double)rd.getValue() });
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
            (uint64_t)MessageTypes::TIME_VALUE_V1, msg.address.as_string(),
            seanerbus::msg::payload_type_to_str(msg.payload.first));
    }
}

// Handler for HistoryReqV1
void history_req_handler(seanerbus::client::Connection& conn, const seanerbus::Message& msg,
                         hta::Directory& dir)
{
    if (msg.payload.first == (uint64_t)::MessageTypes::HISTORY_REQ_V1)
    {
        try
        {
            capnp::MallocMessageBuilder msg_builder;

            seanerbus::msg::initMessageBuilderFromVec(msg_builder, msg.payload.second);

            ::HistoryReqV1::Reader rd = msg_builder.getRoot<::HistoryReqV1>();

            auto start =
                hta::TimePoint(hta::duration_cast(std::chrono::nanoseconds(rd.getStartTime())));
            auto end =
                hta::TimePoint(hta::duration_cast(std::chrono::nanoseconds(rd.getEndTime())));

            auto addr = seanerbus::Address(
                seanerbus::Uuid(rd.getAddress().getLower(), rd.getAddress().getUpper()));
            if (rd.getHistoryType() == HistoryType::AGGREGATE)
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

                auto hist = seanerbus::helpers::History({}, { aggr });

                capnp::MallocMessageBuilder msg_builder;

                ::HistoryResV1::Builder tv_msg = msg_builder.initRoot<::HistoryResV1>();

                auto history_bld = tv_msg.getOkOrErr().initOk();
                hist.to_capnp(history_bld);

                auto res = capnp::messageToFlatArray(msg_builder);
                conn.respond_to(msg, seanerbus::msg::to_payload_pair(::HistoryResV1::TYPE, res));
            }
            else
            {
                seanerbus::logging::debug("Data from {} ({}-{}) requested (TIMELINE)",
                                          addr.as_string(), start.time_since_epoch().count(),
                                          end.time_since_epoch().count());

                auto& metric = dir[addr.as_string()];

                auto res = metric.retrieve_flex(
                    start, end, hta::duration_cast(std::chrono::nanoseconds(rd.getIntervalMax())));

                // Check if we are getting raw (hta::TimeValue in HTA speak) or aggregated
                // (hta::Row) values
                if (std::holds_alternative<std::vector<hta::TimeValue>>(res))
                {
                    auto raw_data =
                        hta_time_values_to_sean_raws(std::get<std::vector<hta::TimeValue>>(res));

                    seanerbus::logging::debug("Got raw timeline result: {} items",
                                              raw_data.first.size());

                    auto hist = seanerbus::helpers::History(raw_data.first, raw_data.second);
                    capnp::MallocMessageBuilder msg_builder;

                    ::HistoryResV1::Builder tv_msg = msg_builder.initRoot<::HistoryResV1>();

                    auto history_bld = tv_msg.getOkOrErr().initOk();
                    hist.to_capnp(history_bld);

                    auto res = capnp::messageToFlatArray(msg_builder);
                    conn.respond_to(msg,
                                    seanerbus::msg::to_payload_pair(::HistoryResV1::TYPE, res));
                }
                else
                {
                    auto aggr_data = hta_rows_to_sean_aggr(std::get<std::vector<hta::Row>>(res));

                    seanerbus::logging::debug("Got aggregated timeline result: {} items",
                                              aggr_data.first.size());

                    auto hist = seanerbus::helpers::History(aggr_data.first, aggr_data.second);

                    capnp::MallocMessageBuilder msg_builder;

                    ::HistoryResV1::Builder tv_msg = msg_builder.initRoot<::HistoryResV1>();

                    auto history_bld = tv_msg.getOkOrErr().initOk();
                    hist.to_capnp(history_bld);

                    auto res = capnp::messageToFlatArray(msg_builder);
                    conn.respond_to(msg,
                                    seanerbus::msg::to_payload_pair(::HistoryResV1::TYPE, res));
                }
            }
        }
        catch (std::exception& e)
        {
            seanerbus::logging::warn("Servicing HistoryReqV1 failed: {}", e.what());

            capnp::MallocMessageBuilder msg_builder;

            ::HistoryResV1::Builder tv_msg = msg_builder.initRoot<::HistoryResV1>();

            tv_msg.getOkOrErr().setErr(
                fmt::format("Got exception from HTA Time-Series Database: {}", e.what()));
            auto res = capnp::messageToFlatArray(msg_builder);
            conn.respond_to(msg, seanerbus::msg::to_payload_pair(::HistoryResV1::TYPE, res));
        }
    }
    else
    {
        seanerbus::logging::warn(
            "Client {}: expected HistoryResV1, got {} instead. Answering with error",
            msg.from->u_.as_string(), seanerbus::msg::payload_type_to_str(msg.payload.first));

        capnp::MallocMessageBuilder msg_builder;

        ::HistoryResV1::Builder tv_msg = msg_builder.initRoot<::HistoryResV1>();

        tv_msg.getOkOrErr().setErr("Not a HistoryReqV1!");

        auto res = capnp::messageToFlatArray(msg_builder);
        conn.respond_to(msg, seanerbus::msg::to_payload_pair(::HistoryResV1::TYPE, res));
    }
}

int main(void)
{
    seanerbus::logging::set_log_level(seanerbus::logging::LogLevel::TRACE);

    hta::json config{ { "type", "file" },
                      { "path", "./data" },
                      { "metrics", hta::json::object() } };

    std::vector<seanerbus::Address> metrics;
    std::ifstream metrics_file("metrics.list");

    std::string line;

    while (std::getline(metrics_file, line))
    {
        metrics.emplace_back(seanerbus::Address(line));
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
    seanerbus::client::Connection conn("localhost", "5398");

    for (auto metric : metrics)
    {
        seanerbus::logging::debug("subscribing to {}", metric.as_string());
        conn.subscribe(metric, [&dir](seanerbus::client::Connection& conn, const auto& msg) {
            sub_handler(conn, msg, dir);
        });
    }

    auto history_req_addr = seanerbus::Address();

    seanerbus::logging::info("Using address {} for HistoryReqV1 interface",
                             history_req_addr.as_string());

    // Register address for HistoryReqV1 req/res
    conn.register_endpoint(history_req_addr, [&dir](auto& conn, const auto& msg) {
        history_req_handler(conn, msg, dir);
    });

    // Register us a {"service" : "time_series_db"} to the naming serve, so that we can be found
    seanerbus::helpers::register_resolve(conn, { { "service", "time_series_db" } },
                                         history_req_addr, [](auto&, bool, const auto&) {});

    seanerbus::logging::debug("Entering Main Event Loop!");
    seanerbus::client::SimpleEventLoop sel(conn);

    sel.run();
    seanerbus::logging::info("Database received SIGINT, shutting down...");

    conn.reset_shutdown();
    seanerbus::helpers::unregister_resolve(conn, history_req_addr, [](auto&, bool, const auto&) {});
}
