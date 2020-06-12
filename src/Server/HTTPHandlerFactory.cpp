#include "HTTPHandlerFactory.h"

#include <Poco/Util/LayeredConfiguration.h>

#include "HTTPHandler.h"
#include "NotFoundHandler.h"
#include "StaticRequestHandler.h"
#include "ReplicasStatusHandler.h"
#include "InterserverIOHTTPHandler.h"
#include "PrometheusRequestHandler.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
    extern const int INVALID_CONFIG_PARAMETER;
}

HTTPRequestHandlerFactoryMain::HTTPRequestHandlerFactoryMain(const std::string & name_)
    : log(&Poco::Logger::get(name_)), name(name_)
{
}

Poco::Net::HTTPRequestHandler * HTTPRequestHandlerFactoryMain::createRequestHandler(const Poco::Net::HTTPServerRequest & request)
{
    LOG_TRACE(log, "HTTP Request for {}. Method: {}, Address: {}, User-Agent: {}{}, Content Type: {}, Transfer Encoding: {}",
        name, request.getMethod(), request.clientAddress().toString(), request.has("User-Agent") ? request.get("User-Agent") : "none",
        (request.hasContentLength() ? (", Length: " + std::to_string(request.getContentLength())) : ("")),
        request.getContentType(), request.getTransferEncoding());

    for (auto & handler_factory : child_factories)
    {
        auto * handler = handler_factory->createRequestHandler(request);
        if (handler != nullptr)
            return handler;
    }

    if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_GET
        || request.getMethod() == Poco::Net::HTTPRequest::HTTP_HEAD
        || request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST)
    {
        return new NotFoundHandler;
    }

    return nullptr;
}

HTTPRequestHandlerFactoryMain::~HTTPRequestHandlerFactoryMain()
{
    while (!child_factories.empty())
    {
        delete child_factories.back();
        child_factories.pop_back();
    }
}

HTTPRequestHandlerFactoryMain::TThis * HTTPRequestHandlerFactoryMain::addHandler(Poco::Net::HTTPRequestHandlerFactory * child_factory)
{
    child_factories.emplace_back(child_factory);
    return this;
}

static inline auto createHandlersFactoryFromConfig(
    IServer & server, const std::string & name, const String & prefix, AsynchronousMetrics & async_metrics)
{
    auto main_handler_factory = std::make_unique<HTTPRequestHandlerFactoryMain>(name);

    Poco::Util::AbstractConfiguration::Keys keys;
    server.config().keys(prefix, keys);

    for (const auto & key : keys)
    {
        if (!startsWith(key, "rule"))
            throw Exception("Unknown element in config: " + prefix + "." + key + ", must be 'rule'", ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);

        const auto & handler_type = server.config().getString(prefix + "." + key + ".handler.type", "");

        if (handler_type == "root")
            addRootHandlerFactory(*main_handler_factory, server);
        else if (handler_type == "ping")
            addPingHandlerFactory(*main_handler_factory, server);
        else if (handler_type == "defaults")
            addDefaultHandlersFactory(*main_handler_factory, server, async_metrics);
        else if (handler_type == "prometheus")
            addPrometheusHandlerFactory(*main_handler_factory, server, async_metrics);
        else if (handler_type == "replicas_status")
            addReplicasStatusHandlerFactory(*main_handler_factory, server);
        else if (handler_type == "static")
            main_handler_factory->addHandler(createStaticHandlerFactory(server, prefix + "." + key));
        else if (handler_type == "dynamic_query_handler")
            main_handler_factory->addHandler(createDynamicHandlerFactory(server, prefix + "." + key));
        else if (handler_type == "predefined_query_handler")
            main_handler_factory->addHandler(createPredefinedHandlerFactory(server, prefix + "." + key));
        else if (handler_type.empty())
            throw Exception("Handler type in config is not specified here: " +
                            prefix + "." + key + ".handler.type", ErrorCodes::INVALID_CONFIG_PARAMETER);
        else
            throw Exception("Unknown handler type '" + handler_type +"' in config here: " +
                            prefix + "." + key + ".handler.type",ErrorCodes::INVALID_CONFIG_PARAMETER);
    }

    return main_handler_factory.release();
}

static inline Poco::Net::HTTPRequestHandlerFactory * createHTTPHandlerFactory(
    IServer & server, const std::string & name, AsynchronousMetrics & async_metrics)
{
    if (server.config().has("http_handlers"))
        return createHandlersFactoryFromConfig(server, name, "http_handlers", async_metrics);
    else
    {
        auto factory = std::make_unique<HTTPRequestHandlerFactoryMain>(name);

        addRootHandlerFactory(*factory, server);
        addPingHandlerFactory(*factory, server);
        addReplicasStatusHandlerFactory(*factory, server);
        addPrometheusHandlerFactory(*factory, server, async_metrics);

        auto query_handler = std::make_unique<HandlingRuleHTTPHandlerFactory<DynamicQueryHandler>>(server, "query");
        query_handler->allowPostAndGetParamsRequest();
        factory->addHandler(query_handler.release());
        return factory.release();
    }
}

static inline Poco::Net::HTTPRequestHandlerFactory * createInterserverHTTPHandlerFactory(IServer & server, const std::string & name)
{
    auto factory = std::make_unique<HTTPRequestHandlerFactoryMain>(name);

    addRootHandlerFactory(*factory, server);
    addPingHandlerFactory(*factory, server);
    addReplicasStatusHandlerFactory(*factory, server);

    auto main_handler = std::make_unique<HandlingRuleHTTPHandlerFactory<InterserverIOHTTPHandler>>(server);
    main_handler->allowPostAndGetParamsRequest();
    factory->addHandler(main_handler.release());

    return factory.release();
}

Poco::Net::HTTPRequestHandlerFactory * createHandlerFactory(IServer & server, AsynchronousMetrics & async_metrics, const std::string & name)
{
    if (name == "HTTPHandler-factory" || name == "HTTPSHandler-factory")
        return createHTTPHandlerFactory(server, name, async_metrics);
    else if (name == "InterserverIOHTTPHandler-factory" || name == "InterserverIOHTTPSHandler-factory")
        return createInterserverHTTPHandlerFactory(server, name);
    else if (name == "PrometheusHandler-factory")
    {
        auto factory = std::make_unique<HTTPRequestHandlerFactoryMain>(name);
        auto handler = std::make_unique<HandlingRuleHTTPHandlerFactory<PrometheusRequestHandler>>(
            server, PrometheusMetricsWriter(server.config(), "prometheus", async_metrics));
        handler->attachStrictPath(server.config().getString("prometheus.endpoint", "/metrics"))->allowGetAndHeadRequest();
        factory->addHandler(handler.release());
        return factory.release();
    }

    throw Exception("LOGICAL ERROR: Unknown HTTP handler factory name.", ErrorCodes::LOGICAL_ERROR);
}

void addDefaultHandlersFactory(HTTPRequestHandlerFactoryMain & factory, IServer & server, AsynchronousMetrics & async_metrics)
{
    addRootHandlerFactory(factory, server);
    addPingHandlerFactory(factory, server);
    addReplicasStatusHandlerFactory(factory, server);
    addPrometheusHandlerFactory(factory, server, async_metrics);
}

}
