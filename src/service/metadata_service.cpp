#include "aistore/service/metadata_service.hpp"

#include <boost/json.hpp>

namespace aistore::service {

namespace {

namespace beast_http = aistore::http::beast_http;

aistore::http::HttpResponse make_json_response(const aistore::http::HttpRequest& request, beast_http::status status,
                                               const boost::json::value& body) {
    aistore::http::HttpResponse response{
        status,
        request.version(),
    };
    response.set(beast_http::field::content_type, "application/json");
    response.body() = boost::json::serialize(body);
    response.prepare_payload();
    return response;
}

}  // namespace

aistore::http::HttpResponse MetadataService::handle_request(const aistore::http::HttpRequest& request) const {
    if (request.target() == "/health") {
        if (request.method() == beast_http::verb::get) {
            return make_json_response(request, beast_http::status::ok,
                                      boost::json::object{
                                          {"status", "ok"},
                                      });
        }

        aistore::http::HttpResponse response = make_json_response(request, beast_http::status::method_not_allowed,
                                                                  boost::json::object{
                                                                      {"error", "method_not_allowed"},
                                                                  });
        response.set(beast_http::field::allow, "GET");
        return response;
    }

    return make_json_response(request, beast_http::status::not_found,
                              boost::json::object{
                                  {"error", "not_found"},
                              });
}

}  // namespace aistore::service
