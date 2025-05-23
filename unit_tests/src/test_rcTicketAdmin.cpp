#include <catch2/catch_all.hpp>

#include "irods/authentication_plugin_framework.hpp"
#include "irods/client_connection.hpp"
#include "irods/filesystem.hpp"
#include "irods/irods_at_scope_exit.hpp"
#include "irods/irods_auth_constants.hpp"
#include "irods/rcConnect.h"
#include "irods/rodsClient.h"
#include "irods/ticket_administration.hpp"
#include "irods/user_administration.hpp"

#include <nlohmann/json.hpp>

namespace ia = irods::authentication;

// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST_CASE("rcTicketAdmin")
{
    load_client_api_plugins();

    rodsEnv env;
    _getRodsEnv(env);

    SECTION("#7179: disassociate ticket from connection")
    {
        //
        // IMPORTANT: This test must be run using a rodsadmin account.
        //

        // clang-format off
        namespace adm = irods::experimental::administration;
        namespace fs  = irods::experimental::filesystem;
        // clang-format on

        irods::experimental::client_connection conn;

        // Create a ticket that allows users to read the rodsadmin's home collection.
        auto ticket = adm::ticket::client::create_ticket(conn, adm::ticket::ticket_type::read, env.rodsHome);
        CHECK_FALSE(ticket.empty());

        // Create a rodsuser.
        const adm::user test_user{"issue_7179_test_user"};

        REQUIRE_NOTHROW(adm::client::add_user(conn, test_user));

        irods::at_scope_exit remove_user{
            [&conn, &test_user] { CHECK_NOTHROW(adm::client::remove_user(conn, test_user)); }};

        adm::user_password_property password_prop{"rods"};
        REQUIRE_NOTHROW(adm::client::modify_user(conn, test_user, password_prop));

        // Enable the ticket and show the test_user can read the rodsadmin's home collection.
        irods::experimental::client_connection test_user_conn{
            irods::experimental::defer_authentication, env.rodsHost, env.rodsPort, {test_user.name, env.rodsZone}};
        const auto ctx = nlohmann::json{
            {irods::AUTH_PASSWORD_KEY, password_prop.value.data()}, {ia::scheme_name, env.rodsAuthScheme}};
        REQUIRE(ia::authenticate_client(static_cast<RcComm&>(test_user_conn), ctx) == 0);

        TicketAdminInput input{};
        input.arg1 = "session";
        input.arg2 = ticket.data();
        input.arg3 = "";

        REQUIRE(rcTicketAdmin(static_cast<RcComm*>(test_user_conn), &input) == 0);
        CHECK(fs::client::exists(test_user_conn, env.rodsHome));

        // Disable the ticket and show the rodsuser can no longer read the rodsadmin's home collection.
        input.arg2 = "";

        REQUIRE(rcTicketAdmin(static_cast<RcComm*>(test_user_conn), &input) == 0);
        CHECK_FALSE(fs::client::exists(test_user_conn, env.rodsHome));
    }
}
