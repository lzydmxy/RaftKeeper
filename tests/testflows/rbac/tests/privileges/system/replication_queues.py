from testflows.core import *
from testflows.asserts import error

from rbac.requirements import *
from rbac.helper.common import *
import rbac.helper.errors as errors

@TestSuite
def replicated_privileges_granted_directly(self, node=None):
    """Check that a user is able to execute `SYSTEM REPLICATION QUEUES` commands if and only if
    the privilege has been granted directly.
    """
    user_name = f"user_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):

        Suite(run=check_replicated_privilege, flags=TE,
            examples=Examples("privilege on grant_target_name user_name", [
                tuple(list(row)+[user_name,user_name]) for row in check_replicated_privilege.examples
            ], args=Args(name="check privilege={privilege}", format_name=True)))

@TestSuite
def replicated_privileges_granted_via_role(self, node=None):
    """Check that a user is able to execute `SYSTEM REPLICATION QUEUES` commands if and only if
    the privilege has been granted via role.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"), role(node, f"{role_name}"):

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Suite(run=check_replicated_privilege, flags=TE,
            examples=Examples("privilege on grant_target_name user_name", [
                tuple(list(row)+[role_name,user_name]) for row in check_replicated_privilege.examples
            ], args=Args(name="check privilege={privilege}", format_name=True)))

@TestOutline(Suite)
@Examples("privilege on",[
    ("SYSTEM", "*.*"),
    ("SYSTEM REPLICATION QUEUES", "table"),
    ("SYSTEM STOP REPLICATION QUEUES", "table"),
    ("SYSTEM START REPLICATION QUEUES", "table"),
    ("START REPLICATION QUEUES", "table"),
    ("STOP REPLICATION QUEUES", "table"),
])
def check_replicated_privilege(self, privilege, on, grant_target_name, user_name, node=None):
    """Run checks for commands that require SYSTEM REPLICATION QUEUES privilege.
    """

    if node is None:
        node = self.context.node

    Suite(test=start_replication_queues, setup=instrument_clickhouse_server_log)(privilege=privilege, on=on, grant_target_name=grant_target_name, user_name=user_name)
    Suite(test=stop_replication_queues, setup=instrument_clickhouse_server_log)(privilege=privilege, on=on, grant_target_name=grant_target_name, user_name=user_name)

@TestSuite
def start_replication_queues(self, privilege, on, grant_target_name, user_name, node=None):
    """Check that user is only able to execute `SYSTEM START REPLICATION QUEUES` when they have privilege.
    """
    exitcode, message = errors.not_enough_privileges(name=user_name)
    table_name = f"table_name_{getuid()}"

    if node is None:
        node = self.context.node

    on = on.replace("table", f"{table_name}")

    with table(node, table_name, "ReplicatedMergeTree-sharded_cluster"):

        with Scenario("SYSTEM START REPLICATION QUEUES without privilege"):
            with When("I check the user can't start sends"):
                node.query(f"SYSTEM START REPLICATION QUEUES {table_name}", settings = [("user", f"{user_name}")],
                    exitcode=exitcode, message=message)

        with Scenario("SYSTEM START REPLICATION QUEUES with privilege"):
            with When(f"I grant {privilege} on the table"):
                node.query(f"GRANT {privilege} ON {on} TO {grant_target_name}")

            with Then("I check the user can start sends"):
                node.query(f"SYSTEM START REPLICATION QUEUES {table_name}", settings = [("user", f"{user_name}")])

        with Scenario("SYSTEM START REPLICATION QUEUES with revoked privilege"):
            with When(f"I grant {privilege} on the table"):
                node.query(f"GRANT {privilege} ON {on} TO {grant_target_name}")

            with And(f"I revoke {privilege} on the table"):
                node.query(f"REVOKE {privilege} ON {on} FROM {grant_target_name}")

            with Then("I check the user can't start sends"):
                node.query(f"SYSTEM START REPLICATION QUEUES {table_name}", settings = [("user", f"{user_name}")],
                    exitcode=exitcode, message=message)

@TestSuite
def stop_replication_queues(self, privilege, on, grant_target_name, user_name, node=None):
    """Check that user is only able to execute `SYSTEM STOP REPLICATION QUEUES` when they have privilege.
    """
    exitcode, message = errors.not_enough_privileges(name=user_name)
    table_name = f"table_name_{getuid()}"

    if node is None:
        node = self.context.node

    on = on.replace("table", f"{table_name}")

    with table(node, table_name, "ReplicatedMergeTree-sharded_cluster"):

        with Scenario("SYSTEM STOP REPLICATION QUEUES without privilege"):
            with When("I check the user can't stop sends"):
                node.query(f"SYSTEM STOP REPLICATION QUEUES {table_name}", settings = [("user", f"{user_name}")],
                    exitcode=exitcode, message=message)

        with Scenario("SYSTEM STOP REPLICATION QUEUES with privilege"):
            with When(f"I grant {privilege} on the table"):
                node.query(f"GRANT {privilege} ON {on} TO {grant_target_name}")

            with Then("I check the user can start sends"):
                node.query(f"SYSTEM STOP REPLICATION QUEUES {table_name}", settings = [("user", f"{user_name}")])

        with Scenario("SYSTEM STOP REPLICATION QUEUES with revoked privilege"):
            with When(f"I grant {privilege} on the table"):
                node.query(f"GRANT {privilege} ON {on} TO {grant_target_name}")

            with And(f"I revoke {privilege} on the table"):
                node.query(f"REVOKE {privilege} ON {on} FROM {grant_target_name}")

            with Then("I check the user can't start sends"):
                node.query(f"SYSTEM STOP REPLICATION QUEUES {table_name}", settings = [("user", f"{user_name}")],
                    exitcode=exitcode, message=message)

@TestFeature
@Name("system replication queues")
@Requirements(
    RQ_SRS_006_RBAC_Privileges_System_ReplicationQueues("1.0"),
)
def feature(self, node="clickhouse1"):
    """Check the RBAC functionality of SYSTEM REPLICATION QUEUES.
    """
    self.context.node = self.context.cluster.node(node)

    Suite(run=replicated_privileges_granted_directly, setup=instrument_clickhouse_server_log)
    Suite(run=replicated_privileges_granted_via_role, setup=instrument_clickhouse_server_log)
