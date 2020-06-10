#!/usr/bin/expect -f

log_user 0
set timeout 60
match_max 100000

spawn clickhouse-client
expect ":) "

# Make a query
send -- "SELECT 'for the history'\r"
expect "for the history"
expect ":) "

# Kill the client to check if the history was saved
exec kill -9 [exp_pid]
close

# Run client one more time and press "up" to see the last recorded query
spawn clickhouse-client
expect ":) "
send -- "\[A"
expect "SELECT 'for the history'"

# Will check that Ctrl+C clears current line.
send -- "\3"
expect ":)"

# Will check that second Ctrl+C invocation does not exit from client.
send -- "\3"
expect ":)"

# But Ctrl+D does.
send -- "\4"
expect eof
