SELECT DISTINCT number FROM remote('localhost,127.0.0.{1,2}', system.numbers) LIMIT 10
