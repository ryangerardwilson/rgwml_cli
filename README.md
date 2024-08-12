    gcc -o rgwml_cli rgwml_cli.c -lmysqlclient -lcjson -lfort
    ./rgwml_cli happy "SELECT * FROM recentincomingcalls LIMIT 2000"
