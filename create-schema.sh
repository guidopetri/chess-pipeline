#!/usr/bin/sh

cd ~/Temp
java -jar schemaspy.jar -t pgsql -db chess_db -u [user] -o schema -host localhost -dp postgresql-jdbc.jar -p [password] -s public
zip schema.zip schema -r
rm -rf schema
