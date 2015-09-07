sql.table.exists=

sql.table.create=CREATE TABLE %s (name VARCHAR(50) NOT NULL, content LONGVARBINARY, size INTEGER, PRIMARY KEY (name) )

sql.select.listall=SELECT name FROM %s

sql.select.name=SELECT name FROM %s WHERE name = ?
sql.select.size=SELECT size FROM %s WHERE name = ?
sql.select.content=SELECT content FROM %s WHERE name = ?

sql.insert=INSERT INTO %s (content, size, name) VALUES (?, ?, ?)

sql.update=UPDATE %s SET content = ?, size = ? where name = ?

sql.update.rename=UPDATE %s SET name = ? WHERE name = ?

sql.delete=DELETE FROM %s WHERE name = ?
