from airflow.hooks.mysql_hook import MySqlHook

mysqlhook = MySqlHook(mysql_conn_id="PTT")

def create_table(tablename):
    connection = mysqlhook.get_conn()
    mysqlhook.set_autocommit(connection, True)
    cursor = connection.cursor()

    sql = """CREATE TABLE IF NOT EXISTS `{}` (
          `id` bigint(20) NOT NULL AUTO_INCREMENT,
          `title` varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL,
          `author` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
          `board` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
          `url` varchar(4096) COLLATE utf8mb4_unicode_ci NOT NULL,
          `timestamp` varchar(512) COLLATE utf8mb4_unicode_ci NOT NULL,
          `description` varchar(4096) COLLATE utf8mb4_unicode_ci NOT NULL,
            """.format(tablename)
    cursor.execute(sql)
    cursor.close()