import pymysql
import pandas as pd
from sqlalchemy import create_engine
import time

def create_bg(user,password,database_name):
    # 连接MySQL数据库
    conn = pymysql.connect(host='localhost', port=3308, user=user, password=password, charset='utf8')

    # 创建数据库

    cursor = conn.cursor()
    cursor.execute(
        'CREATE DATABASE IF NOT EXISTS {} DEFAULT CHARSET utf8 COLLATE utf8_general_ci;'.format(database_name))
    cursor.close()

    # 连接test_db数据库
    conn.select_db(database_name)
    cursor = conn.cursor()

    # 创建表格
    table_name = 'novels'
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS {} (
    照片链接 varchar(1000) not null,
    小说链接 VARCHAR(1000) not null,
    小说名 VARCHAR(220) NOT NULL,
    标签 VARCHAR(100) NOT NULL,
    推荐信息 VARCHAR(2000) NOT NULL,
    作品信息 VARCHAR(2000) NOT NULL,
    更新时间 datetime not null
     ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
     """.format(table_name)
    cursor.execute(create_table_sql)
    cursor.close()

    # 关闭数据库连接
    conn.close()


def import_data(path,passward,database_name):
    # 连接MySQL数据库
    conn = create_engine('mysql+pymysql://root:{}@localhost:3306/{}'.format(passward,database_name))
    # 读取CSV文件中的数据
    df = pd.read_excel(path)

    # 将数据写入MySQL数据库中
    df.to_sql(name='novels', con=conn, if_exists='replace', index=False)


if __name__ == '__main__':
    database_name = 'novels_sys'
    user = input('输入你的数据库名（如：root）：')
    password = input('输入数据的密码：')
    create_bg(user=user,password=password,database_name=database_name)
    print('创建成功')
    import_data(path='data.xlsx',passward=password,database_name=database_name)
    print('导入成功')
    time.sleep(3)
