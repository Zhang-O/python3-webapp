#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'Zhang'

# aiomysql是Mysql的python异步驱动程序，操作数据库要用到
import asyncio, logging, aiomysql


# 这个函数的作用是输出信息，让你知道这个时间点程序在做什么
def log(sql, args=()):
    logging.info('SQL: %s' % sql)

# 创建全局连接池
# 这个函数将来会在app.py的init函数中引用
# 目的是为了让每个HTTP请求都能s从连接池中直接获取数据库连接
# 避免了频繁关闭和打开数据库连接
@asyncio.coroutine
def create_pool(loop, **kw):
    logging.info(' 常见数据库连接池... (create database connection pool...)')
    # log('create database connection pool...')

    # 声明变量__pool是一个全局变量，如果不加声明，__pool就会被默认为一个私有变量，不能被其他函数引用
    global __pool
    __pool = yield from aiomysql.create_pool(

        # 下面就是创建数据库连接需要用到的一些参数，从**kw（关键字参数）中取出来
        # kw.get的作用应该是，当没有传入参数是，默认参数就是get函数的第二项

        host=kw.get('host', 'localhost'),  # 数据库服务器位置，默认设在本地
        port=kw.get('port', 3306),  # mysql的端口，默认设为3306
        user=kw['user'],  # 登陆用户名，通过关键词参数传进来。
        password=kw['password'],  # 登陆密码，通过关键词参数传进来
        db=kw['db'],  # 当前数据库名
        charset=kw.get('charset', 'utf8'),  # 设置编码格式，默认为utf-8
        autocommit=kw.get('autocommit', True),  # 自动提交模式，设置默认开启
        maxsize=kw.get('maxsize', 10),  # 最大连接数默认设为10
        minsize=kw.get('minsize', 1),  # 最小连接数，默认设为1，这样可以保证任何时候都会有一个数据库连接
        loop=loop  # 传递消息循环对象，用于异步执行
    )


# =================================以下是SQL函数处理区====================================
# select和execute方法是实现其他Model类中SQL语句都经常要用的方法

# 将执行SQL的代码封装仅select函数，调用的时候只要传入sql，和sql所需要的一些参数就好
# sql参数即为sql语句，args表示要搜索的参数
# size用于指定最大的查询数量，不指定将返回所有查询结果
@asyncio.coroutine
def select(sql, args, size=None):
    log(sql, args)
    # 声明全局变量，这样才能引用create_pool函数创建的__pool变量
    global __pool
    # 从连接池中获得一个数据库连接
    # 用with语句可以封装清理（关闭conn)和处理异常工作
    with (yield from __pool) as conn:
        cur = yield from conn.cursor(aiomysql.DictCursor)
        # SQL语句的占位符是?，而MySQL的占位符是%s
        yield from cur.execute(sql.replace('?', '%s'), args or ())
        if size:
            rs = yield from cur.fetchmany(size)
        else:
            rs = yield from cur.fetchall()
        yield from cur.close()
        logging.info('rows returned: %s' % len(rs))
        return rs


# 定义execute()函数执行insert update delete语句
# execute()函数只返回结果数，不返回结果集，适用于insert, update这些语句
@asyncio.coroutine
def execute(sql, args):
    log(sql)
    with (yield from __pool) as conn:
        try:
            cur = yield from conn.cursor()
            yield from cur.execute(sql.replace('?', '%s'), args)
            affected = cur.rowcount
            yield from cur.close()
        except BaseException as e:
            raise
        return affected

# 这个函数在元类中被引用，作用是创建一定数量的占位符
def create_args_string(num):
    L = []
    for n in range(num):
        L.append('?')
    #比如说num=3，那L就是['?','?','?']，通过下面这句代码返回一个字符串'?,?,?'
    return ', '.join(L)


class ModelMetaclass(type):

    def __new__(cls, name, bases, attrs):
        # 排除Model类本身:
        if name=='Model':
            return type.__new__(cls, name, bases, attrs)
        # 获取table名称: None or 1 -> 1
        tableName = attrs.get('__table__', None) or name
        logging.info('found model: %s (table: %s)' % (name, tableName))
        # 获取所有的Field和主键名:
        mappings = dict()
        fields = []
        primaryKey = None
        for k, v in attrs.items():
            if isinstance(v, Field):
                logging.info('  found mapping: %s ==> %s' % (k, v))
                mappings[k] = v
                if v.primary_key:
                    # 找到主键:
                    if primaryKey:
                        raise RuntimeError('Duplicate primary key for field: %s' % k)
                    primaryKey = k
                else:
                    fields.append(k)
        if not primaryKey:
            raise RuntimeError('Primary key not found.')
        for k in mappings.keys():
            attrs.pop(k)
        escaped_fields = list(map(lambda f: '`%s`' % f, fields))
        attrs['__mappings__'] = mappings # 保存属性和列的映射关系
        attrs['__table__'] = tableName
        attrs['__primary_key__'] = primaryKey # 主键属性名
        attrs['__fields__'] = fields # 除主键外的属性名
        # 构造默认的SELECT, INSERT, UPDATE和DELETE语句:
        attrs['__select__'] = 'select `%s`, %s from `%s`' % (primaryKey, ', '.join(escaped_fields), tableName)
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (tableName, ', '.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
        return type.__new__(cls, name, bases, attrs)

# =====================================Model基类区==========================================


# 编写元类
class ModelMetaclass(type):
    def __new__(cls, name, bases, attrs):
        # 排除Model类本身
        if name=='Model':
            return type.__new__(cls, name, bases, attrs)
        # 获取table名称
        tableName = attrs.get('__table__', None) or name
        logging.info('found model: %s (table: %s)' % (name, tableName))
        # 获取所有定义域中的属性和主键
        mappings = dict()
        fields = []
        primaryKey = None
        for k, v in attrs.items():
            if isinstance(v, Field):
                logging.info('  found mapping: %s ==> %s' % (k, v))
                mappings[k] = v
                # 先判断找到的映射是不是主键
                if v.primary_key:
                    if primaryKey:  # 若主键已存在,又找到一个主键,将报错,每张表有且仅有一个主键
                        raise StandardError('Duplicate primary key for field: %s' % k)
                    primaryKey = k
                else:
                    fields.append(k)
        # 如果没有找到主键，也会报错
        if not primaryKey:
            raise StandardError('Primary key not found.')
        # 定义域中的key值已经添加到fields里了，就要在attrs中删除，避免重名导致运行时错误
        for k in mappings.keys():
            attrs.pop(k)
        # 将非主键的属性变形,放入escaped_fields中,方便sql语句的书写
        escaped_fields = list(map(lambda f: '`%s`' % f, fields))
        attrs['__mappings__'] = mappings  # 保存属性和列的映射关系
        attrs['__table__'] = tableName  # 表名
        attrs['__primary_key__'] = primaryKey  # 主键属性名
        attrs['__fields__'] = fields  # 除主键外的属性名
        # 构造默认的SELECT, INSERT, UPDATE, DELETE语句
        # 以下都是sql语句
        attrs['__select__'] = 'select `%s`, %s from `%s`' % (primaryKey, ', '.join(escaped_fields), tableName)
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (tableName, ', '.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
        return type.__new__(cls, name, bases, attrs)



# =====================================Field定义域区==============================================
# 首先来定义Field类，它负责保存数据库表的字段名和字段类型
class Field(object):
    # 定义域的初始化，包括属性（列）名，属性（列）的类型，主键，默认值
    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default

    # 定制输出信息为 类名，列的类型，列名
    def __str__(self):
        return '<%s, %s:%s>' % (self.__class__.__name__, self.column_type, self.name)

class StringField(Field):
    # ddl是数据定义语言("data definition languages")，默认值是'varchar(100)'，意思是可变字符串，长度为100
    # 和char相对应，char是固定长度，字符串长度不够会自动补齐，varchar则是多长就是多长，但最长不能超过规定长度
    def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
        # ddl='varchar(100)'  映射为  self.column_type
        super().__init__(name, ddl, primary_key, default)


class BooleanField(Field):

    def __init__(self, name=None, default=False):
        super().__init__(name, 'boolean', False, default)

class IntegerField(Field):

    def __init__(self, name=None, primary_key=False, default=0):
        super().__init__(name, 'bigint', primary_key, default)

class FloatField(Field):

    def __init__(self, name=None, primary_key=False, default=0.0):
        super().__init__(name, 'real', primary_key, default)

class TextField(Field):

    def __init__(self, name=None, default=None):
        super().__init__(name, 'text', False, default)