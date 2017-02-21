#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'Zhang'

# aiomysql是Mysql的python异步驱动程序，操作数据库要用到
import asyncio, logging, aiomysql

s = "just for test"
# 这个函数的作用是输出信息，让你知道这个时间点程序在做什么
def log(sql, args=()):
    logging.info('SQL: %s' % sql)

# 创建全局连接池
# 这个函数将来会在app.py的init函数中引用
# 目的是为了让每个HTTP请求都能s从连接池中直接获取数据库连接
# 避免了频繁关闭和打开数据库连接

async def create_pool(loop, **kw):
    logging.info(' 创建数据库连接池... (create database connection pool...)')
    # log('create database connection pool...')

    # 声明变量__pool是一个全局变量，如果不加声明，__pool就会被默认为一个私有变量，不能被其他函数引用
    global __pool
    __pool = await aiomysql.create_pool(

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

# 将执行SQL的代码封装进select函数，调用的时候只要传入sql，和sql所需要的一些参数就好
# sql参数即为sql语句，args表示要搜索的参数
# size用于指定最大的查询数量，不指定将返回所有查询结果
async def select(sql, args, size=None):
    log(sql, args)
    # 声明全局变量，这样才能引用create_pool函数创建的__pool变量
    global __pool
    # 从连接池中获得一个数据库连接
    # 用with语句可以封装清理（关闭conn)和处理异常工作
    with (await __pool) as conn:
        cur = await conn.cursor(aiomysql.DictCursor)
        # SQL语句的占位符是?，而MySQL的占位符是%s
        await cur.execute(sql.replace('?', '%s'), args or ())
        if size:
            rs = await cur.fetchmany(size)
        else:
            rs = await cur.fetchall()
        await cur.close()
        logging.info('rows returned: %s' % len(rs))
        return rs


# 定义execute()函数执行insert update delete语句
# execute()函数只返回结果数，不返回结果集，适用于insert, update这些语句
async def execute(sql, args):
    log(sql)
    with (await __pool) as conn:
        try:
            cur = await conn.cursor()
            await cur.execute(sql.replace('?', '%s'), args)
            affected = cur.rowcount
            await cur.close()
        except BaseException as e:
            raise
        return affected


# =====================================Model基类区==========================================

# 这个函数在元类中被引用，作用是创建一定数量的占位符
def create_args_string(num):
    L = []
    for n in range(num):
        L.append('?')
    #比如说num=3，那L就是['?','?','?']，通过下面这句代码返回一个字符串'?,?,?'
    return ', '.join(L)


class ModelMetaclass(type):
    #cls <class 'orm.ModelMetaclass'>  元类
    #name 'User' 子类类名
    #bases <class 'tuple'>: (<class 'orm.Model'>,)  子类初始化时的父类
    #attrs 是dict ，eg: 包含了user类的属性，模块名
    def __new__(cls, name, bases, attrs):
        # 排除Model类本身:
        if name == 'Model':
            return type.__new__(cls, name, bases, attrs)
        # 获取table名称: None or 1 -> 1
        tableName = attrs.get('__table__', None) or name # 获取表名(用户 、博客、评论)
        # name 是models.py 里面的类User Blog 等类名，是模型，tableName 是数据库里对应的表名
        logging.info('found model(建立模型): %s(表名(table): %s)' % (name, tableName))
        # 获取 attrs 所有的Field和主键名:
        mappings = dict()
        fields = [] # 除主键外的属性名
        primaryKey = None
        for k, v in attrs.items():
            # 表的每一个字段 都是 Field 的子类的实例 ，也是Field 的实例
            if isinstance(v, Field):
                logging.info('  found mapping (建立映射): %s ==> %s' % (k, v))
                mappings[k] = v
                if v.primary_key:
                    # 找到主键:
                    if primaryKey:
                        raise RuntimeError('Duplicate primary key for field (重复的主键字段): %s' % k)
                    primaryKey = k
                else:
                    fields.append(k)
        if not primaryKey:
            raise RuntimeError('%s:表中未发现主键.'% tableName)
        for k in mappings.keys():
            attrs.pop(k) #从类属性中删除跟数据库对应的属性

        # list(map(lambda f: '`%s`' % f, [1, 2, 3]))  --> ['`1`', '`2`', '`3`']
        escaped_fields = list(map(lambda f: '`%s`' % f, fields))
        logging.info('escaped_fields:%s'% escaped_fields)

        attrs['__mappings__'] = mappings # 保存属性和列的映射关系
        attrs['__table__'] = tableName
        attrs['__primary_key__'] = primaryKey # 主键属性名
        attrs['__fields__'] = fields # 除主键外的属性名

        # 构造默认的SELECT, INSERT, UPDATE和DELETE语句:
        attrs['__select__'] = 'select `%s`, %s from `%s`' % (primaryKey, ', '.join(escaped_fields), tableName)
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (tableName, ', '.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)

        logging.info('  attrs:  %s' % attrs)
        return type.__new__(cls, name, bases, attrs)


# =====================================Model基类区==========================================


# 定义所有ORM映射的基类Model， 使他既可以像字典那样通过[]访问key值，也可以通过.访问key值
# 继承dict是为了使用方便，例如对象实例user['id']即可轻松通过UserModel去数据库获取到id
# 元类自然是为了封装我们之前写的具体的SQL处理函数，从数据库获取数据
# ORM映射基类,通过ModelMetaclass元类来构造类

class Model(dict, metaclass=ModelMetaclass):
    # 这里直接调用了Model的父类dict的初始化方法，把传入的关键字参数存入自身的dict中
    #__new__ implement before __init__
    def __init__(self, **kw):
        super(Model, self).__init__(**kw)

    # 获取dict的key
    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)

    # 设置dict的值的，通过d.k = v 的方式
    def __setattr__(self, key, value):
        self[key] = value

    # 获取某个具体的值即Value,如果不存在则返回None
    def getValue(self, key):
        # getattr(object, name[, default]) 根据name(属性名）返回属性值，默认为None
        return getattr(self, key, None)

    # 与上一个函数类似，但是如果这个属性与之对应的值为None时，就需要返回定义的默认值
    def getValueOrDefault(self, key):
        value = getattr(self, key, None)
        if value is None:
            # self.__mapping__在metaclass中，用于保存不同实例属性在Model基类中的映射关系
            # field是一个定义域!
            field = self.__mappings__[key]
            # 如果field存在default属性，那可以直接使用这个默认值
            if field.default is not None:
                # 如果field的default属性是callable(可被调用的)，就给value赋值它被调用后的值，如果不可被调用直接返回这个值
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s: %s' % (key, str(value)))
                # 把默认值设为这个属性的值
                setattr(self, key, value)
        return value

    # ==============往Model类添加类方法，就可以让所有子类调用类方法=================

    @ classmethod# 一般来说，要使用某个类的方法，需要先实例化一个对象再调用方法。这个装饰器是类方法的意思，即可以不创建实例直接调用类方法
    async def find(cls, pk):
        '''查找对象的主键'''
        # select函数之前定义过，这里传入了三个参数分别是之前定义的 sql、args、size
        rs = await select("%s where `%s`=?" % (cls.__select__, cls.__primary_key__), [pk], 1)
        if len(rs) == 0:
            return None
        return cls(**rs[0])

    # findAll() - 根据WHERE条件查找
    @classmethod
    async def findAll(cls, where=None, args=None, **kw):
        # sql语句不太会。。这里好像是添加了几个参数 where、args、OrderBy、limit
        sql = [cls.__select__]
        # 如果有where参数就在sql语句中添加字符串where和参数where
        if where:
            sql.append("where")
            sql.append(where)
        if args is None:  # 这个参数是在执行sql语句前嵌入到sql语句中的，如果为None则定义一个空的list
            args = []
        # 如果有OrderBy参数就在sql语句中添加字符串OrderBy和参数OrderBy，但是OrderBy是在关键字参数中定义的
        orderBy = kw.get("orderBy", None)
        if orderBy:
            sql.append("order by")
            sql.append(orderBy)
        limit = kw.get("limit", None)
        if limit is not None:
            sql.append("limit")
            if isinstance(limit, int):
                sql.append("?")
                args.append(limit)
            if isinstance(limit, tuple) and len(limit) == 2:
                sql.append("?,?")
                args.extend(limit)  # extend() 函数用于在列表末尾一次性追加另一个序列中的多个值（用新列表扩展原来的列表）。
            else:
                raise ValueError("错误的limit值：%s" % limit)
        rs = await select(" ".join(sql), args)
        return [cls(**r) for r in rs]

    # findNumber() - 根据WHERE条件查找，但返回的是整数，适用于select count(*)类型的SQL。
    @classmethod
    async def findNumber(cls, selectField, where=None, args=None):
        sql = ['select %s _num_ from `%s`' % (selectField, cls.__table__)]
        if where:
            sql.append("where")
            sql.append(where)
        rs = await select(" ".join(sql), args, 1)
        logging.info("以下是findNumber:rs  sql")
        logging.info(sql)
        # INFO:root:['select count(id) _num_ from `blogs`']
        logging.info(rs)
        # INFO:root:[{'_num_': 0}]
        if len(rs) == 0:
            return None
        return rs[0]['_num_']

    # ===============往Model类添加实例方法，就可以让所有子类调用实例方法===================

    # save、update、remove这三个方法需要管理员权限才能操作，所以不定义为类方法，需要创建实例之后才能调用
    async def save(self):
        args = list(map(self.getValueOrDefault, self.__fields__))  # 将除主键外的属性名添加到args这个列表中
        args.append(self.getValueOrDefault(self.__primary_key__))  # 再把主键添加到这个列表的最后
        rows = await execute(self.__insert__, args)
        if rows != 1:  # 插入纪录受影响的行数应该为1，如果不是1 那就错了
            logging.warn("无法插入纪录，受影响的行：%s" % rows)

    async def update(self):
        args = list(map(self.getValue, self.__fields__))
        args.append(self.getValue(self.__primary_key__))
        rows = await execute(self.__update__, args)
        if rows != 1:
            logging.warn('failed to update by primary key: affected rows: %s' % rows)

    async def remove(self):
        args = [self.getValue(self.__primary_key__)]
        rows = await execute(self.__delete__, args)
        if rows != 1:
            logging.warning('failed to remove by primary key: affected rows: %s' % rows)


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
        return '<类名，列的类型，列名:%s,%s:%s>' % (self.__class__.__name__, self.column_type, self.name)


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
