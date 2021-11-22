# 1. python环境准备

在Linux系统中，系统预装了python2，但算法是依赖python3的，因此，首先需要安装python3环境。

为了不与系统的python2环境发生冲突，推荐安装Miniconda。下载地址为：
```
https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
```

MD5校验码为：
```
122c8c9beb51e124ab32a0fa6426c656
```

**注意：**

python的版本必须是python3.8，为此，Conda的版本也必须是对应的，上述链接可能在将来会对应更高版本的python，因此，必须保证校验码的一致性。

历史版本的Miniconda可以在[此处](https://repo.anaconda.com/miniconda/) 找到。


下载完安装文件后，执行`sh Miniconda3-latest-Linux-x86_64.sh`进行安装，安装完成后可能需要重启shell或者重新导入配置文件：
```
source ~/.bashrc
```

# 2. 依赖包的安装

全部依赖包定义在`requirements.txt`文件中，只需要执行`pip install -r requirements.txt`命令即可安装成功。

**注意 1：**

如果在无网络环境下进行配置, 有以下三种方式:

1.首先将全部依赖下载并解压到代码根目录. 

下载地址为:链接: https://pan.baidu.com/s/1X2TMlDDVrziHxKVZ-PY1xw 提取码: t5sh 

解压后的文件夹名称应为`pkgs`,然后在代码根目录执行`python setup.py`即可自动安装所有依赖。

> 此种方法可能存在滞后性,例如,无法安装支持SQL Server的驱动. 为了保证时效性,建议采取下面的方法安装.


2.在虚拟机中创建与目标服务器相同的操作系统，按照上述步骤安装conda，然后利用conda创建一个虚拟环境，在虚拟环境中安装好对应的依赖包，最后将整个虚拟环境拷贝到目标服务器上。

3.利用`pip`将待安装的包编译成whl文件，然后将全部文件拷贝至目标服务器进行安装，参考[这里](https://blog.csdn.net/SunJW_2017/article/details/103222205) 。

**注意 2：**

如果涉及到Oracle数据库，那么可能需要安装对应的驱动文件，参考文章在[这里](https://blog.csdn.net/SunJW_2017/article/details/118152349) ，下载地址在[这里](https://www.oracle.com/database/technologies/instant-client/linux-x86-64-downloads.html) 。

# 3. 服务启动与终止

正式环境中推荐使用`gunicorn`进行启动，算法已经进行了封装，启动命令为：
```cmd
sh start_app.sh
```

算法终止命令:
```cmd
sh stop_app.sh
```

如果要实现对进程的动态监控，推荐使用`supervisor`，使用方法参考[这里](https://blog.csdn.net/SunJW_2017/article/details/114533853) 。

# 4. 本地测试
想要执行本地测试，打开`test.py`修改对应的数据源信息，然后执行`python test.py`。

# 5. 参数释义
算法可调节的参数分为两部分:一部分在配置文件`config.py`中; 一部分通过前端调用界面进行配置.

## 5.1 `config.py`中的参数
> 注意:修改`config.py`中的参数后,需要**重启** 整个算法服务才能生效;且修改时必须**符合python语法规范** .

- `both_roles`: 布尔值,取值为`True`或`False`,表示一个字段是或否可以既被其他字段引用,又可以引用其他字段.
- `not_base_table`: 取值为`None`或一个由表名组成的列表,如果是列表,则该列表中的表不会作为被引用表.
- `not_cite_table`: 取值与`not_base_table`相同,如果是列表,则该列表中的表不会引用其他表.
- `sup_out_foreign_key`: 浮点数,取值范围为`[0,1)`,表明一个字段(引用字段)引用另一个字段(被引用字段)时,允许引用字段中存在的不在被引用字段中的值所占引用字段的值总数的百分比.默认为0,即引用字段中的值都必须存在于被引用字段中.
- `multi_process`: 取值为`0`/`1`或一个正整数.取值为`0`表明不启动多进程(默认);取值为`1`表明启用多进程,且进程数与cpu核数相等;其他整数则指定了进程的数量.
- `mysql_type_list`及后续: 许可进行关系计算的字段的数据类型,根据不同的数据库进行设置.目前仅支持配置`MySQL`,`Oracle`与`PostgreSQL`.

## 5.2 前端界面配置
1. `data_cleansing`: 值必须符合python中字典的语法.该值通过字段名称来对该字段是否可以用于计算表关系做了更加细致的约束.该值的格式如下:
```text
{
    "_": ["str1", "str2", ...],
    "tab1": ["tab1_str1", "tab1_str2", ...],
    ...
}
```
键"_"对应的值（规则）适用于所有表；其他的键对应的值（规则）只适用于对应表。

所有的值（规则）均是字符串，并且只有两种格式：以"%"结尾，或不以"%"结尾。以"%"结尾表示col必须与该规则完全一致才会被过滤；否则，只要col以该规则字符串开头即被过滤。

2. `computeStringLength`: 取值为`true`或`false`(默认),表示是否利用某字段中的值的字符长度来决定是否计算该字段的关系.如果设置为`true`,需要配合下一个参数一起使用.
3. `infStringLength`: 仅当`computeStringLength`为`true`时才有效,表示如果一个字段的值的平均字符长度小于该值,则该字段不会被计算关系.
4. `infDuplicateRatio`: 浮点数,取值范围为`[0,1)`,允许的该字段中重复值所占的比例,超过该比例的值将不会被计算关系.
5. `infTableLength`: 正整数,表示可用于计算关系的表的最小长度.如果表的行数低于该值,则该表将不会被计算关系.
