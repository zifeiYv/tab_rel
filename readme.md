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

**注意：**

如果无网络环境下进行配置环境，推荐以下两种做法：

1. 在虚拟机中创建与目标服务器相同的操作系统，按照上述步骤安装conda，然后利用conda创建一个虚拟环境，在虚拟环境中安装好对应的依赖包，最后将整个虚拟环境拷贝到目标服务器上。

2. 利用`pip`将待安装的包编译成whl文件，然后将全部文件拷贝至目标服务器进行安装，参考[这里](https://blog.csdn.net/SunJW_2017/article/details/103222205) 。

3. 如果涉及到Oracle数据库，那么可能需要安装对应的驱动文件，下载地址在[这里](https://www.oracle.com/database/technologies/instant-client/linux-x86-64-downloads.html) 。

# 3. 服务启动

正式环境中推荐使用`gunicorn`进行启动，启动命令为：
```cmd
gunicorn -c gunicorn_config.py app:app
```

如果要实现对进程的动态监控，推荐使用`supervisor`，使用方法参考[这里](https://blog.csdn.net/SunJW_2017/article/details/114533853) 。