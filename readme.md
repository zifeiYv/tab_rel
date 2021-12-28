# 1、基础环境要求

操作系统：`Linux`(CentOS6.5及以上经过了完全的测试，以下安装说明均假定此环境)

Python版本：`3.7.x`，推荐安装`Anaconda3`

# 2、python依赖安装

## 2.1 安装

所有的第三方依赖均写在根目录下的`requirements.txt`文件中，根据网络条件，任选以下一种方式进行安装：

1. **有网络时（推荐）**，在根目录下执行`pip install -r requirements.txt`即可批量安装（可能会要求环境中必须安装`gcc`，如果没有，通过`yum install gcc`进行安装）。
2. 无网络时，需要批量下载所有的第三方依赖（链接: https://pan.baidu.com/s/1Gxdw0Dqx6DvtjE5vLDDzBw 提取码: 94sw）并上传至服务器的某个路径（如`~/home`)，进入解压后的目录（`~/home/whl`）并执行`pip install --no-index --find-links=. -r requirements.txt `即可完成批量安装。

## 2.2 安装后的操作

用于Gbase 8a的驱动需要手动进行安装，步骤如下：

1. 在解压后的目录执行`tar -vf GBaseConnector-1.0.0.tar.gz && cd GBaseConnector`；
2. 执行`python setup.py install`。

# 3、配置修改

根目录下的`config.py`中包含了所有的配置项，建议用户只需要修改`multi_process`与`hive_ip/presto_port`，其他保持现状即可。


# 4、项目启停

执行`sh start_app.sh`启动项目；执行`sh stop_app.sh`终止项目。

>  如果修改了`config.py`中的配置，需要重启项目。

