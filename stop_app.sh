kill `cat pid`
if [ $? -eq 0 ]; then
  echo "算法已经停止"
else
  echo "算法终止失败,请检查是否已经启动,或使用ps+kill手动终止进程"
fi