gunicorn -c gunicorn_config.py tab_rel_app:app
if [ $? -eq 0 ]; then
  echo "启动成功"
else
  echo "算法启动失败,请检查配置文件"
fi