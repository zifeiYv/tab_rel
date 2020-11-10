# -*- coding: utf-8 -*-
# 根据日志统计各阶段耗费时间
f = open('5e45865df65b4743bd184423ab571854.log')
records = 0
fiedls = 0
time = 0.0

while True:
    i = f.readline()
    if not i:
        break
    # if 'INFO main_utils.py line  617 |' in i:
    #     start = i.index('# records:') + 10
    #     end = i.index('# fields:')
    #     records += int(i[start: end])

    #     start = i.index('# fields:') + 9
    #     end = i.index('run time:')
    #     fiedls += int(i[start: end])

    #     start = end + 9
    #     time += float(i[start: ])
    if 'INFO main_utils.py line  690 |' in i:
        start = i.index('cost ') + 5
        s = i[start: ]
        time += float(s[: -3])
f.close()
print(records)
print(fiedls)
print(time)
