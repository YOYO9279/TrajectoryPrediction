import os
from urllib.parse import unquote

from tqdm import tqdm

filePath = "/root/data/track/2018/10/10/"
# filePath = "C:\\Users\\YOYO\\Downloads\\track\\2019\\07\\02\\"

list_data = os.listdir(filePath)

k = open('/root/data/res/20181010.txt', 'w+')

for i in tqdm(list_data):
    length = len(i)
    append = ''
    if length < 19:
        color = str(i[0:1])
        province = unquote(str(i[2:9]), encoding="GB2312")
        car_number = str(i[9:14])
        append = ':{}:{}:{}\n'.format(color, province, car_number)
    else:
        color = str(i[0:1])
        province = unquote(str(i[2:9]), encoding="GB2312")
        car_number = str(i[9:13]) + unquote(str(i[13:-4]), encoding="GB2312")
        append = ':{}:{}:{}\n'.format(color, province, car_number)
    # print(append)
    with open(filePath + i, 'r', encoding="utf-8") as f:
        lines = f.readlines()
        for line in lines:
            new_line = line.replace('\n', append)
            k.write(new_line)
    f.close()
k.close()
print("ok")
