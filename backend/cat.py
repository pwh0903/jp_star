import os
import shutil
import re

movie_path = './download'
new_path = './download1'

movies = os.listdir(movie_path)
for movie in movies:
    l1_path = movie[0]
    if l1_path in [str(i) for i in range(10)]:
        l2_path = ''
    else:
        l2_path = re.findall('[a-zA-Z]+', movie)[0]
    old_path = os.path.join(movie_path, movie)
    l1_path = os.path.join(new_path, l1_path)
    l2_path = os.path.join(l1_path, l2_path)
    if not os.path.isdir(l1_path):
        os.mkdir(l1_path)
    if not os.path.isdir(l2_path):
        os.mkdir(l2_path)
    shutil.move(old_path, l2_path)