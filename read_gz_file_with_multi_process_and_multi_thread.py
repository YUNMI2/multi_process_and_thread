import os
import os.path
import gzip
import sys
import thread
import threading
import multiprocessing
from bs4 import BeautifulSoup
reload(sys)
sys.setdefaultencoding('utf8')

def read_gz_file(path):
    if os.path.exists(path):
        with gzip.open(path, 'r') as pf:
            for line in pf:
                yield line
    else:
        print('the path [{}] is not exist!'.format(path))

def readJsonfile(file_name):
	'''
	# load json file
	'''
	Json_dict = {}
	if file_name != "":
		import json
		with open(file_name, "r") as fo:
			Json_dict = json.load(fo)
	return Json_dict

def is_kong(list_in):
	for one in list_in:
		if not one.text.strip():
			return 1
	return 0


def extract_info_and_write(gz_file):
	'''
	# 按行读取.gz文件
	# 按照html标签筛选信息
	# 将筛选的信息写进文件
	'''
	con = read_gz_file(gz_file)
	num_index = 0

	out_file = gz_file.replace("gz","out_v2")
	tmp_file = gz_file.replace("gz","tmp_v2")

	if getattr(con, '__iter__', None):
		fw = open(out_file,"w")
		for line in con:
			num_index += 1
			if num_index%5 == 1:
				fo = open( tmp_file, "w")	
			
			fo.write(line)
			
			if num_index % 5 == 0:
				fo.close()
				dict_html = readJsonfile(tmp_file)
				soup = BeautifulSoup(dict_html["html"], 'html.parser')
				content_list = soup.find_all("div",attrs={"class":"para"})
				for content in content_list:
					key_list = content.find_all("a", attrs={'target':"_blank"})
					if len(key_list) > 0 and not is_kong(key_list):
						fw.write(content.text + "\n")
						for one in content.find_all("a", attrs={'target':"_blank"}):
							fw.write(one.text + "\n")
						fw.write("\n\n")
						
	if os.path.exists(tmp_file):
		os.remove(tmp_file) #删除临时文件
	fw.close()

def load_dir_file(dir_path,list_file_name):
	'''
	# 递归读取一个文件夹下面的.gz文件
	'''
	parents = os.listdir(dir_path)
	for parent in parents:
		child = os.path.join(dir_path,parent)
		if os.path.isdir(child):
			load_dir_file(child,list_file_name)
		else:
			if ".gz" in child and child.endswith(".gz"):
				list_file_name.append(child)

			
def deal_with_multi_thread(list_file):
	'''
	# 多线程，每个文件一个线程，python多线程只能使用一个cpu
	'''
	threads = []
	for one_file in list_file:
		print "start reading file: ", one_file
		one_thread = threading.Thread(target=extract_info_and_write,args=(one_file,))
		threads.append(one_thread)
	for one_thread in threads:
		one_thread.start()
	for one_thread in threads:
		one_thread.join()
 


def deal_with_multi_process(list_file, num_thread_in_one_process):
	'''
	# 多进程，使用多核cpu，因为python 多线程只能使用一个cpu，效率不高
	'''
	processs = []
	
	groups = []
	
	one_group = []
	
	for i in range(len(list_file)):
		one_group.append(list_file[i])
		if i % num_thread_in_one_process == num_thread_in_one_process-1:
			if one_group != []:
				groups.append(one_group)
			one_group = []
	if one_group != []:
		groups.append(one_group)
	
	for one_group in groups:
		each_process = multiprocessing.Process(target=deal_with_multi_thread,args=(one_group,))
		processs.append(each_process)
	
	for one_process in processs:
		one_process.start()
	
	for one_process in processs:
		one_process.join()
	



if __name__ == "__main__":
	list_file_name = []
	load_dir_file("./",list_file_name)
	deal_with_multi_process(list_file_name,5)	

