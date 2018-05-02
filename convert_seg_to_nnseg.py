#!/usr/bin/python
# -*- coding: UTF-8 -*-

import os
import os.path
import gzip
import sys
import threading
import multiprocessing
import codecs

reload(sys)
sys.setdefaultencoding('utf8')

def Attach_Tag(cur_char,pre_char,next_char):
    assert (cur_char != " ")

    if pre_char == " " or pre_char == "#START#":
        if next_char == "#END#" or next_char == " ":
            return "s-seg"
        else:
            return "b-seg"
    else:
        if next_char == "#END#" or next_char == " ":
            return "e-seg"
        else:
            return "m-seg"

def Get_Bichar(cur_char, pre_char, pre_two_char,line,file_name):
    prefix_tag = "[T1]"
    bichar = cur_char
    assert(cur_char != " ")

    if pre_char == " ":
        assert (pre_two_char != " ")
        bichar += pre_two_char
    else:
        bichar += pre_char
    return prefix_tag + bichar

def extract_info_and_write(file_name):
    file_name_out = file_name.replace("hwc.seg","bichar.feats")
    with codecs.open(file_name, "r",encoding="utf-8") as fo:
        with codecs.open(file_name_out, "w", encoding="utf-8") as fw:
            for line in fo.readlines():
                line = line.strip()
                if not line:
                    continue
                line = " ".join(line.split()) # 语料里面不是严格意义的每个词之间是一个空格，先split然后join一个空格
                for i in range(len(line)):
                    if line[i] != " ":
                        cur_char = line[i]
                        pre_char = line[i-1] if i > 0 else "#START#"
                        pre_two_char = line[i-2] if i > 1 else "#START#"
                        next_char = line[i+1] if i < len(line)-1 else "#END#"
                        cur_char_tag = Attach_Tag(cur_char,pre_char,next_char)
                        cur_bichar = Get_Bichar(cur_char,pre_char,pre_two_char,line,file_name)
                        fw.write(" ".join([cur_char,cur_bichar,cur_char_tag]) + "\n")
                fw.write("\n")
    print file_name,"end reading!"


def load_dir_file(dir_path, list_file_name):
    parents = os.listdir(dir_path)

    for parent in parents:
        child = os.path.join(dir_path, parent)
        if os.path.isdir(child):
            load_dir_file(child, list_file_name)
        else:
            if  child.endswith(".hwc.seg"):
                list_file_name.append(child)


def deal_with_multi_thread(list_file):
    threads = []

    for one_file in list_file:
        print "start reading file: ", one_file
        one_thread = threading.Thread(target=extract_info_and_write, args=(one_file,))
        threads.append(one_thread)

    for one_thread in threads:
        one_thread.start()

    for one_thread in threads:
        one_thread.join()


def deal_with_multi_process(list_file, num_thread_in_one_process):
    processs = []
    groups = []
    one_group = []

    for i in range(len(list_file)):
        one_group.append(list_file[i])
        if i % num_thread_in_one_process == num_thread_in_one_process - 1:
            if one_group != []:
                groups.append(one_group)
            one_group = []
    if one_group != []:
        groups.append(one_group)

    for one_group in groups:
        each_process = multiprocessing.Process(target=deal_with_multi_thread, args=(one_group,))
        processs.append(each_process)

    for one_process in processs:
        one_process.start()

    for one_process in processs:
        one_process.join()


if __name__ == "__main__":
	'''
	input file: 我 是 中国人
	output file:
	我 [T1]我#START# s-seg
	是 [T1]是我 s-seg
	中 [T1]中是 b-seg
    国 [T1]国中 m-seg
	人 [T1]人国 e-seg 
	'''
    list_file_name = []
    load_dir_file("./", list_file_name)
    deal_with_multi_process(list_file_name, 5)

