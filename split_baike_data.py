#!/usr/bin/python
# -*- coding: UTF-8 -*-

import os
import os.path
import sys
import threading
import multiprocessing
import codecs

reload(sys)
sys.setdefaultencoding('utf8')


def split_baike_data_to_2_kinds(file_name = ""):
    file_name_normal = file_name + ".normal"
    file_name_unnormal = file_name + ".unnormal"
    num_normal_sen = 0
    num_normal_word = 0
    num_unnormal_sen = 0
    num_unnormal_word = 0
    with codecs.open(file_name, "r", encoding="utf-8") as fo:
        with codecs.open(file_name_normal, "w", encoding="utf-8") as fw_normal:
            with codecs.open(file_name_unnormal, "w", encoding="utf-8") as fw_unnormal:
                list_sen_and_word = []
                flag_normal = True
                for line in fo.readlines():
                    if line.startswith("sentence:") and list_sen_and_word != []:
                        for i in range(len(list_sen_and_word)):
                            if "- Data © NavInfo & CenNavi & 道道通" in list_sen_and_word[i] or "张)" in list_sen_and_word[i] : #表示有地图的或者有剧照的
                                flag_normal = False
                                break
                        if flag_normal:
                            start_ix = 0
                            end_ix = 0
                            for i in range(len(list_sen_and_word)):
                                if list_sen_and_word[i].startswith("key word:"):
                                    end_ix = i
                                    break
                            sen = ""
                            while start_ix < end_ix:
                                sen += list_sen_and_word[start_ix].strip()
                                start_ix += 1
                            fw_normal.write(sen + "\n")
                            num_normal_sen += 1
                            while end_ix < len(list_sen_and_word):
                                if list_sen_and_word[end_ix].strip() and list_sen_and_word[end_ix].strip() != "key word:":
                                    fw_normal.write(list_sen_and_word[end_ix].strip() + "\n")
                                    num_normal_word += 1
                                end_ix += 1
                            fw_normal.write("\n")

                        else:
                            for one in list_sen_and_word:
                                fw_unnormal.write(one)
                                if one.startswith("sentence:"):
                                    num_unnormal_sen += 1
                                elif one.strip().startswith("key word:"):
                                    num_unnormal_word += 1
                            fw_unnormal.write("\n")

                        list_sen_and_word = [line]
                        flag_normal = True
                    else:
                        list_sen_and_word.append(line)
    print "\n"
    print file_name, "static info:"
    print "\t","num_normal_sen: ",num_normal_sen
    print "\t", "num_normal_word: ", num_normal_word
    print "\t", "num_unnormal_sen: ", num_unnormal_sen
    print "\t", "num_unnormal_word: ", num_unnormal_word
    print "end reading!"
    print "\n"


def load_dir_file(dir_path, list_file_name):
    parents = os.listdir(dir_path)

    for parent in parents:
        child = os.path.join(dir_path, parent)
        if os.path.isdir(child):
            load_dir_file(child, list_file_name)
        else:
            if  child.endswith(".json.out_v2"):
                list_file_name.append(child)


def deal_with_multi_thread(list_file):
    threads = []

    for one_file in list_file:
        print "start reading file: ", one_file
        one_thread = threading.Thread(target=split_baike_data_to_2_kinds, args=(one_file,))
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
    list_file_name = []
    stat_info_list = []
    load_dir_file("./", list_file_name)
    deal_with_multi_process(list_file_name, 2)

