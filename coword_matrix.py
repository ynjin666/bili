import numpy as np
import csv
import xlrd
import pandas as pd


def build_worddict(fullsample, featureword):
    text_dict = {word: set() for word in featureword}
    print(f'已建立好原文本字典的空集\r')
    current = 0
    for singleline in fullsample:
        word_list = singleline.split()
        for word in word_list:
            if word in featureword:
                text_dict[word].add(fullsample.index(singleline))
        current += 1


    return text_dict


def build_matrix(featureword):
    edge = len(featureword) + 1
    matrix = np.empty((edge, edge), dtype=str)
    matrix = [['' for j in range(edge)] for i in range(edge)]  # 初始化矩阵
    matrix[0][1:] = np.array(featureword)
    matrix = list(map(list, zip(*matrix)))
    matrix[0][1:] = np.array(featureword)

    return matrix


def count_matrix(matrix, text_dict):


    for row in range(1, len(matrix)):
        for column in range(1, len(matrix)):  # 跳过关键词所在的第一列第一行

            if matrix[0][row] == matrix[column][0]:
                matrix[row][column] = 0  # 对角线上的值为0


            else:
                word1 = matrix[0][row]
                word2 = matrix[column][0]
                set1 = text_dict[word1]
                set2 = text_dict[word2]
                times = len(set1&set2)
                matrix[column][row] = times
                matrix[row][column] = times


    return matrix


def main():
    rank_file = f'D:/country_cipin/qingxi.xlsx'  # 词频文件路径
    data = xlrd.open_workbook(rank_file)
    table = data.sheet_by_name('Sheet1')
    featureword = table.col_values(1)
    # featureword=table.col_values(0)[0:50]
    fullsample_file = f'D:/country_cipin/demo_afterfenci.txt'  # 样本全文路径
    common_matrix_file = f'D:/country_cipin/demo.csv'

    with open(fullsample_file, 'r', encoding='utf-8') as f:
        fullsample = f.readlines()
        print('已读取到样本全文\n')

    text_dict = build_worddict(fullsample, featureword)
    print('已创建原文本字典\n')

    have_matrix = build_matrix(featureword)
    print('已创建共词矩阵\n')
    matrix = count_matrix(have_matrix, text_dict)

    print('共词矩阵计算完毕！\n')
    print('开始输出结果...\n')

    with open(common_matrix_file, 'w', newline='', encoding='UTF-8-sig') as csvfile:
        writer = csv.writer(csvfile)
        for row in matrix:
            writer.writerow(row)

    print('恭喜，保存完成！')


main()
