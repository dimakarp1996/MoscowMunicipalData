# -*- coding: utf-8 -*-
"""
Created on Tue Sep  8 13:27:39 2020

@author: DK
"""

import os
import pandas as pd
from collections import defaultdict
from tqdm import tqdm
import numpy as np

os.chdir('C://Downloads')


def parse_population(population_file):
    population = pd.read_excel(population_file, header=5)
    population = population.dropna()
    names = list(population[population.columns[1]])
    pop = list(population[population.columns[2]])
    keys = list(population[population.columns[0]])
    for i in range(len(names)):
        if type(names[i]) == str:
            names[i] = names[i].strip()
            names[i] = names[i].replace('г. Москва - город федерального значения', 'г. Москва')
    start_index1 = list(names).index('Московская область')
    end_index1 = list(names).index('Сельское поселение Трубинское')
    start_index2 = list(names).index('г. Москва')
    end_index2 = list(names).index('Чертаново Южное')
    names = names[start_index1:end_index1] + names[start_index2:end_index2]
    pop = pop[start_index1:end_index1] + pop[start_index2:end_index2]
    keys = keys[start_index1:end_index1] + keys[start_index2:end_index2]
    keys = [str(j)[:8] for j in keys]
    return names, pop, keys


def parse_declaration(full_dir):
    taxes = pd.read_excel(full_dir)
    key = int(taxes[taxes.columns[0]][3].split(':')[1].split(',')[0].strip())
    names = taxes[taxes.columns[0]]
    income_start_inds = names[names == 'по коду дохода 2000'].index
    income_end_inds = names[names == 'по коду дохода 3010'].index
    num_workers = taxes[taxes.columns[3]][income_start_inds[0]]
    num_russian_workers = taxes[taxes.columns[4]][income_start_inds[0]]
    num_foreign_workers = taxes[taxes.columns[5]][income_start_inds[0]]
    if type(num_foreign_workers) == str:
        num_foreign_workers = 0
    if type(num_workers) == str:
        num_workers = taxes[taxes.columns[2]][income_start_inds[0]]
        num_russian_workers = num_workers
        num_foreign_workers = 0
    labor_tax_sums = taxes[income_start_inds[1]:income_end_inds[1]]
    num_months = 12  # число месяцев в году

    all_people_wages = labor_tax_sums[labor_tax_sums.columns[2]]
    avg_income = sum([j for j in all_people_wages if type(j) != str]) / (num_months * (num_workers + 1e-10))

    russian_people_wages = labor_tax_sums[labor_tax_sums.columns[3]]
    russian_income = sum([j for j in russian_people_wages if type(j) != str]) / (
        num_months * (num_russian_workers + 1e-10))

    foreigner_people_wages = labor_tax_sums[labor_tax_sums.columns[4]]
    foreigner_income = sum([j for j in foreigner_people_wages if type(j) != str]) / (
        num_months * (num_foreign_workers + 1e-10))
    key = str(key)
    if key == '77':
        key = '45000000'
    elif key == '50':
        key = '46000000'
    return key, [num_workers, num_russian_workers, num_foreign_workers,
                 avg_income, russian_income, foreigner_income]


def process(directory):
    tax_data = defaultdict(list)
    for file in tqdm(os.listdir(directory)):
        if '$' not in file:  # skip cached files
            full_dir = directory + '/' + file
            key, data = parse_declaration(full_dir)
            if key == '46000000':
                print(full_dir)
            if (key not in tax_data and not np.isnan(data).any()) or data[
                0] > 100000:  # костыль для исправления бага с муниципалитетв=ами
                tax_data[key] = data
    return tax_data


names1, pop1, keys1 = parse_population('2016_data/population.xls')
names2, pop2, keys2 = parse_population('2018_data/population.xlsx')

tax_data1 = process('2016_data/tax_by_district')

tax_data2 = process('2018_data/tax_by_district')
# Замену ОКТМО оставляем за скобкой и территории с замененным ОКТМО не учитываем, если кто желает, может добавить новые ОКТМО
all_keys = set(keys1) & set(keys2) & set(tax_data1.keys()) & set(tax_data2.keys())

moscow_keys = [j for j in all_keys if '45' == j[:2]]
region_keys = [j for j in all_keys if '45' != j[:2]]

moscow_data = defaultdict(list)
region_data = defaultdict(list)
for i, key1 in enumerate(keys1):
    if keys1[i] in keys2:
        j = keys2.index(keys1[i])
        name = names1[i]
        if key1 in moscow_keys:
            try:
                moscow_data[name] = [int(key1)] + [pop1[i]] + tax_data1[key1] + [pop2[j]] + tax_data2[key1]
                moscow_data[name] = [int(j) for j in moscow_data[name]]
            except:
                pass
        elif key1 in region_keys:
            try:
                region_data[name] = [int(key1)] + [pop1[i]] + tax_data1[key1] + [pop2[i]] + tax_data2[key1]
                region_data[name] = [int(j) for j in region_data[name]]
            except:
                pass


def final_process_data(data_dict):
    frame = pd.DataFrame(data_dict)
    frame.index = ['ОКТМО', 'Население 2016', 'Число работников 2016', 'Число работников-россиян 2016',
                   'Число работников-иностранцев 2016',
                   'Средняя белая зарплата 2016', 'Средняя белая зарплата россиян 2016',
                   'Средняя белая зарплата иностранцев 2016',
                   'Население 2018', 'Число работников 2018', 'Число работников-россиян 2018',
                   'Число работников-иностранцев 2018',
                   'Средняя белая зарплата 2018', 'Средняя белая зарплата россиян 2018',
                   'Средняя белая зарплата иностранцев 2018']
    frame = frame.transpose()
    frame['Доля иностранных работников 2016, %'] = 100 * (
        (frame['Число работников-иностранцев 2016'] / frame['Число работников 2016']))
    frame['Доля иностранных работников 2018, %'] = 100 * (
        (frame['Число работников-иностранцев 2018'] / frame['Число работников 2018']))
    frame['Темпы роста числа иностранных работников 2018-2016'] = 100 * (
        (frame['Число работников-иностранцев 2018'] / frame['Число работников-иностранцев 2016']) - 1)
    frame['Темпы роста доли иностранных работников 2018-2016, %'] = 100 * (
        (frame['Доля иностранных работников 2018, %'] / frame['Доля иностранных работников 2016, %']) - 1)
    frame['Темпы роста населения 2018-2016, %'] = 100 * ((frame['Население 2018'] / frame['Население 2016']) - 1)
    frame['Темпы роста числа рабочих мест 2018-2016, %'] = 100 * (
        (frame['Число работников 2018'] / frame['Число работников 2016']) - 1)
    frame['Темпы роста средней белой зарплаты 2018-2016, %'] = 100 * (
        (frame['Средняя белая зарплата 2018'] / frame['Средняя белая зарплата 2016']) - 1)
    frame['Средняя белая зарплата иностранцев % от российской 2016'] = 100 * frame[
        'Средняя белая зарплата иностранцев 2016'] / frame['Средняя белая зарплата 2016']
    frame['Средняя белая зарплата иностранцев % от российской 2018'] = 100 * frame[
        'Средняя белая зарплата иностранцев 2018'] / frame['Средняя белая зарплата 2018']
    frame['Число рабочих мест в % к численности населения 2016'] = 100 * frame['Число работников 2016'] / frame[
        'Население 2016']
    frame['Число рабочих мест в % к численности населения 2018'] = 100 * frame['Число работников 2018'] / frame[
        'Население 2018']
    frame['Темпы роста доли рабочих мест 2016-2018, %'] = 100 * ((frame[
                                                                      'Число рабочих мест в % к численности населения 2018'] /
                                                                  frame[
                                                                      'Число рабочих мест в % к численности населения 2016']) - 1)
    return frame


moscow_data = final_process_data(moscow_data)
region_data = final_process_data(region_data)
moscow_data.to_csv('moscow_data.csv')
region_data.to_csv('region_data.csv')

# Ломоносовский Москва ОКТМО. Плюс вс
