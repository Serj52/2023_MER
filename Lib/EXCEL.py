import logging
import openpyxl
from CONFIG import Config as cfg
import json
import os
import re
from datetime import datetime
import time
from Lib import EXCEPTION_HANDLER
from openpyxl.styles.borders import Border, Side

BEGIN_ROW_PATTERN = 10
BORDER = Border(left=Side(style='thin'),
                right=Side(style='thin'),
                top=Side(style='thin'),
                bottom=Side(style='thin'))



class Excel:

    def init_forecast(self, task_id, forecast, date_upload):
        """
        Инициализация параметров
        :param task_id: id запроса
        :param forecast: имя прогноза
        """
        self.result = {}
        self.ignore_list = []
        self.deflator_razdel_C = ''
        self.icp_razdel_C = ''
        self.code_list = []
        self.forecast = None
        self.code_list = ['18', '31', '32', '34']
        self.forecast = None

        with open(cfg.file_data, mode='r', encoding='utf-8') as file:
            self.result = json.load(file)

    def task_kill_excel(self):
        """
        Убиваем все открытые процессы Excel
        """
        logging.info('Закрываю Excel')
        logging.info('Close excel.exe')
        os.system("2>nul taskkill /f /t /im excel.exe")
        time.sleep(2)

    def file_processing(self, exctract_dir,
                   forecast, task_id, date_upload):
        """
        Функция запись excel в dict
        :param exctract_dir: директория с извлеченными файлами прогнозов
        :param forecast: имя прогноза
        :param task_id: id запроса
        :return:
        """
        self.init_forecast(task_id, forecast, date_upload)
        self.forecast = 'long' if 'long' in forecast else 'short'
        logging.info(f'Обрабатываю {forecast}')
        for workbook in os.listdir(exctract_dir):
            if os.path.isfile(fr'{exctract_dir}/{workbook}') and 'xlsx' in workbook:
                logging.info(f'Начинаю обработку файла {workbook}')
                workbook_path = os.path.join(exctract_dir, workbook)
                excel_book = openpyxl.load_workbook(workbook_path, data_only=True)
                sheets = excel_book.worksheets
                # Выбираем шаблон обработки
                if self.forecast == 'long':
                    self.long_forecast(workbook, sheets)
                elif self.forecast == 'short':
                    self.short_forecast(workbook, sheets)
                logging.info(f'Обработка файла {workbook} завершена')
        json.dump(self.result, open(os.path.join(exctract_dir, 'file_data.json'), mode='w',
                                    encoding='utf-8'), indent=4, ensure_ascii=False)


    def short_forecast(self, workbook, sheets):
        """
        Функция обработки краткосрочных прогнозов
        :param workbook: имена файлов с прогнозами
        :param sheets: листы файлов с прогнозами
        :return:
        """
        if 'Дефлятор' in workbook:
            for sheet in sheets:
                if 'дефлятор' in sheet.title.lower() or 'год' in sheet.title.lower():
                    logging.info(f'Обрабатываю лист {sheet.title}')
                    self.deflator_handler(sheet)
                    for sheet in sheets:
                        if 'кварт' in sheet.title.lower():
                            #Лист кварт обрабатываем только после Листа Дефлятор
                            logging.info(f'Обрабатываю лист {sheet.title}')
                            self.quarter_handler(sheet)
                            return
                    logging.error(f'В файле {workbook} нет листа импорт')
                    raise EXCEPTION_HANDLER.FileProcessError(f'В файле {workbook} нет листа импорт')
                else:
                    continue
            logging.error(f'В файле {workbook} нет листа Дефлятор')
            raise EXCEPTION_HANDLER.FileProcessError(f'В файле {workbook} нет листа Дефлятор')
        elif 'Внешняя торговля' in workbook:
            for sheet in sheets:
                if 'импорт' in sheet.title.lower():
                    logging.info(f'Обрабатываю лист {sheet.title}')
                    self.import_handler(sheet)
                    return
            logging.error(f'В файле {workbook} нет листа импорт')
            raise EXCEPTION_HANDLER.FileProcessError(f'В файле {workbook} нет листа импорт')
        elif 'ИПЦ базовый' in workbook:
            for sheet in sheets:
                if 'ипц' in sheet.title.lower():
                    logging.info(f'Обрабатываю лист {sheet.title}')
                    self.ipc_service_handler(sheet)
                    return

    def ipc_service_handler(self, sheet):
        start_cell = self.search_start_cell(sheet, 'ipc')
        years = self.search_years(sheet, 'ipc')
        for row in range(start_cell['row'], sheet.max_row + 1):
            value = sheet.cell(row=row, column=start_cell['column']).value
            if 'услуги' in value.lower():
                for row in range(row, sheet.max_row + 1):
                    value = sheet.cell(row=row, column=start_cell['column']).value
                    if 'рост цен на конец периода, % к декабрю предыдущего года' in value:
                        for year, index_column in years.items():
                            dict = {}
                            dict["date"] = f'{year}-01-01T00:00:00'
                            dict["IPC"] = round(sheet.cell(row=row, column=index_column).value, 1)
                            self.result["service"].append(dict)
                        return
                raise EXCEPTION_HANDLER.FileProcessError(
                    f'На листе {sheet} не найдена строка "рост цен на конец периода, % к декабрю предыдущего года"')
        raise EXCEPTION_HANDLER.FileProcessError(
            f'На листе {sheet} не найдена строка "Услуги"')


    def long_forecast(self, workbook, sheets):
        """
        Функция обработки долгосрочных прогнозов
        :param workbook: имена файлов с прогнозами
        :param sheets: объекты Worksheet
        :return:
        """
        if 'Экспорт-импорт' in workbook:
            for sheet in sheets:
                if 'print imp' in sheet.title.lower():
                    logging.info(f'Обрабатываю лист {sheet.title}')
                    self.import_handler(sheet)
                    break
        elif 'Дефлятор' in workbook:
            #Предполагаеися, что лист будет один. Название меняется
            for sheet in sheets:
                logging.info(f'Обрабатываю лист {sheet.title}')
                self.deflator_handler(sheet)
        elif 'ИПЦ базовый' in workbook:
            for sheet in sheets:
                if 'ипц' in sheet.title.lower():
                    logging.info(f'Обрабатываю лист {sheet.title}')
                    self.ipc_service_handler(sheet)
                    return

    def search_start_cell(self, sheet, file_name):
        """
        Функция поиска начальной ячейки
        :param sheet: объект Worksheet
        :param file_name: имя обрабатываемого файла
        :return: словарик с индексом строки и столбца ячейки
        """
        logging.info(f'Выполняю поиск начальной строки')
        text_row = ''
        end_column = sheet.max_column
        end_row = sheet.max_row
        if file_name == 'deflator':
            text_row = '(bcde)'
        elif file_name == 'import':
            text_row = 'наименование товарной группы'
        elif file_name == 'ipc':
            text_row = 'показатели инфляции'
        for column in range(1, end_column + 1):
            for row in range(1, end_row + 1):
                value = sheet.cell(row=row, column=column).value
                if value is None:
                    continue
                elif text_row in value.lower():
                    return {'row': row, 'column': column}
                else:
                    continue
        logging.error(f'На листе {sheet.title} не найдена начальная строка')
        raise EXCEPTION_HANDLER.FileProcessError(f'На листе {sheet.title} не найдена начальная строка')

    def search_end_cell(self, sheet, start_cell: dict, file_name):
        """
        Функция поиска последней строки
        :param sheet: объект Worksheet
        :param start_cell: словарик с индексом строки и столбца начальной ячейки
        :param file_name: имя обрабатываемого файла
        :return: индекс строки
        """
        logging.info(f'Выполняю поиск последней строки')
        column = start_cell['column']
        end_row = sheet.max_row
        start_row = start_cell['row']
        if file_name == 'deflator':
            #Ищем пока не найдем слово 'услуги'
            for row in range(start_row, end_row + 1):
                value = sheet.cell(row=row, column=column).value
                if value is None:
                    continue
                elif 'услуги' in value:
                    return row
                else:
                    continue
        elif file_name == 'import' or file_name == 'ipc':
            # Ищем пока значение ячейки станет None
            for row in range(start_row, end_row + 1):
                value = sheet.cell(row=row, column=column).value
                if value is None:
                    return row

        logging.error(f'На листе {sheet.title} не найдена последняя строка')
        raise EXCEPTION_HANDLER.FileProcessError(f'На листе {sheet.title} не найдена последняя строка')

    def search_years(self, sheet, file_name):
        """
        Поиск столбцов с периодом на листе "Дефляторы" или листе "год"
        :param sheet: объект Worksheet
        :param file_name: имя обрабатываемого файла
        :return: словарь вида {год:индекс столбца,}. Например,{2020: 3, 2021: 4, 2022: 5, 2023: 6, 2024: 7}
        """
        logging.info(f'Собираю данные по годам')
        end_column = sheet.max_column
        start_cell = self.search_start_cell(sheet, file_name)
        start_column = start_cell['column']
        end_row = self.search_end_cell(sheet, start_cell, file_name)
        for column in range(start_column + 1, end_column):
            for row in range(1, end_row + 1):
                value = sheet.cell(row=row, column=column).value
                if value is None:
                    continue
                elif re.findall(r'^20\d\d$', str(value)):
                    years = {}
                    start_column = column
                    for column in range(start_column, end_column + 1):
                        if sheet.cell(row=row, column=column).value is None:
                            return years
                        years[sheet.cell(row=row, column=column).value] = column
                    logging.info('Период не найден')
                else:
                    continue
        logging.error(f'На листе {sheet.title} не найден период')
        raise EXCEPTION_HANDLER.FileProcessError(f'На листе {sheet.title} не найден период')

    def write_to_result(self, years: dict, name, okveds: list, sheet, deflator_row=None, icp_row=None, code_list='on'):
        """
        Записываем данные в self.result
        :param years:словарь вида {год:индекс столбца,}. Например,{2020: 3, 2021: 4, 2022: 5, 2023: 6, 2024: 7}
        :param name: наименование кода ОКВЭД или ТНВЭД
        :param okveds:код OKVED или ТНВЭД вида ["05","19"]
        :param sheet: объект Worksheet
        :param deflator_row: индекс строки со значением Дефлятора
        :param icp_row: индекс строки со значением ИЦП
        :param code_list: параметр для удаления кодов 18, 31, 32, 34, если нашли их на листе
        :return:
        """
        for okved in okveds:
            logging.info(f'Записываю код "{okved}" в result')
            if code_list == 'on':
                [self.code_list.remove(okved) for code in self.code_list if code == okved] #Удаляем из code_list коды 18, 31, 32, 34 если есть свопадения с okved
            if 'раздел' in okved.lower():
                okved = okved.replace('(', '').replace(')', '') #Убираем скобки из названия раздела

            dict_okved = {}
            dict_okved["Name"] = name
            if 'импорт' in sheet.title.lower() or 'print' in sheet.title.lower():
                dict_okved["TNVED"] = okved
            else:
                dict_okved["OKVED"] = okved
            dict_okved["value"] = []
            for year in years:
                dict_period = {}
                dict_period['date'] = f'{year}-01-01T00:00:00'
                dict_period['period'] = 'year'
                if deflator_row is None:
                    dict_period['ICP'] = round(sheet.cell(row=icp_row, column=years[year]).value, 1)
                elif deflator_row:
                    dict_period['ICP'] = round(sheet.cell(row=icp_row, column=years[year]).value, 1)
                    dict_period['Deflator'] = round(sheet.cell(row=deflator_row, column=years[year]).value, 1)
                dict_okved["value"].append(dict_period)
            self.result["data"].append(dict_okved)

    def search_quarter(self, sheet):
        """
        Поиск периода на листе кварт
        :param sheet: объект Worksheet
        :return:словарь вида {'период':{'Deflator':индекс столбца с периодом}}. Например, {'2022-01-01T00:00:00': {'Deflator': 2}, }
        """
        logging.info(f'Собираю данные по кварталам')
        result_quarters = {}
        end_column = sheet.max_column
        start_cell = self.search_start_cell(sheet, 'deflator')
        end_row = self.search_end_cell(sheet, start_cell, 'deflator')
        for column in range(2, end_column + 1):
            for row in range(1, end_row + 1):
                value = str(sheet.cell(row=row, column=column).value)
                if value is None:
                    continue
                elif 'Дефляторы на продукцию' in value:
                    value = str(sheet.cell(row=row + 1, column=column).value)
                    logging.info('Найден столбец "Дефляторы на продукцию"')
                    if re.search(r'^1\s{0,}кв\D{0,}\d{2}', value): #1кв.21
                        year = re.findall(r'^1\s{0,}кв\D{0,}(\d{2})', value)[0]
                        period = f"20{year}-01-01T00:00:00"
                        try:
                            result_quarters[period]['Deflator'] = column
                        except KeyError:
                            result_quarters[period] = {}
                            result_quarters[period]['Deflator'] = column

                        period = f"20{year}-04-01T00:00:00"
                        try:
                            result_quarters[period]['Deflator'] = column + 1
                        except KeyError:
                            result_quarters[period] = {}
                            result_quarters[period]['Deflator'] = column + 1

                        period = f"20{year}-07-01T00:00:00"
                        try:
                            result_quarters[period]['Deflator'] = column + 2
                        except KeyError:
                            result_quarters[period] = {}
                            result_quarters[period]['Deflator'] = column + 2

                        period = f"20{year}-10-01T00:00:00"
                        try:
                            result_quarters[period]['Deflator'] = column + 3
                        except KeyError:
                            result_quarters[period] = {}
                            result_quarters[period]['Deflator'] = column + 3
                        break
                    else:
                        logging.error(f'На листе {sheet.title} столбец с данными за 1 кв не найден')
                        raise EXCEPTION_HANDLER.FileProcessError(f'На листе {sheet.title} столбец с данными за 1 кв не найден')

                elif 'Индексы цен' in value:
                    value = str(sheet.cell(row=row + 1, column=column).value)
                    logging.info('Найден столбец "Индексы цен производителей"')
                    if re.search(r'^1\s{0,}кв\D{0,}\d{2}', value): #1кв.21
                        year = re.findall(r'^1\s{0,}кв\D{0,}(\d{2})', value)[0]
                        period = f"20{year}-01-01T00:00:00"
                        try:
                            result_quarters[f"20{year}-01-01T00:00:00"]['ICP'] = column
                        except KeyError:
                            result_quarters[period] = {}
                            result_quarters[period]['ICP'] = column

                        period = f"20{year}-04-01T00:00:00"
                        try:
                            result_quarters[f"20{year}-04-01T00:00:00"]['ICP'] = column + 1
                        except KeyError:
                            result_quarters[period] = {}
                            result_quarters[period]['ICP'] = column + 1

                        period = f"20{year}-07-01T00:00:00"
                        try:
                            result_quarters[f"20{year}-07-01T00:00:00"]['ICP'] = column + 2
                        except KeyError:
                            result_quarters[period] = {}
                            result_quarters[period]['ICP'] = column + 2
                        period = f"20{year}-10-01T00:00:00"
                        try:
                            result_quarters[f"20{year}-10-01T00:00:00"]['ICP'] = column + 3
                        except KeyError:
                            result_quarters[period] = {}
                        result_quarters[period]['ICP'] = column + 3
                        break
                    else:
                        logging.error(f'На листе {sheet.title} столбец с данными за 1 кв не найден')
                        raise EXCEPTION_HANDLER.FileProcessError(f'На листе {sheet.title} столбец с данными за 1 кв не найден')
        return result_quarters

    def match_okved(self, templates, value):
        """
        Ищет совпадение templates с value
        :param templates: Список с кодами. Например, ['05', '06', '09']
        :param value: Код. Например, ['05']
        :return :Код. Например, ['05']
        """
        for template in templates:
            if template in value:
                logging.info(f'В строке найден {template}')
                return template
        return False

    def razdel_handler(self, row, end_row, value, years, sheet):
        """
        Обрабатываем строку с Разделом
        :param row: индекс текущей строки
        :param end_row: индекс последней строки
        :param value: значение ячейки с кодами
        :param years: словарь вида {год:индекс столбца,}. Например,{2020: 3, 2021: 4, 2022: 5, 2023: 6, 2024: 7}
        :param sheet: объект Worksheet
        :return:
        """
        name_regex = re.search(r'(\w{1,},{0,};{0,}\s{0,})+(?=\(.{1,}\))', value)[0] # Из названия Раздела убираем скобки 'Добыча полезных ископаемых (Раздел B)'
        name = self.clean_text(name_regex)
        okveds = re.findall(r'\(раздел .{1,}\)', value, re.IGNORECASE) # оставляем значение только внутри скобок
        logging.info(f'Обрабатываю раздел {okveds}')
        deflator_row = row + 1
        icp_row = row + 2

        if '(раздел c)' in okveds[0].lower():
            if self.forecast == 'short':
                self.deflator_razdel_C = deflator_row
                self.icp_razdel_C = icp_row
            elif self.forecast == 'long':
                self.icp_razdel_C = row
            return

        for i in range(row + 1, end_row + 1): #проходим по строкам ниже Раздела
            value = sheet.cell(row=i, column=1).value
            # если находим подраздел, например, 'Добыча топливно-энергетических полезных ископаемых (05, 06+09) '
            #Ищим цифры в названии
            if re.findall(r'(?:\d{1,}\w{0,})+', value):
                logging.info("Раздел раскрыт")
                return
            elif 'дефлятор' in value or 'ицп' in value.lower():
                continue
            else:
                logging.info("Раздел не раскрыт")
                if self.forecast == 'short':
                    self.write_to_result(years, name, okveds, sheet, deflator_row=deflator_row, icp_row=icp_row)
                elif self.forecast == 'long':
                    self.write_to_result(years, name, okveds, sheet, icp_row=row)
                return

    def clean_text(self, text):
        """
        Очищаем текст от пробелов
        :param text: строка
        :return: текст без лишних пробелов
        """
        text = ' '.join(text.split()) # удаляем лишние пробелы
        clean_text = text.strip()
        return clean_text

    def podrazdel_handler(self, row, end_row, value, years, sheet):
        """
        Обрабатываем строку с Подразделом
        :param row: индекс текущей строки
        :param end_row: индекс последней строки
        :param value: значение ячейки с кодами
        :param years: словарь вида {год:индекс столбца,}. Например,{2020: 3, 2021: 4, 2022: 5, 2023: 6, 2024: 7}
        :param sheet: объект Worksheet
        :return:
        """
        name_regex = re.search(r'(\w{1,},{0,};{0,}\s{0,})+(?=\(.{1,}\))',value)[0] # Из названия подаздела убираем скобки "Добыча прочих полезных ископаемых (08)"
        name = self.clean_text(name_regex)
        okveds = re.findall(r'(?:\d{1,}[.]{0,}\w{0,})+', value) # оставляем значение только внутри скобок
        logging.info(f'Обрабатываю подраздел {okveds}')
        deflator_row = row + 1
        icp_row = row + 2
        for i in range(row + 1, end_row + 1):
            cell_value = sheet.cell(row=i, column=1).value
            if '(06+09)' in cell_value and okveds == ['06', '09']:
                deflator_row = i + 1
                icp_row = i + 2
                for code in [['06'], ['09']]:
                    if self.forecast == 'short':
                        self.write_to_result(years, name, code, sheet, deflator_row=deflator_row, icp_row=icp_row)
                    elif self.forecast == 'long':
                        self.write_to_result(years, name, code, sheet, icp_row=row)
                self.ignore_list.append(['(06+09)'])
                return
            elif re.findall(r'\(раздел .{1,}\)', cell_value.lower(), re.IGNORECASE):
                if self.forecast == 'short':
                    logging.info("Подраздел не раскрыт")
                    self.write_to_result(years, name, okveds, sheet, deflator_row=deflator_row, icp_row=icp_row)
                elif self.forecast == 'long':
                    logging.info("Подраздел не раскрыт")
                    self.write_to_result(years, name, okveds, sheet, icp_row=row)
                self.ignore_list.append(okveds)
                return
            elif re.findall(r'(?:\d{1,}[.]{0,}\w{0,})+', cell_value): #если в строке есть цифры
                okved_value = re.findall(r'(?:\d{1,}[.]{0,}\w{0,})+', cell_value)
                #Если ниже найден подраздел из текущего okveds
                if self.match_okved(okveds, okved_value):
                    logging.info(f"Подраздел {okveds} раскрыт ниже")
                    return
                else:
                    if self.forecast == 'short':
                        logging.info(f"Подраздел {okveds} не раскрыт")
                        self.write_to_result(years, name, okveds, sheet, deflator_row=deflator_row, icp_row=icp_row)
                    elif self.forecast == 'long':
                        logging.info(f"Подраздел {okveds} не раскрыт")
                        self.write_to_result(years, name, okveds, sheet, icp_row=row)
                    self.ignore_list.append(okveds)
                    return
            elif 'дефлятор' in cell_value.lower() or 'ицп' in cell_value.lower():
                continue

    def deflator_handler(self, sheet):
        """
        Обработка Листа дефлятор, год
        :param sheet:объект Worksheet
        :return:
        """
        start_cells = self.search_start_cell(sheet, 'deflator')
        start_row = start_cells['row']
        end_row = self.search_end_cell(sheet, start_cells, 'deflator')
        years = self.search_years(sheet, 'deflator')
        for row in range(start_row, end_row + 1):
            value = sheet.cell(row=row, column=1).value
            okved = re.findall(r'\(.{1,}\)', value) #ищет все, что содержится в мкобках( )
            if re.findall(r'\(раздел .{1,}\)', value, re.IGNORECASE):
                self.razdel_handler(row, end_row, value, years, sheet)
            elif okved in self.ignore_list:
                continue
            elif okved:
                if re.findall(r'(?:\d{1,}[.]{0,}\w{0,})+', value): #ищет цифры в скобках ( )
                    self.podrazdel_handler(row, end_row, value, years, sheet)
            elif 'дефлятор' in value.lower() or 'ицп' in value.lower():
                continue
        if self.code_list:
            name = 'Обрабатывающие производства'
            okveds = self.code_list
            logging.info(f'Записываю информацию по кодам {self.code_list}')
            if self.forecast == 'short':
                self.write_to_result(years, name, okveds, sheet, deflator_row=self.deflator_razdel_C,
                                     icp_row=self.icp_razdel_C, code_list='off')
            elif self.forecast == 'long':
                self.write_to_result(years, name, okveds, sheet, icp_row=self.icp_razdel_C, code_list='off')

    def quarter_handler(self, sheet):
        """
        Записываем в self.result данные из листа кварт
        :param sheet: объект Worksheet
        :return:
        """
        if self.result:
            logging.info(f'Записываю в result данные по кварталам')
            start_cells = self.search_start_cell(sheet, 'deflator')
            start_row = start_cells['row']
            end_row = self.search_end_cell(sheet, start_cells, 'deflator')
            column = start_cells['column']
            quarters = self.search_quarter(sheet)
            if self.code_list:
                logging.info(f'Записываю информацию по кодам {self.code_list}')
                #Если в списке есть коды [18, 31, 32, 34]
                #Записываем их с данными из раздела С
                for row in range(start_row, end_row + 1):
                    value = sheet.cell(row=row, column=column).value
                    name = ' '.join(value.split()) #убираем лишние пробелы
                    if '(раздел c)' in name.lower():
                        if self.code_list:
                            razdel_C = row
                            break

            for okved in self.result["data"]:
                for row in range(start_row, end_row + 1):
                    value = sheet.cell(row=row, column=column).value
                    name = self.clean_text(value)
                    if okved.get('OKVED','TNVED') in self.code_list:
                        for quarter in quarters:
                            okved['value'].append({
                                "date": quarter,
                                "period": "quarter",
                                "ICP": round(sheet.cell(row=razdel_C, column=quarters[quarter]['ICP']).value, 1),
                                "Deflator": round(sheet.cell(row=razdel_C, column=quarters[quarter]['Deflator']).value, 1)
                            })
                        break

                    elif okved['Name'].lower() in name.lower():
                        for quarter in quarters:
                            okved['value'].append({
                                "date": quarter,
                                "period": "quarter",
                                "ICP": round(sheet.cell(row=row, column=quarters[quarter]['ICP']).value, 1),
                                "Deflator": round(sheet.cell(row=row, column=quarters[quarter]['Deflator']).value, 1)
                            })
                        break

    def import_handler(self, sheet):
        """
        Обработка листа Импорт
        :param sheet: объект Worksheet
        :return:
        """

        start_cell = self.search_start_cell(sheet, 'import')
        start_row = start_cell['row']
        column = start_cell['column']
        end_row = self.search_end_cell(sheet, start_cell, 'import')
        years = self.search_years(sheet, 'import')

        for row in range(start_row, end_row + 1):
            value = sheet.cell(row=row, column=1).value
            if value is None:
                continue
            elif re.findall(r'\d{1,}-\d{1,}', value):
                #Для строк вида 68-70
                name = sheet.cell(row=row, column=2).value
                icp_row = self.search_index_cost(sheet, row, column)
                codes = re.findall(r'\d{1,}-\d{1,}', value)
                if len(codes) == 1:
                    # Если строка вида 68-70 задаем начало и конец
                    start = re.search(r'^\s{0,}\d{1,}\s{0,}', value)[0].strip()
                    stop = re.search(r'-\s{0,}\d{1,}\s{0,}', value)[0].replace('-', '').strip()
                    # Создаем последовательность чисел 68-70
                    okveds = ['0' + str(code) if len(str(code)) == 1 else str(code) for code in range(int(start), int(stop) + 1)]
                    self.write_to_result(years, name, okveds, sheet, icp_row=icp_row, code_list='off')
                elif len(codes) > 1:
                    # Если строка вида 68-70, 56-90
                    for code in codes:
                        start = re.search(r'^\s{0,}\d{1,}\s{0,}', code)[0].strip()
                        stop = re.search(r'-\s{0,}\d{1,}\s{0,}', code)[0].replace('-', '').strip()
                        # Создаем последовательность чисел 68-70
                        okveds = ['0' + str(code) if len(str(code)) == 1 else str(code) for code in
                                  range(int(start), int(stop) + 1)] #Добавляем к коду 0, если 1, то 01
                        self.write_to_result(years, name, okveds, sheet, icp_row=icp_row, code_list='off')
            elif re.findall(r'\d{1,}', value):
                #Для строк вида 97
                name = sheet.cell(row=row, column=2).value
                icp_row = self.search_index_cost(sheet, row, column)
                code = re.findall(r'\d{1,}', value)[0]
                okveds = [code]
                self.write_to_result(years, name, okveds, sheet, icp_row=icp_row, code_list='off')

    def search_index_cost(self, sheet, start_row, column):
        """
        Возвращает индекс строки, содержащей 'индекс цен'
        :param sheet: объект Worksheet
        :param start_row: индекс начальной строки
        :param column: индекс столбца
        :return: индекс строки
        """
        logging.info(f'Ищу строку с индексом цен ')
        start_cell = self.search_start_cell(sheet, 'import')
        end_row = self.search_end_cell(sheet, start_cell, 'import')
        for row in range(start_row, end_row):
            value = sheet.cell(row=row, column=column).value
            if 'индекс цен' in value.lower():
                return row

    def add_data_to_log(self, task, forecast, status):
        """
        Записываем в log.xlsx информацию полученных запросах
        :param task: id запроса
        :param forecast: имя прогноза
        :param status: статус выполнения
        :return:
        """
        data = {
            1: datetime.today().strftime("%d.%m.%Y %H:%M"),
            2: task,
            3: forecast,
            4: status
        }

        logging.info(f'Открываем файл: log.xlsx')
        workbook = openpyxl.load_workbook(cfg.log_tasks)
        logging.debug('workbook.active')
        worksheet = workbook.active
        end_row = worksheet.max_row
        logging.debug('ws_pattern_total.max_row')
        row = 1
        while True:
            if worksheet.cell(row=row, column=1).value is None:
                break
            row += 1
        for number_column, value in data.items():
            logging.info(f'Вносим значение: {value}')
            worksheet.cell(row=row, column=number_column).value = value
            worksheet.cell(row=row, column=number_column).border = BORDER
        workbook.save(cfg.log_tasks)
        logging.info('Запись в логфайл добавлена')

