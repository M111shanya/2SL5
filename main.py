import os
import pandas as pd

EXPECTED_STRUCTURE = {
    'Участники гражданского оборота': 'object',
    'Тип операции': 'object',
    'Сумма операции': 'float64',
    'Результат операции': 'object',
    'Место оплаты': 'object',
    'Терминал оплаты': 'object',
    'Дата оплаты': 'object',
    'Время оплаты': 'object',
    'Cash-back': 'object',
    'Сумма cash-back': 'float64', 
    'Вид расчета': 'object'
}

class Data():
    def __init__(self, file_path, expected_structure: dict):
        self.file_path = file_path
        self.expected_structure = expected_structure
        self.data = None
        
    def check_file_exists(self):
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"Ошибка: Файл не найден по пути {self.file_path}")
        
    def read_csv(self):
        try:
            self.data = pd.read_csv(self.file_path)
            if self.data.empty:
                raise ValueError(f"Ошибка: Файл {self.file_path} пуст")
        except Exception as e:
            raise IOError(f"Ошибка при чтении файла {self.file_path}: {e}")
            
    def validate_structure(self):

        actual_columns = set(self.data.columns)
        expected_columns = set(self.expected_structure.keys())

        if actual_columns != expected_columns:
            missing = expected_columns - actual_columns
            extra = actual_columns - expected_columns
            error_msg = "Ошибка: Структура датафрейма не соответствует ожидаемой"
            if missing:
                error_msg = f"Отсутствуют колонки {missing}"
            if extra:
                error_msg = f"Найдены лишние колонки {extra}"
            raise ValueError(error_msg)

        for col_name, expected_type in self.expected_structure.items():
            actual_type = str(self.data[col_name].dtype)
            if actual_type != expected_type:
                raise TypeError(
                    f"Ошибка: Неверный тип данных в колонке {col_name} "
                    f"Ожидался {expected_type}, но получен {actual_type}"
                )
        
        print("Структура и типы данных датафрейма соответствуют ожидаемым")
            
    def load_and_validate(self):
        self.check_file_exists()
        self.read_csv()
        self.validate_structure()

def main():
    data = Data(file_path="var1.csv", expected_structure=EXPECTED_STRUCTURE)
    data.load_and_validate()

if __name__ == "__main__":
    main()
