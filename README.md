# 4_4_Hadoop_intro2
## Задание 2

### Загрузка файлов
Данные загружены с репозитория https://github.com/sultanmurad/csv_files  
в папку C:\Users\User1\4_4_Hadoop\data  

__Загрузка данных в контейнер:__
C:\Users\User1\4_4_Hadoop>docker cp data/ docker-hadoop-hive-parquet-datanode-1:/import  

__Загрузка данных в HDFS:__  
  ___Создаем папки для данных___
  hadoop fs -mkdir /user/user_one/data/people  
  hadoop fs -mkdir /user/user_one/data/organizations  
  hadoop fs -mkdir /user/user_one/data/customs  
  ___Копируем данные___
  hadoop fs -put docker-hadoop-hive-parquet-datanode-1:/import/data/people.csv /user/user_one/data/people  
  hadoop fs -put docker-hadoop-hive-parquet-datanode-1:/import/data/organizations.csv /user/user_one/organizations  
  hadoop fs -put docker-hadoop-hive-parquet-datanode-1:/import/data/customers.csv /user/user_one/customs  

### Добавить столбцы.
С помощью python Jupiter Notebook во все таблицы добавлены столбцы с номером группы от 0 до 9.  
В таблицу customers добавляем год  
Код на __python__ приведен в тетради __load_data.jpynb__

### Загрузка таблиц
Загрузка таблиц приведена в файле Create_tables.sql

### Составить для аналитиков сводную статистику на уровне каждой компании и на уровне каждого года.   
Получить целевую возрастную группу подписчиков — то есть, возрастную группу, представители которой чаще всего совершали  
подписку именно в текущий год на текущую компанию.   

## Предварительный(исследовательский) анализ данных показал:
- что какую-либо связь между таблицами customers и organizations
можно установить только по колонкам company и name.
- большинство сайтов привязано к разным компаний. По сайтам связь устанавливать нельзя. 
- дополнительных данных в таблице organizations нет.
- Компании с одинаковым наименованием расположены в разных странах и имеют разные виды деятельности и вебсайты.

## Решения 
- При попытке придать уникальности компаниям добавив к ним страну получился результат с десятками тысяч строк,
где у каждой компании 1-2 возрастных групп. Поэтому принимаем 1 название = 1 компания.
- мы посчитываем только подписчиков из customers, присутствующих в organizations (иных отсеиваем INNER JOIN)
  
- в скрипте __Query_WinFunc.sql__ приведен запрос, в котором с помощью оконной функции __ROW_NUMBER()__ отбирается
для компании и отчетного года первая возрастная группа из списка по убыванию
  __Но данных отбор не учитывает возрастные группы с одинаковым максимальным количеством подписчиков__
  Случайным образом отбрасывать другие группы, с таким же числом подписчиков не корректно для аналитики!
  Решение второе приведено в скрипте __Query_JoinMAX.sql__, где отбираются все группы с максимальными значениями  
  __Пример__
  
| No | company | year | age_group | age_cnt |  
|--:|:------------|:----:|:------:|---:|   
|1|Abbott Group|2020|0-18|1|     
|2|Abbott Group|2020|65+|1|  
|3|Abbott LLC|2020|0-18|4|  
|4|Abbott LLC|2020|65+|4|  


Explain для запросов приведены в соответствующих файлах:  
- ExplainQuery_WinFunc.txt
- ExplainQuery_JoinMAX.txt





