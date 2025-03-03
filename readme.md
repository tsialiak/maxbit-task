### 1. Составьте список KPIметрик необходимых для понимания качества интернет-казино.Ответ аргументируйте.

**Доход**
1.	Выручка и прибыль казино – показывает доходность казино.
2.	Средний доход на всех игроков и тех, кто делает депозиты (ARPU/ARPPU) – важно понимать сколько нам приносит дохода один игрок для принятия решений по влиянию на поведения игрока с целью увеличения этих показателей. Сравнения этих двух показателей помогает увидеть разницу между игроками, кто совершает депозиты и всеми.
3.	Разница поставленных и выигранных денежных средств и их соотношение – эти метрики просматривать, как общие по всему казино, так и в разрезе каждой игры. Это позволит отслеживать прибыльность каждой игры и казино в целом. 

**Поведение игроков**
1.	Количество уникальных пользователей в день и месяц(DAU/MAU) – метрика, которая напрямую влияет на доходы онлайн-казино, на качество мероприятий по привлечению новых игроков и работе с имеющейся базой игроков. 
2.	Retention и среднее время проведенное в казино за сессию – показывает вовлеченность и удовлетворенность игроков.
3.	Отток (Churn Rate) – метрика, которая помогает отслеживать качество работ с имеющейся базой игроков.

**Иные показатели**
1.	Ценность игрока(LTV) – метрика, которая оценивает качество игроков и помогает в оценке прошедших и будущих маркетинговых компаний, и оценка мероприятий по удержанию игроков.
2.	Стоимость привлечения клиента(CAC) и возврат инвестиций (ROMI) – так как онлайн-казино во многом зависит от маркетинга, то эти метрики совместно с LTV помогают оценивать и улучшать маркетинговые активности.

### 2. SQL-запросы

- **Посчитает количество уникальных устройств и уникальных пользователей по месяцам в период с Июля-2023 по Декабрь-2023 (включительно)**
```sql
select date_trunc('month', el.event_timestamp) as mon
	,uniq(el.player_id) as players
	,uniq(el.gadget_id) as gadgets
from event_log el
where el.event_timestamp between '2023-07-01' and '2023-12-31'
group by mon
```

- **Выводит ТОП-5 стран по количеству зарегистрированных пользователей в 2023 году**
```sql
select ui.country_code, count(*) as cntr_co
from user_info ui 
where toYear(ui.record_created) = 2023
group by ui.country_code
order by cntr_co desc
limit 5
```

- **Выводит ТОП-5 пользователей по общей сумме платежей, и по каждой стране за 2023 год**
```sql
select *
from (
	select ui.country_code
		,pr.player_id
		,sum(pr.amount_RUB) as amount_RUB
		,ROW_NUMBER() OVER (PARTITION BY ui.country_code ORDER BY sum(pr.amount_RUB) desc) AS pos
	from payment_records pr 
	inner join user_info ui on ui.player_id = pr.player_id 
	where pr.payment_successful = 1
	and toYear(pr.payment_initiated) = 2023
	group by ui.country_code, pr.player_id
	order by sum(pr.amount_RUB) desc
) t1
where t1.pos <= 5
```

- **Посчитает среднюю продолжительности сессии на сайте за 2023 год** *(Запрос не учитывает сессии, которые начались в одном году и закончились в другом и те, что ещё не были завершены на момент выгрузки)*
```sql
select avg(date_diff('minute', el.event_timestamp, el2.event_timestamp)) as avg_session
from event_log el 
inner join event_log el2 on el2.play_session_id = el.play_session_id and el2.activity_name = 'Session End' and toYear(el2.event_timestamp) = 2023
where el.activity_name ='Session Start'
and toYear(el.event_timestamp) = 2023
```

- **Посчитает количество уникальных пользователей, посетивших сайт (DAU) по месяцам, которые подтвердили свой email-адрес. Аналитика нужна за период с Июля-2023 по Декабрь-2023** *(Я рассчитал среднее значение и медиану по DAU за каждый месяц, так как по заданию нужен именно DAU)*
```sql
select date_trunc('month', day) as mon
	,avg(players) avg_dau
	,median(players) as median_dau
from (
	select date_trunc('day', el.event_timestamp) as day, uniq(el.player_id) as players
	from event_log el 
	inner join user_info ui on ui.player_id = el.player_id and ui.email_verified <>'1970-01-01'
	where el.event_timestamp between '2023-07-01' and '2023-12-31'
	group by day
)
group by mon
```

- **Позволит оценить долю (по сумме успешного депозита) пользователей с подтвержденной почтой, в Декабре 2023 года**
```sql
select sumIf(pr.amount_RUB, ui.email_verified<>'1970-01-01') / sum(pr.amount_RUB) as prop
from payment_records pr 
inner join user_info ui on ui.player_id = pr.player_id 
where pr.payment_successful = 1
and date_trunc('month', pr.payment_initiated) = '2023-12-01'
```

- **Выведет для каждого пользователя среднее значение, нижний квартиль, медиану и верхний квартиль по времени между успешными депозитами.**
```sql
select t2.player_id	
	,avg(t2.diff) as avg
	,quantile(0.25)(t2.diff) as quantile25
	,median(t2.diff) as median
	,quantile(0.75)(t2.diff) as quantile75
from (
	select t1.player_id
		,date_diff('minute', t1.payment_initiated, t1.next_payment)	as diff
	from (
		select pr.player_id
			,pr.payment_initiated
			,anyOrNull(pr.payment_initiated) over (
			   	partition by pr.player_id
		        order by pr.payment_initiated
		        rows between 1 following and 1 following
		    ) AS next_payment
		from payment_records pr 
		where pr.payment_successful = 1
	) t1
	where next_payment is not null
) t2
group by t2.player_id
```

### 3. Оценить результаты АБ-теста

Так как в файле хранятся данные только о посещении пользователями сайт и нет данных, когда этот сайт они покинули, то в качестве конца сессии бралась дата следующей сессии. Если такой сессии нет, то дата старта сессии равна дате конца сессии.

Конверсия во вторую сессию по данным из файла получилась такова: группа 0 = 98.45%, группа 1 = 98.19%. Из цифр можно сделать вывод, что результат теста является статистически незначимым и опираться на его не стоит, поэтому для дополнительной проверки теста обратимся к иным метрикам.  
В качестве дополнительных метрик бралось среднее время сессии и среднее количество сессий на клиента.   
Среднее время сессии для группы 0 и 1 составляет 28:09 и 27:49 минут соответственно.  
Среднее количество сессий на клиента – 56.83 и 55.84.   
Из этих метрик можно сделать вывод, что данный тест повлиял не на конверсию во вторую сессию, а на частоты входов пользователей на сайт и их нахождение на нем.  

Из предложений по A/B-тесту есть два варианта:
1. Продолжать тест с использованием иных метрик (если такая возможность есть).
2. Раскатывать версию группы 0 на всех пользователей, так как он показал лучше результаты в рамках ключевой и дополнительных метрик.


Код скрипта:
```python
import numpy as np
import pandas as pd

df = pd.read_parquet('task_3_events.parquet')

df = df.groupby('user_id')[['user_id', 'user_group', 'time']].apply(lambda x: x.sort_values('time')).reset_index(drop=True)
df['session_end'] = df.groupby('user_id')['time'].shift(-1)
df['session_date'] = df['time'].dt.date
df['session_end'] = df['session_end'].fillna(df['time'])
df.columns = ['user_id', 'ab_group','session_start', 'session_end','session_date']

##убираем из фрейма сессии, где разница в сессиях ~30 минут 
##df['diff_date'] = df['session_end'] - df['session_start']
##df = df[np.floor(df['diff_date'].dt.total_seconds()/60.0) != 30]

users = df.groupby(['user_id','ab_group']).agg(session_count=('user_id','size')).reset_index()
filtered_users = users[users['session_count'] > 1]
groups = filtered_users.groupby('ab_group').size()
##groups = users.groupby('ab_group').size('session_count')
##groups = users.groupby('ab_group').size()
groups
```

