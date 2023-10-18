# DWH
В данном проекте необходимо было произвести миграцию данных в отдельные логические таблицы, а затем собрать на них витрину данных. Это поможет оптимизировать нагрузку на хранилище и позволит аналитикам, перед которыми стоит задача построить анализ эффективности и прибыльности бизнеса, отвечать на точечные вопросы о тарифах вендоров, стоимости доставки в разные страны, количестве доставленных заказов за последнюю неделю. Если искать эти данные в таблице исходных логов доставки, нагрузка на хранилище будет не оптимальна. Придется усложнять запросы, что может привести к ошибкам.

Заказ в интернет-магазине — это набор купленных товаров и их количество. Покупатели привыкли получать заказы одномоментно, поэтому каждый заказ из набора товаров формируется в одну сущность доставки.

Интернет-магазину важно видеть, что сроки доставки соблюдаются, а её стоимость соответствует тарифам. Он платит за доставку самостоятельно, и стоимость доставки меняется в зависимости от страны — это базовая сумма, которую учитывает вендор. По договору он дополнительно получает прибыль за счет комиссии от вендора. 
