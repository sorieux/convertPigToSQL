-- Charger les données
data = LOAD 'data/input/transactions.csv' USING PigStorage(',') AS (id:int, date:chararray, amount:float, category:chararray);

-- Filtrer pour une catégorie spécifique, par exemple 'Electronique'
filteredElectronics = FILTER data BY category == 'Electronique';

-- Regrouper par date
groupedByDate = GROUP filteredElectronics BY date;

-- Calculer la somme des ventes pour chaque groupe
salesPerDay = FOREACH groupedByDate GENERATE group AS date, SUM(filteredElectronics.amount) AS totalSales;

-- Charger une deuxième table, par exemple les données client
customers = LOAD 'data/input/customers.csv' USING PigStorage(',') AS (id:int, name:chararray, city:chararray);

-- Jointure avec la table des clients sur l'id
transactionsWithCustomerInfo = JOIN data BY id, customers BY id;

-- Filtrer les transactions supérieures à un certain montant
highValueTransactions = FILTER transactionsWithCustomerInfo BY amount > 100;

-- Regrouper les transactions par ville
groupedByCity = GROUP highValueTransactions BY city;

-- Calculer le montant total des ventes par ville
totalSalesPerCity = FOREACH groupedByCity GENERATE group AS city, SUM(highValueTransactions.amount) AS totalCitySales;

-- Trier les ventes totales par ville en ordre décroissant
sortedSalesPerCity = ORDER totalSalesPerCity BY totalCitySales DESC;

-- Stocker le résultat dans un fichier
STORE sortedSalesPerCity INTO 'data/output/city_sales_results' USING PigStorage(',');

-- Stocker également les ventes quotidiennes dans un fichier
STORE salesPerDay INTO 'data/output/daily_sales_results' USING PigStorage(',');
