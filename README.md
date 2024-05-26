# TableHunt

TableHunt is an Azure Function intended to store aggregated queries in Azure Table storage for the purpose of reporting and longer term storage.

Whilst it would work, it is not intended for non-aggregated data, as table storage isn't the best mechanism for performing queries on.

## Installation