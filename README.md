# common-tools

Master repository of common tools for analysis used in the Market Insights team.

## Data 

Add data to this folder so that relevant data can be easily extracted. Keep compact.

Refer to sharepoint_links folder for where you can find the data.

## Daily Data

Go to this directory

```
> cd ~/Daily Data/scripts/python/
```

and run this command:

```python
> python main.py
```

ONLY daily data:

```python
> python main.py --dd
```

or ONLY coal outages:

```python
> python main.py --outages
```

### Price Calculator

Date format: YYYY-MM-DD.

Return the VWA price between two dates.

```python
> python price_calculator.py date1 date2
```

Return a summary for a given date.

```python
> python price_calculator.py date
```

### Outages

```python
> python main.py --update --outages
```

## Panels and Turbines

## LOR Events

## Coal Flexibility

## Generation Ownership

## Market Notices Dashboard

Scripts to generate tagging data are found below.