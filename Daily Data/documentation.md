# Changes

## Coal Outages by Region

Changed fill table 2 for loop in get_coal_outages() function within coal_outages.py so that it writes coal outages by region.

get_coal_outages() now returns the current outages (with a region column) so that it can be used as an input to the visualise_outages() function to generate a outage duration visual. 