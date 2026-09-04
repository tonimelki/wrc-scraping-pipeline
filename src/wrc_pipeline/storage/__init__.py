"""Mongo and object-storage clients.

Deliberately outside `scraper/`: both the spider and the transformation job
need these, and putting them inside the Scrapy package would force `transform`
to import from `scraper` just to reach a database.
"""
