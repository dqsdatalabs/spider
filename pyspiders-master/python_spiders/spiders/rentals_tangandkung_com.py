# -*- coding: utf-8 -*-
# Author: Noor
import re

import scrapy
from ..helper import sq_feet_to_meters
from ..loaders import ListingLoader


class MySpider(scrapy.Spider):
    name = 'rentals_tangandkung_com'
    allowed_domains = ['rentals.tangandkung.com']
    start_urls = [
        'https://rentals.tangandkung.com/']
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    thousand_separator = ','
    scale_separator = '.'


    def parse(self, response):
        links = response.css('.item-no-footer::attr(href)').extract()
        for link in links:
            yield scrapy.Request(
                url=link,
                callback=self.get_property_details,
                dont_filter=True)
    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        available = response.css('.status-color-21::text').extract()[0].strip()
        if 'rented' not in available.lower():
            item_loader.add_value('external_link', response.url)
            address=response.css('.item-address::text').extract()[0].strip()
            item_loader.add_value('address',address)
            item_loader.add_value('city',address.split(',')[-3])
            item_loader.add_value('zipcode',address.split(',')[-2])
            rent = response.css('.item-price::text').extract()[0].strip()[1:]
            item_loader.add_value('rent_string', rent)
            title = response.css('h1::text').extract()[0].strip()
            item_loader.add_value('title', title)
            images =  response.css('img::attr( data-lazy-src)').extract()[16:-2]
            item_loader.add_value('images', images)
            item_loader.add_value('property_type', 'apartment')
            desc = ''.join(response.css('p::text').extract()).strip()
            item_loader.add_value('description', desc)

            
            item_loader.add_value('landlord_name', 'Tangand Kung')
            item_loader.add_value('landlord_phone', '604-210-9532')
            item_loader.add_value('landlord_email', 'admin@tangandkung.com')
            item_loader.add_value('currency', 'CAD')
            item_loader.add_value('external_source', self.external_source)
            info =  response.css('li ::text').extract()
            stripped_details = [i.strip().lower() for i in info]

            if 'bedroom' in stripped_details:
                i = stripped_details.index('bedroom')
                value = stripped_details[i - 2]
                if value!='':
                    item_loader.add_value('room_count', int(value))
            if 'bathroom' in stripped_details:
                i = stripped_details.index('bathroom')
                value = stripped_details[i - 1]
                item_loader.add_value('bathroom_count', int(value))
            if 'sqft' in stripped_details:
                i = stripped_details.index('sqft')
                value = stripped_details[i - 1]
                item_loader.add_value('square_meters', int(int(sq_feet_to_meters(value))*10.764))
            if 'garages' in stripped_details:
                i = stripped_details.index('garages')
                value = stripped_details[i - 1]
                if value>0:
                    item_loader.add_value('parking',True)
            if 'pets allowed' in stripped_details:
                item_loader.add_value('pets_allowed',True)
            if 'parking stall' in stripped_details:
                item_loader.add_value('parking',True)
            if 'washer/dryer' in stripped_details:
                item_loader.add_value('washing_machine',True)
            id=response.css('link[rel="shortlink"]::attr(href)').extract()[0]
            item_loader.add_value('external_id',id[id.index('=')+1:])

            loc = ''.join(response.css('script::text').extract())
            if loc:
                location_link = loc
                location_regex = re.compile(r'\"lat\"\:\"([0-9]+\.[0-9]+)\",\"lng\"\:\"-?([0-9]+\.[0-9]+)\"')
                ll = location_regex.search(location_link)
                if ll:
                    long_lat = str(ll.group())
                    lat = long_lat[long_lat.index(':') + 2:long_lat.index(',') - 1]
                    long = long_lat[long_lat.rfind(':') + 2:-1]
                    item_loader.add_value('longitude', long)
                    item_loader.add_value('latitude', lat)
            yield item_loader.load_item()
