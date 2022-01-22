# -*- coding: utf-8 -*-
# Author: Noor
import json

import scrapy

from ..helper import sq_feet_to_meters
from ..loaders import ListingLoader


class MySpider(scrapy.Spider):
    name = 'kodiakpropertymanagement_ca'
    allowed_domains = ['kodiakpropertymanagement.ca']
    start_urls = ['https://kodiakpropertymanagement.ca/regina/regina-rental-properties/']
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    thousand_separator = ','
    scale_separator = '.'

    def parse(self, response, *args):
        links=response.css('.title a::attr(href)').extract()
        for link in links:
            yield scrapy.Request(url=link,
                                 callback=self.get_property_details,

                                 dont_filter=True)

    def get_property_details(self, response):
        if 'parking' not in response.url:
            item_loader = ListingLoader(response=response)
            item_loader.add_value('external_link', response.url)
            title = response.css('.single-title::text').extract()[0].strip()
            item_loader.add_value('title', title)
            rent = response.css('.price::text').extract()[0][1:]
            item_loader.add_value('rent_string', rent)
            images = response.css('#main img::attr(src)').extract()
            item_loader.add_value('images', images)
            item_loader.add_value('property_type', 'apartment')
            desc = ''.join(response.css('#main p::text').extract())
            item_loader.add_value('description', desc)
            item_loader.add_value('landlord_name', 'Kodiak Property Management Ltd')
            item_loader.add_value('landlord_phone', '306-522-6080')
            item_loader.add_value('landlord_email', 'hello@kodiakpropertymanagement.ca')
            item_loader.add_value('currency', 'CAD')
            item_loader.add_value('external_source', self.external_source)

            info = response.css('.bullet-item::text').extract()
            stripped_details = [i.strip().lower() for i in info]
            for d in stripped_details:
                if 'bedroom' in d:
                    value = d[d.index(':')+1:].strip()
                    if '.' not in value:
                        item_loader.add_value('room_count', int(value))
                    else:
                        item_loader.add_value('room_count', int(value[:value.index('.')]))
                if 'bathroom' in d:
                    value =d[d.index(':')+1:].strip()
                    if '.' not in value:
                        item_loader.add_value('bathroom_count', int(value))
                    else:
                        item_loader.add_value('bathroom_count', int(value[:value.index('.')]))
                if 'square feet' in d:
                    value = d[d.index(':')+1:].strip()
                    if value !='contact us':
                        item_loader.add_value('square_meters', int(int(sq_feet_to_meters(value))*10.764))
                if 'address' in d:
                    value = d[d.index(':')+1:].strip()
                    item_loader.add_value('address', value)

            yield item_loader.load_item()
