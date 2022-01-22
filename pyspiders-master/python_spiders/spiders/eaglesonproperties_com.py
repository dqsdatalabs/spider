# -*- coding: utf-8 -*-
# Author: Noor
import urllib

import requests
import scrapy

from ..helper import sq_feet_to_meters
from ..loaders import ListingLoader
import re


class MySpider(scrapy.Spider):
    name = 'eaglesonproperties_com'
    allowed_domains = ['eaglesonproperties.com']
    start_urls = ['https://eaglesonproperties.com/rentals/']
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    thousand_separator = ','
    scale_separator = '.'

    def parse(self, response):
        links = response.css('.propery-title::attr(href)').extract()
        for link in links:
            yield scrapy.Request(
                url='https://eaglesonproperties.com' + link,
                callback=self.get_property_details,
                dont_filter=True)

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        for_rent = response.css('.mkdf-property-status::text').extract()[0]
        if for_rent == 'For Rent':
            item_loader.add_value('external_link', response.url)
            rent = response.css('.mkdf-property-price-value::text').extract()[0]
            item_loader.add_value('rent_string', rent[:rent.index(' ')])
            title = response.css('h2::text').extract()[0].strip()
            item_loader.add_value('title', title)
            item_loader.add_value('currency', 'CAD')
            item_loader.add_value('external_source', self.external_source)
            address = response.css('.mkdf-full-address .mkdf-label-items-value::text').extract()[0].strip()
            item_loader.add_value('address', address)
            desc = ''.join(response.css('.mkdf-property-items-style p ::text').extract())
            item_loader.add_value('description', desc)
            item_loader.add_value('landlord_name', 'Eagle Son')
            item_loader.add_value('landlord_phone', '6048791070')
            item_loader.add_value('landlord_email', 'info@eaglesonproperties.com')
            images = response.css('.mkdf-property-single-lightbox img::attr(src)').extract()
            item_loader.add_value('images', images)

            details = response.css('.mkdf-spec .mkdf-grid-row ::text').extract()
            stripped_details = [i.strip().lower() for i in details]
            if 'property size:' in stripped_details:
                i = stripped_details.index('property size:')
                value = stripped_details[i + 3]
                sq = value[:value.index('sq')]
                item_loader.add_value('square_meters', int(int(sq_feet_to_meters(sq))*10.764))
            if 'bedrooms:' in stripped_details:
                i = stripped_details.index('bedrooms:')
                value = stripped_details[i + 3]
                item_loader.add_value('room_count', int(value))
            if 'bathrooms:' in stripped_details:
                i = stripped_details.index('bathrooms:')
                value = stripped_details[i + 3]
                item_loader.add_value('bathroom_count', int(value))

            features = response.css('.mkdf-property-features ::text').extract()
            features = [i.strip().lower() for i in features]
            if 'street parking' in features:
                item_loader.add_value('parking', True)
            if 'pet friendly' in features:
                item_loader.add_value('pets_allowed',True)
            if 'coin laundry' in features:
                item_loader.add_value('washing_machine',True)

            item_loader.add_value('property_type', 'apartment')
            city=response.url.split('/')[-2]
            item_loader.add_value('city',city)
            yield item_loader.load_item()
