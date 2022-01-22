# -*- coding: utf-8 -*-
# Author: Noor

import scrapy

from ..helper import sq_feet_to_meters
from ..loaders import ListingLoader

class MySpider(scrapy.Spider):
    name = 'metrocorerealty_com'
    allowed_domains = ['metrocorerealty.com']
    start_urls = ['https://metrocorerealty.com/rentals']
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    thousand_separator = ','
    scale_separator = '.'

    def parse(self, response):
        links = response.css('#center a::attr(href)').extract()
        links=list(dict.fromkeys(links))
        for link in links:
            yield scrapy.Request(
                url='https://metrocorerealty.com' + link,
                callback=self.get_property_details,
                dont_filter=True)

    def get_property_details(self, response):
            item_loader = ListingLoader(response=response)
            item_loader.add_value('external_link', response.url)
            rent = response.css('.price::text').extract()[0][1:]
            item_loader.add_value('rent_string', rent[:rent.index(' ')])
            title = response.css('h2::text').extract()[0].strip()
            item_loader.add_value('title', title)
            images =response.css('img::attr(src)').extract()
            images=[i.replace("gallery", "large") for i in images]
            item_loader.add_value('images', images)
            item_loader.add_value('property_type', 'apartment')
            address=response.css('#center h4::text').extract()[0]
            item_loader.add_value('address',address)
            item_loader.add_value('city',address.split(',')[-2])
            item_loader.add_value('zipcode',address.split(',')[-1])
            desc = response.css('p+ p::text').extract()[0]
            item_loader.add_value('description', desc)
            item_loader.add_value('landlord_name', 'Janice McDonald')
            item_loader.add_value('landlord_phone', '604.729.4149')
            item_loader.add_value('currency', 'CAD')
            item_loader.add_value('external_source', self.external_source)

            info =response.css('#center li ::text').extract()
            stripped_details = [i.strip().lower() for i in info]
            if 'bedrooms' in stripped_details:
                i = stripped_details.index('bathrooms')
                v = stripped_details[i + 1]
                value=v[v.index(':')+2:]
                item_loader.add_value('room_count', int(value))
            if 'bathrooms' in stripped_details:
                i = stripped_details.index('bathrooms')
                v = stripped_details[i + 1]
                value = v[v.index(':') + 2:]
                item_loader.add_value('bathroom_count', int(value))
            if 'sqft' in stripped_details:
                i = stripped_details.index('sqft')
                v = stripped_details[i + 1]
                value = v[v.index(':') + 2:]
                item_loader.add_value('square_meters', int(int(sq_feet_to_meters(value))*10.764))
            if 'available' in stripped_details:
                i = stripped_details.index('available')
                v = stripped_details[i + 1]
                value = v[v.index(':') + 2:]
                item_loader.add_value('available_date', value)
            yield item_loader.load_item()
