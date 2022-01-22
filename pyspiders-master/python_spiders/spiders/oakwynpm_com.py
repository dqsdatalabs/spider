# -*- coding: utf-8 -*-
# Author: Noor

import scrapy

from ..helper import sq_feet_to_meters
from ..loaders import ListingLoader


class MySpider(scrapy.Spider):
    name = 'oakwynpm_com'
    allowed_domains = ['oakwynpm.com']
    start_urls = ['https://www.oakwynpm.com/properties-for-rent']
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    thousand_separator = ','
    scale_separator = '.'

    
    def parse(self, response):
        links = response.css('.blog-more-link::attr(href)').extract()
        for link in links:
            yield scrapy.Request(
                url='https://www.oakwynpm.com' + link,
                callback=self.get_property_details,
                dont_filter=True)

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        available = response.css('h4::text').extract()[0].strip()
        if 'rented' not in available.lower():
            item_loader.add_value('available_date',available[available.index(':')+1:].strip())
            item_loader.add_value('external_link', response.url)
            rent = response.css('h4::text').extract()[1][1:]
            item_loader.add_value('rent_string', rent[:rent.index(' ')])
            title = response.css('.p-name::text').extract()[0].strip()
            item_loader.add_value('title', title)
            address=title.split('|')[0]
            item_loader.add_value('address',address)
            images = response.css('.slide img::attr(src)').extract()
            item_loader.add_value('images', images)
            item_loader.add_value('property_type', 'apartment')
            desc =  response.css('.sqs-block-content ::text').extract()[-50]
            item_loader.add_value('description', desc)
            item_loader.add_value('landlord_name', 'Oakwyn')
            item_loader.add_value('landlord_phone', '778-929-0200')
            item_loader.add_value('landlord_email', 'info@oakwynpm.com')
            item_loader.add_value('currency', 'CAD')
            item_loader.add_value('external_source', self.external_source)
            info = response.css('#article- .span-3 .sqs-block-content ::text').extract()
            stripped_details = [i.strip().lower() for i in info]
            for d in stripped_details:
                if 'bedroom' in d:
                    value = d[:d.index(' ')]
                    item_loader.add_value('room_count', int(value))
                if 'bathroom' in d:
                    value = d[:d.index(' ')]
                    item_loader.add_value('bathroom_count', int(value))
                if 'sf' in d:
                    value = d[:d.index(' ')]
                    item_loader.add_value('square_meters', int(int(sq_feet_to_meters(value))*10.764))
                if 'sq ft' in d:
                    value = d[:d.index(' ')]
                    item_loader.add_value('square_meters', int(int(sq_feet_to_meters(value))*10.764))
            if 'balcony' in stripped_details:
                item_loader.add_value('balcony', True)
            if 'elevators' in stripped_details:
                item_loader.add_value('elevator', True)
            if 'underground parking'in stripped_details or 'street parking'in stripped_details:
                item_loader.add_value('parking', True)
            if 'unfurnished' in stripped_details or 'not furnished' in stripped_details :
                item_loader.add_value('furnished', False)
            if 'furnished' in stripped_details  :
                item_loader.add_value('furnished', True)
            if 'dishwasher' in stripped_details  :
                item_loader.add_value('dish_washer', True)
            if 'no pets' in stripped_details:
                item_loader.add_value('pets_allowed',False)
            lat= response.css('meta[property="og:latitude"]::attr(content)').extract()[0]
            lng= response.css('meta[property="og:longitude"]::attr(content)').extract()[0]
            item_loader.add_value('latitude',lat)
            item_loader.add_value('longitude', lng)




            yield item_loader.load_item()
