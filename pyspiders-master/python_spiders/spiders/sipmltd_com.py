# -*- coding: utf-8 -*-
# Author: Noor

import scrapy
from ..helper import sq_feet_to_meters
from ..loaders import ListingLoader


class MySpider(scrapy.Spider):
    name = 'sipmltd_com'
    allowed_domains = ['sipmltd.com']
    start_urls = ['https://www.sipmltd.com/rentals']
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    thousand_separator = ','
    scale_separator = '.'

    def parse(self, response):
        links = response.css('#rentals a::attr(href)').extract()
        links=list(dict.fromkeys(links))
        for link in links:
            yield scrapy.Request(
                url='https://www.sipmltd.com' + link,
                callback=self.get_property_details,
                dont_filter=True,
            )

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.url)
        title = response.css('h1::text').extract()[0].strip()
        item_loader.add_value('title', title)
        address = title.split('-')[1]
        item_loader.add_value('address', address)
        imgs = response.css('#content img::attr(src)').extract()[1:]
        images = []
        for i in imgs:
            images.append('https://www.sipmltd.com' + i)
        item_loader.add_value('images', images)
        item_loader.add_value('property_type', 'apartment')
        para = response.css('.rental-detail ::text').extract()
        start = len(para) - 1 - para[::-1].index('All fields are required.') + 1
        end = para.index('Available:')
        desc = ''.join(para[start:end]).strip()
        item_loader.add_value('description', desc)
        item_loader.add_value('landlord_name', 'South Island')
        item_loader.add_value('landlord_phone', '250-595-6680')
        item_loader.add_value('landlord_email', 'propertymanagement@sipmltd.com')
        item_loader.add_value('currency', 'CAD')
        item_loader.add_value('external_source', self.external_source)
        data = response.css('.rental-detail1::text').extract()
        values = response.css('.rental-detail2::text').extract()
        values = [i.strip().lower() for i in values]
        if 'Price:' in data:
            i = data.index('Price:')
            value = values[i]
            item_loader.add_value('rent_string', value[1:value.index('/')])
        if 'Available:' in data:
            i = data.index('Available:')
            value = values[i]
            item_loader.add_value('available_date', value)
        if 'Location:' in data:
            i = data.index('Location:')
            value = values[i]
            item_loader.add_value('city', value)
        if 'Bedrooms:' in data:
            i = data.index('Bedrooms:')
            value = values[i]
            item_loader.add_value('room_count', int(value))
        if 'Bathrooms:' in data:
            i = data.index('Bathrooms:')
            value = values[i]
            item_loader.add_value('bathroom_count', int(value))
        if 'Total sqft:' in data:
            i = data.index('Total sqft:')
            value = values[i]
            item_loader.add_value('square_meters', int(int(sq_feet_to_meters(value))*10.764))
        if 'Pets OK:' in data:
            i = data.index('Pets OK:')
            value = values[i]
            if value == 'no':
                item_loader.add_value('pets_allowed', False)
            elif value=='yes':
                item_loader.add_value('pets_allowed', True)

        yield item_loader.load_item()
