# -*- coding: utf-8 -*-
# Author: Noor
import urllib

import requests
import scrapy
from ..loaders import ListingLoader
import re


class MySpider(scrapy.Spider):
    name = 'vancouverrentalproperties_com'
    allowed_domains = ['vancouverrentalproperties.com']
    start_urls = [
        'https://www.vancouverrentalproperties.com/property-search/?location=all&bedrooms=&status=all&pricerange=&type=apartment&bathrooms=&furnished=&orderby=date-new&pageid=21']
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    thousand_separator = ','
    scale_separator = '.'

    def parse(self, response):
        pages = int(response.css('.page-numbers::text').extract()[-3])
        for i in range(1, pages + 1):
            yield scrapy.Request(
                url='https://www.vancouverrentalproperties.com/property-search/page/' + str(i) + '/?location=all&bedrooms&status=all&pricerange&type=apartment&bathrooms&furnished&orderby=date-new&pageid=21#038;bedrooms&status=all&pricerange&type=apartment&bathrooms&furnished&orderby=date-new&pageid=21',
                callback=self.parse2,
                dont_filter=True)

    def parse2(self, response):
        links = response.css('.property-item a ::attr(href)').extract()
        for link in links:
            yield scrapy.Request(
                url=link,
                callback=self.get_property_details,
                dont_filter=True)

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.url)
        title = response.css('h2::text').extract()[0].strip()
        item_loader.add_value('title', title)
        item_loader.add_value('currency', 'CAD')
        item_loader.add_value('external_source', self.external_source)
        adr=response.css('.el-block::text').extract()
        if adr and adr[0]:
            address = adr[0].strip()
            item_loader.add_value('address', address)
        images = response.css('#carousel img::attr(src)').extract()
        images = [re.sub(r'-\d*x\d*', "", img) for img in images]
        item_loader.add_value('images', images)
        landlord_info = response.css('.property_agentinfo p::text').extract()
        ll_info = [i[i.index(' ') + 1:] for i in landlord_info]
        item_loader.add_value('landlord_name', ll_info[0])
        item_loader.add_value('landlord_phone', ll_info[1])
        item_loader.add_value('landlord_email', ll_info[2])
        desc = ''.join(response.css('.siglepoperty_decp ::text').extract()[3:]).strip()
        item_loader.add_value('description', desc)
        item_loader.add_value('property_type', 'apartment')
        item_loader.add_value('city',"Vancouver")
        scrape_it=False
        rent_is_existed = False
        details = response.css('.singlepoperty_leftinfo p::text').extract()
        stripped_details = [i.strip().lower() if type(i) == str else str(i) for i in details]
        for d in stripped_details:
            if 'bedrooms:' in d:
                value = d[d.rfind(":") + 1:].strip()
                if '.' not in value:
                    item_loader.add_value('room_count', int(value))
                else:
                    item_loader.add_value('room_count', int(value[:value.index('.')]))
            elif 'room' in d:
                value = d[d.rfind(":") + 1:].strip()
                if '.' not in value:
                    item_loader.add_value('room_count', int(value))
                else:
                    item_loader.add_value('room_count', int(value[:value.index('.')]))
            if 'bathroom:' in d:
                value = d[d.rfind(":") + 1:].strip()
                if '.' not in value:
                    item_loader.add_value('bathroom_count', int(value))
                else:
                    item_loader.add_value('bathroom_count', int(value[:value.index('.')]))
            if 'status:' in d and 'fur' in d:
                value = d[d.rfind(":") + 1:].strip()
                if 'unfurnished' in value:
                    item_loader.add_value('furnished', False)
                else:
                    item_loader.add_value('furnished', True)
            if 'garages:' in d:
                value = int(d[d.rfind(":") + 1:].strip())
                if value > 0:
                    item_loader.add_value('parking', True)
                else:
                    item_loader.add_value('parking', False)
            if 'size:' in d:
                sq_ft = int(d[d.rfind(":") + 1:d.rfind('sq')].strip())
                item_loader.add_value('square_meters', int(int(int(sq_ft / 10.764))*10.764))
            if 'price' in d:
                rent_is_existed=True
                value = d[d.rfind("$") + 1:d.rfind('-')].strip()
                item_loader.add_value('rent_string', value)
            if 'property id' in d:
                value = d[d.rfind(":") + 1:].strip()
                item_loader.add_value('external_id', value)
            if 'availability from' in d:
                value = d[d.rfind(":") + 1:].strip()
                item_loader.add_value('available_date', value)
            if 'property status:' in d:
                value = d[d.rfind(":") + 1:].strip()
                if 'Available Now!'.lower() in value:
                    scrape_it=True
        if scrape_it and rent_is_existed:
            yield item_loader.load_item()
