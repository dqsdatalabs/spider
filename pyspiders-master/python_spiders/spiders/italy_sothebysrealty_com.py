
# -*- coding: utf-8 -*-
# Author: Noor
import scrapy
from ..loaders import ListingLoader
import re

class MySpider(scrapy.Spider):
    name = 'italy_sothebysrealty_com'
    allowed_domains = ['italy-sothebysrealty.com']
    start_urls = ['https://www.italy-sothebysrealty.com/it/affitto/?property_cat=1&fromsearch=1&dosearch=1']
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    thousand_separator = '.'
    scale_separator = '.'

    def parse(self, response):
        num=int(response.css('.pageNav a::text').extract()[-3])
        for i in range(1,num+1):
            yield scrapy.Request(
                url='https://www.italy-sothebysrealty.com/it/affitto/?property_cat=1&fromsearch=1&dosearch=1&page='+str(i),
                callback=self.parse2,
                dont_filter=True)
    def parse2(self, response):
        lnks=response.css('.launch-property-wrapper a').xpath('@href').extract()
        links = list(dict.fromkeys(lnks))
        for link in links:
            yield scrapy.Request(
                url= link,
                callback=self.get_property_details,
                dont_filter=True)

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.url)
        title = response.css('h1::text').extract()[0]
        item_loader.add_value('title', title)
        item_loader.add_value('property_type','apartment')
        desc=''.join(response.css('p::text').extract())
        item_loader.add_value('description',desc)
        ll_name=response.css('.agent::text').extract()[0]
        item_loader.add_value('landlord_name',ll_name)
        item_loader.add_value('landlord_phone','+390287078300')
        item_loader.add_value('landlord_email', 'italy@sothebysrealty.it')
        img_count=int(response.css('.gallery span::text').extract()[0][1:-1])-1
        imgs=response.css('.lazyload').xpath('@data-src').extract()
        images=[i for i in imgs if 'w_128/h_85' not in i][:img_count]
        item_loader.add_value('images',images)
        item_loader.add_value('currency', 'EUR')
        item_loader.add_value('external_source', self.external_source)

        details=response.css('.property-details .clearfix ::text').extract()
        stripped_details=[i.strip() for i in details]
        if 'Indirizzo' in stripped_details:
            i=stripped_details.index('Indirizzo')
            value=stripped_details[i+2]
            item_loader.add_value('address',value)
            if ','in value:
                item_loader.add_value('city',value.split(',')[0])
        if 'Prezzo' in stripped_details:
            i=stripped_details.index('Prezzo')
            value=stripped_details[i+2]
            if value != 'Trattativa Riservata':
                item_loader.add_value('rent_string',value[:value.index('€')].strip())
        if 'Camere' in stripped_details:
            i=stripped_details.index('Camere')
            value=stripped_details[i+2]
            item_loader.add_value('room_count',int(value))
        if 'Bagni' in stripped_details:
            i=stripped_details.index('Bagni')
            value=stripped_details[i+2]
            item_loader.add_value('bathroom_count',int(value))
        if 'Mq int.' in stripped_details:
            i=stripped_details.index('Mq int.')
            value=stripped_details[i+2]
            item_loader.add_value('square_meters',int(value))
        if 'Riferimento' in stripped_details:
            i=stripped_details.index('Riferimento')
            value=stripped_details[i+2]
            item_loader.add_value('external_id',value)

        yield item_loader.load_item()
