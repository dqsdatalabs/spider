# -*- coding: utf-8 -*-
# Author: Noor

import scrapy
from ..loaders import ListingLoader
import re


class MySpider(scrapy.Spider):
    name = 'highgateproperties_ca'
    allowed_domains = ['highgateproperties.ca']
    start_urls = ['https://highgateproperties.ca/listing/?type=list&sp_sort=price-high&showposts=48']
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    thousand_separator = ','
    scale_separator = '.'


    def parse(self, response):
        links=response.css('h3  a').xpath('@href').extract()
        for link in links:
            yield scrapy.Request(
                url=link,
                callback=self.get_property_details,
                dont_filter=True)

    def get_property_details(self, response):
            item_loader = ListingLoader(response=response)
            item_loader.add_value('external_link', response.url)
            ##
            id = response.css('.search-title .search-title .pull-left ::text').extract()[2].strip()
            item_loader.add_value('external_id', id)
            ##
            r = response.css('.text-line strong::text').extract()[0]
            item_loader.add_value('rent_string', r[1:r.index('/')].strip())
            title =response.css('.custom-margin p::text').extract()[0]
            item_loader.add_value('title', title)

            item_loader.add_value('currency', 'CAD')
            item_loader.add_value('external_source', self.external_source)
            desc = response.css('.muted::text').extract()[0].strip()
            item_loader.add_value('description', desc)
            images = response.css('.sp-gallery-image').xpath('@src').extract()
            item_loader.add_value('images', images)
            address = response.css('.well3:nth-child(2) strong::text').extract()[0]
            item_loader.add_value('address', address)
            item_loader.add_value('city',address.split(',')[-2])
            item_loader.add_value('zipcode',address.split(',')[-1])
            item_loader.add_value('landlord_name','High Gate')
            item_loader.add_value('landlord_phone','+1 (416) 823-0093')

            l = ''.join(response.css('script ::text').extract())
            long_lat=l[l.index('"latitude"'):l.index('"latitude"') + 60]
            lat = long_lat.split()[1][1:-2]
            long = long_lat.split()[3][1:-1]
            item_loader.add_value('longitude', long)
            item_loader.add_value('latitude', lat)

            features=response.css('td ::text').extract()
            if 'Bathrooms' in features:
                i=features.index('Bathrooms')
                value=features[i+1]
                item_loader.add_value('bathroom_count',int(value))
            if 'Bedrooms' in features:
                i=features.index('Bedrooms')
                value=features[i+1]
                item_loader.add_value('room_count',int(value))
            if 'Parking Spaces' in features:
                i = features.index('Parking Spaces')
                value = int(features[i + 1])
                prk=True if value>0 else False
                item_loader.add_value('parking',prk)
            if 'Type' in features:
                i = features.index('Type')
                value = features[i + 1]
                if value !='Offices':
                    if value=='Row / Townhouse':
                        item_loader.add_value('property_type', 'house')
                    elif value=='Triplex':
                        item_loader.add_value('property_type', 'apartment')
                    else:
                        item_loader.add_value('property_type', value.lower())
                    yield item_loader.load_item()


