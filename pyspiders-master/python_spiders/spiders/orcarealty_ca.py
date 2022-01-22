# -*- coding: utf-8 -*-
# Author: Noor
import urllib

import requests
import scrapy
from ..loaders import ListingLoader
import re


class MySpider(scrapy.Spider):
    name = 'orcarealty_ca'
    allowed_domains = ['orcarealty.ca']
    start_urls = ['https://orcarealty.ca/search-results/?states=&type=condos-and-apartments']
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    thousand_separator = ','
    scale_separator = '.'

    def parse(self, response):
        pages = int(response.css('.page-link::text').extract()[-1])
        for i in range(1, pages + 1):
            yield scrapy.Request(
                url='https://orcarealty.ca/search-results/page/' +
                    str(i) +
                    '/?states&type=condos-and-apartments&action=houzez_half_map_listings&sortby=undefined&item_layout=v1',
                callback=self.parse2,
                dont_filter=True)

    def parse2(self, response):
        links = response.css('.item-no-footer::attr(href)').extract()
        for link in links:
            yield scrapy.Request(
                url=link,
                callback=self.get_property_details,
                dont_filter=True)

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        for_rent = response.css('.page-title-wrap .status-color-29::text').extract()
        if for_rent and for_rent[0]:
            item_loader.add_value('external_link', response.url)
            rent = response.css('.property-title-price-wrap .item-price::text').extract()[0][1:]
            item_loader.add_value('rent_string', rent)
            title = response.css('h1::text').extract()[0].strip()
            item_loader.add_value('title', title)
            item_loader.add_value('currency', 'CAD')
            item_loader.add_value('external_source', self.external_source)
            address = response.css('.page-title-wrap .item-address::text').extract()[0]
            item_loader.add_value('address', address)
            item_loader.add_value('city', address.split(',')[-3])
            item_loader.add_value('zipcode', address.split(',')[-2])
            desc = response.css('p::text').extract()[0]
            item_loader.add_value('description', desc)
            if desc=='\n':
                desc=''.join(response.css('#property-description-wrap span:nth-child(1)::text').extract()).strip()
                item_loader.add_value('description',desc)
            item_loader.add_value('landlord_name', 'Orca Realty ')
            item_loader.add_value('landlord_phone', '604.921.6722')
            item_loader.add_value('landlord_email', 'info@orcarealty.ca')
            images = response.css('img::attr(src)').extract()[2:-1]
            images = [re.sub(r'-\d*x\d*', "", img) for img in images]
            item_loader.add_value('images', images)
            id = response.css('#property-overview-wrap .align-items-center div::text').extract()[0].strip()
            item_loader.add_value('external_id', id)
            details = response.css('.detail-wrap ::text').extract()
            stripped_details = [i.strip().lower() for i in details]
            if 'property size:' in stripped_details:
                i = stripped_details.index('property size:')
                value = stripped_details[i + 2]
                sq = value[:value.index(' ')]
                item_loader.add_value('square_meters', int(int(int(sq))*10.764))
            if 'bedrooms:' in stripped_details:
                i = stripped_details.index('bedrooms:')
                value = stripped_details[i + 2]
                item_loader.add_value('room_count', int(value))
            elif'bedroom:' in stripped_details:
                i = stripped_details.index('bedroom:')
                value = stripped_details[i + 2]
                item_loader.add_value('room_count', int(value))
            else :
                value=response.css('.mr-1+ strong::text').extract()[0]
                item_loader.add_value('room_count', int(value))
            if 'bathroom:' in stripped_details:
                i = stripped_details.index('bathroom:')
                value = stripped_details[i + 2]
                item_loader.add_value('bathroom_count', int(value))

            if 'garages:' in stripped_details:
                i = stripped_details.index('garages:')
                value = stripped_details[i + 2]
                if int(value) > 0:
                    item_loader.add_value('parking', True)

            features = response.css('p::text').extract()[1:]
            features = [i.strip().lower() for i in features]
            if 'balcony' in features:
                item_loader.add_value('balcony', True)

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
            item_loader.add_value('property_type', 'apartment')
            yield item_loader.load_item()
