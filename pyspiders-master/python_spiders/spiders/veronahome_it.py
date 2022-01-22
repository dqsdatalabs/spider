# -*- coding: utf-8 -*-
# Author: Noor

import scrapy
from ..loaders import ListingLoader
import re


class MySpider(scrapy.Spider):
    name ='veronahome_it'
    allowed_domains = ['veronahome.it']
    start_urls = ['https://veronahome.it/property-search/?search_status=29&search_type=33']
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    thousand_separator = '.'
    scale_separator = '.'

    def parse(self, response):
        links=response.css('.pxp-results-card-1').xpath('@href').extract()
        for link in links:
            yield scrapy.Request(
                url=link,
                callback=self.get_property_details,
                dont_filter=True)

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.url)
        item_loader.add_value('property_type', 'apartment')
        images = response.css('.pxp-single-property-gallery .pxp-cover').xpath('@href').extract()
        item_loader.add_value('images', images)
        item_loader.add_value('currency', 'EUR')
        item_loader.add_value('landlord_name', 'veronahome')
        item_loader.add_value('landlord_phone', '045 2060742')
        item_loader.add_value('external_source', self.external_source)
        item_loader.add_value('landlord_email', 'info@veronahome.it')
        desc =''.join(response.css('.mt-md-4 p ::text').extract())
        item_loader.add_value('description', desc)
        title = response.css('.pxp-sp-top-title::text').extract()[0].strip()
        item_loader.add_value('title', title)
        id = title.split('-')[-1]
        item_loader.add_value('external_id', id)
        if 'TRILOCALE' in title.upper():
            item_loader.add_value('room_count', 3)
        if 'BILOCALE' in title.upper():
            item_loader.add_value('room_count', 2)
        if 'MONOLOCALE' in title.upper():
            item_loader.add_value('room_count', 1)
        rent = response.css('.pxp-sp-top-price::text').extract()[0].strip()[:-1]
        item_loader.add_value('rent_string', rent)
        address=response.css('.pxp-text-light::text').extract()[0]
        item_loader.add_value('city',address.split(',')[-2].strip())
        item_loader.add_value('zipcode',address.split(',')[-3].strip())
        item_loader.add_value('address',address)
        dt_details = response.css('.mt-md-4 li ::text').extract()
        stripped_details = [i.strip().lower() if type(i) == str else str(i) for i in dt_details]
        for d in stripped_details:
            if 'piano:' in d:
                floor = d[d.rfind(":")+1:].strip()
                item_loader.add_value('floor', floor)
            if 'metri quadri:' in d:
                sq = int(d[d.rfind(":")+1:].strip())
                item_loader.add_value('square_meters', sq)
            if 'posto auto:' in d:
                if d[-2:]=='si':
                    item_loader.add_value('parking',True)
                else:
                    item_loader.add_value('parking',False)
            if 'bagni:' in d:
                bathroom_count = int(d[-1])
                item_loader.add_value('bathroom_count', bathroom_count)
            if 'arredamento:' in d:
                fur = d[d.rfind(":")+1:].strip()
                if fur == 'completamente arredato':
                    item_loader.add_value('furnished', True)
                else:
                    item_loader.add_value('furnished', False)
            if 'ascensore:' in d:
                elev =d[-2:]
                if elev=='si':
                    item_loader.add_value('elevator', True)
                else:
                    item_loader.add_value('elevator', False)
            if 'spese condominiali:' in d:
                utility = d[d.rfind('€')+1:d.rfind('/')].strip()
                item_loader.add_value('utilities',utility)
        location=response.css('#pxp-map-submit-js-extra::text').extract()[0]
        lat_lng=location[location.index('{'):location.index('{') + 60]
        lat=lat_lng[lat_lng.index('":"')+3:lat_lng.index('","')]
        lng=lat_lng[lat_lng.rfind('":"')+3:lat_lng.rfind('","')]
        item_loader.add_value('latitude',lat)
        item_loader.add_value('longitude',lng)

        yield item_loader.load_item()
