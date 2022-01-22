# -*- coding: utf-8 -*-
# Author: Noor

import scrapy
from ..loaders import ListingLoader

class MySpider(scrapy.Spider):
    name = 'immobiliarenonsolocasa_it'
    allowed_domains = ['immobiliarenonsolocasa.it']
    start_urls = ['https://www.immobiliarenonsolocasa.it/']
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    thousand_separator = '.'
    scale_separator = '.'

    def parse(self, response, *args):
        main_links = []
        for i in range(1, 10):
            main_links.append(
                'https://www.immobiliarenonsolocasa.it/ricerca/page/' + str(i) + '/?status=affitto&type=appartamento')
        for main_link in main_links:
            yield scrapy.Request(url=main_link,
                                 callback=self.parse2,
                                 dont_filter=True)

    def parse2(self, response):
        pages_links = response.css('.rh_list_card__map_details h3 a').xpath('@href').extract()
        for link in pages_links:
            if link != '#':
                yield scrapy.Request(url=link, callback=self.get_property_details, cb_kwargs={'link': link},
                                     dont_filter=True)

    def get_property_details(self, response, link):
        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', link)

        address = response.css('.rh_page__property_address::text').extract()[0].strip()
        item_loader.add_value('address', address)
        if ',' in address:
            zip=address.split(',')
            if zip and zip[-2]:
                item_loader.add_value('zipcode', zip[-2][:6] )

        title = response.css('.rh_page__title::text').extract()[0].strip()
        item_loader.add_value('title', title)

        sq_meters = int(response.css('.figure::text').extract()[2].strip())
        item_loader.add_value('square_meters', sq_meters)

        external_id = response.css('.id::text').extract()[0].strip()
        item_loader.add_value('external_id', external_id)

        rent_string = response.css('.price::text').extract()[0].strip()[1:7].strip()
        item_loader.add_value('rent_string', rent_string)

        item_loader.add_value('property_type', 'apartment')

        item_loader.add_value('city',response.css('.property-breadcrumbs a::text').extract()[1])

        description = ''.join(response.css('.rh_content ::text').extract()).strip()
        item_loader.add_value('description', description)

        images = response.css('.venobox').xpath('@href').extract()
        item_loader.add_value('images', images)

        item_loader.add_value('currency', 'EUR')
        item_loader.add_value('landlord_name', 'immobiliarenonsolocasa')
        item_loader.add_value('landlord_email', 'info@immobiliarenonsolocasa.it')

        item_loader.add_value('landlord_phone', '02 29529745')
        item_loader.add_value('external_source', self.external_source)

        bathroom_count = int(response.css('.figure::text').extract()[1].strip())
        item_loader.add_value('bathroom_count', bathroom_count)

        room_count = int(response.css('.figure::text').extract()[0].strip())
        item_loader.add_value('room_count', room_count)
        yield item_loader.load_item()
