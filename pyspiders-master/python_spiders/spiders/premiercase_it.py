# -*- coding: utf-8 -*-
# Author: Noor

import scrapy
from ..loaders import ListingLoader


class MySpider(scrapy.Spider):
    name = 'premiercase_it'
    allowed_domains = ['premiercase.it']
    start_urls = ['https://www.premiercase.it/?ct_ct_status=for-rent&search-listings=true']
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    thousand_separator = '.'
    scale_separator = '.'

    def parse(self, response):
        pages_number = int(response.css('.pagination a::text').extract()[-2])
        for i in range(1, pages_number + 1):
            yield scrapy.Request(
                url='https://www.premiercase.it/page/' + str(i) + '/?ct_ct_status=for-rent&search-listings=true',
                callback=self.parse2,
                dont_filter=True)

    def parse2(self, response):
        links = response.css('.listing-link').xpath('@href').extract()
        for link in links:
            yield scrapy.Request(
                url=link,
                callback=self.get_property_details,
                dont_filter=True)


    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        title = response.css('#listing-title::text').extract()[0]
        item_loader.add_value('title', title)
        address = response.css('.location::text').extract()[0]
        if len(address)>5:
            item_loader.add_value('address', address)
            item_loader.add_value('city',address[0:address.index(',')])
        item_loader.add_value('external_link', response.url)
        item_loader.add_value('property_type', 'apartment')
        images = response.css('.size-listings-featured-image').xpath('@src').extract()
        item_loader.add_value('images', images)
        item_loader.add_value('currency', 'EUR')
        item_loader.add_value('landlord_name', 'premiercase.it')
        item_loader.add_value('landlord_phone', '0112422009')
        item_loader.add_value('landlord_email', ' info@premiercase.it')
        item_loader.add_value('external_source', self.external_source)
        #have sudden error :: Unsupported attribute. May be typo.
        rent = response.css('.marT0 .listing-price::text').extract()
        if rent and rent[0]:
             item_loader.add_value('rent_string', rent[0][1:])
        desc = ''.join(response.css('#listing-content ::text').extract()[0:17])
        item_loader.add_value('description', desc)

        features = response.css('#listing-features li::text').extract()
        balcony = False
        furnished = False
        elev = False
        for f in features:
            if 'balcon' in f:
                balcony = True
            if 'Arredato' in f:
                furnished = True
            if 'Ascensore' in f:
                elev = True
        item_loader.add_value('furnished', furnished)
        item_loader.add_value('elevator', elev)
        item_loader.add_value('balcony', balcony)



        dt_details = response.css('.energy-class ::text').extract()
        if len(dt_details) == 0:
            dt_details = response.css('td ::text').extract()
        stripped_details = [i.strip().lower() if type(i) == str else str(i) for i in dt_details]

        if 'piano' in stripped_details:
            floor_index = stripped_details.index('piano')
            floor = stripped_details[floor_index + 1]
            item_loader.add_value('floor', floor)
        if 'classe energetica' in stripped_details:
            energy_index = stripped_details.index('classe energetica')
            energy_label = stripped_details[energy_index + 1]
            item_loader.add_value('energy_label', energy_label)
        if 'superficie' in stripped_details:
            sq_index = stripped_details.index('superficie')
            sq = stripped_details[sq_index + 1]
            sqq = [int(s) for s in sq.split() if s.isdigit()]
            if sqq and sqq[0]:
                item_loader.add_value('square_meters', sqq[0])
        sq = response.css('.sqft ::text').extract()
        if sq and sq[1]:
            item_loader.add_value('square_meters', int(sq[1]))

        if 'locali' in stripped_details:
            room_index = stripped_details.index('locali')
            if stripped_details[room_index + 1][0].isdigit():
                room_count = int(stripped_details[room_index + 1][0])
                item_loader.add_value('room_count', room_count)
        if 'letto' in stripped_details:
            room_index = stripped_details.index('letto')
            if stripped_details[room_index + 1][0].isdigit():
                room_count = int(stripped_details[room_index + 1][0])
                item_loader.add_value('room_count', room_count)
        if 'bagni' in stripped_details:
            bathroom_index = stripped_details.index('bagni')
            bathroom_count = int(stripped_details[bathroom_index + 1])
            item_loader.add_value('bathroom_count', bathroom_count)

        data = response.css('#single-listing-propinfo li ::text').extract()
        stripped_data = [i.strip().lower() if type(i) == str else str(i) for i in data]

        if 'letto' in stripped_data:
            room_index = stripped_data.index('letto')
            if stripped_data[room_index + 1][0].isdigit():
                room_count = int(stripped_data[room_index + 1][0])
                item_loader.add_value('room_count', room_count)
        if 'bagno' in stripped_data:
            bathroom_index = stripped_data.index('bagno')
            bathroom_count = int(stripped_data[bathroom_index + 1])
            item_loader.add_value('bathroom_count', bathroom_count)

        yield item_loader.load_item()
