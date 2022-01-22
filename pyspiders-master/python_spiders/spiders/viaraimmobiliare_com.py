# -*- coding: utf-8 -*-
# Author: Noor

import scrapy
from ..loaders import ListingLoader


class MySpider(scrapy.Spider):
    name = 'viaraimmobiliare_com'
    allowed_domains = ['viaraimmobiliare.com']
    start_urls = ['https://www.viaraimmobiliare.com/tipo/locazione/']
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    thousand_separator = '.'
    scale_separator = '.'

    def parse(self, response):
        pages_number = int(response.css('.pagination a::text').extract()[-1])
        for i in range(1, pages_number):
            yield scrapy.Request(
                url='https://www.viaraimmobiliare.com/tipo/locazione/page/' + str(i) + '/',
                callback=self.parse2,
                dont_filter=True)

    def parse2(self, response):
        links = response.css('h4 a').xpath('@href').extract()
        for link in links:
            yield scrapy.Request(
                url=link,
                callback=self.get_property_details,
                dont_filter=True)

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        ##
        title = response.css('.entry-title ::text').extract()[-1]
        item_loader.add_value('title', title)
        item_loader.add_value('external_link', response.url)
        item_loader.add_value('property_type', 'apartment')
        ##
        images = response.css('.lightbox_trigger').xpath('@style').extract()
        imgs=[i[20:] for i in images]
        item_loader.add_value('images', imgs)

        ##
        item_loader.add_value('currency', 'EUR')
        item_loader.add_value('landlord_name', 'viaraimmobiliare.com')
        item_loader.add_value('landlord_phone', '+39 338 8883370')
        item_loader.add_value('landlord_email', 'info@viaraimmobiliare.com')
        item_loader.add_value('external_source', self.external_source)

        desc = response.css('.wpestate_estate_property_details_section p::text').extract()[0]
        item_loader.add_value('description', desc)

        ##
        features = response.css('.property-panel .listing_detail  ::text').extract()
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
        if 'Indirizzo:' in features:
            addr_index = features.index('Indirizzo:')
            address = features[addr_index + 1]
            item_loader.add_value('address', address)

        if 'C.A.P.:' in features:
            zip_index = features.index('C.A.P.:')
            zipcode = features[zip_index + 1]
            item_loader.add_value('zipcode', zipcode)
        if 'Città:' in features:
            index = features.index('Città:')
            city = features[index + 2]
            item_loader.add_value('city', city)

        dt_details = response.css('.col-md-6 ::text').extract()
        stripped_details = [i.strip().lower() if type(i) == str else str(i) for i in dt_details]

        if 'piano:' in stripped_details:
            floor_index = stripped_details.index('piano:')
            floor = stripped_details[floor_index + 1]
            item_loader.add_value('floor', floor)
        if 'prezzo:' in stripped_details:
            rent_index = stripped_details.index('prezzo:')
            rent = stripped_details[rent_index + 1]
            item_loader.add_value('rent_string', rent)
        if 'classe energetica:' in stripped_details:
            energy_index = stripped_details.index('classe energetica:')
            energy_label = stripped_details[energy_index + 1]
            item_loader.add_value('energy_label', energy_label)
        if 'dimensione della proprietà:' in stripped_details:
            sq_index = stripped_details.index('dimensione della proprietà:')
            sq = int(stripped_details[sq_index + 1][0:2])
            item_loader.add_value('square_meters', sq)

        if 'camera/e da letto:'  in stripped_details:
            room_index = stripped_details.index('camera/e da letto:')
            if stripped_details[room_index + 1][0].isdigit():
                room_count = int(stripped_details[room_index + 1][0])
                item_loader.add_value('room_count', room_count)
        elif 'locali:' in stripped_details:
            room_index = stripped_details.index('locali:')
            if stripped_details[room_index + 1][0].isdigit():
                room_count = int(stripped_details[room_index + 1][0])
                item_loader.add_value('room_count', room_count)

        if 'bagni:' in stripped_details:
            bathroom_index = stripped_details.index('bagni:')
            bathroom_count = int(stripped_details[bathroom_index + 1])
            item_loader.add_value('bathroom_count', bathroom_count)
        if 'id proprietà:'  in stripped_details:
            index = stripped_details.index('id proprietà:')
            id=stripped_details[index + 1]
            item_loader.add_value('external_id', id)

        yield item_loader.load_item()
