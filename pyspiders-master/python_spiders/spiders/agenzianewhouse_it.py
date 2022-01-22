# -*- coding: utf-8 -*-
# Author: Noor
import scrapy
from ..loaders import ListingLoader


class MySpider(scrapy.Spider):
    name = 'agenzianewhouse_it'
    allowed_domains = ['agenzianewhouse.it']
    start_urls = [
        'https://www.agenzianewhouse.it/it/immobili?contratto=2&tipologia=1&provincia=&prezzo_min=&prezzo_max=&rif=&order_by=&order_dir=']
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    thousand_separator = '.'
    scale_separator = '.'

    def parse(self, response):
        print('parse')
        pages_numbers = int(response.css('.pagination a::text').extract()[-3])
        for i in range(1,pages_numbers+1):
            yield scrapy.Request(
                url='https://www.agenzianewhouse.it/it/immobili?contratto=2&tipologia=1&provincia=&prezzo_min=&prezzo_max=&rif=&order_by=&order_dir=&page=' + str(i),
                callback=self.parse2,
                dont_filter=True)

    def parse2(self, response):
        print('parse2')
        links = response.css('.span4 a').xpath('@href').extract()[2:]
        for link in links:
            print('goto details')
            yield scrapy.Request(
                url='https://www.agenzianewhouse.it/it/' + link,
                callback=self.get_property_details,
                dont_filter=True)

    def get_property_details(self, response):
        print('details')
        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.url)
        address = ''.join(response.css('address ::text').extract()[2:3]).strip()
        item_loader.add_value('address', address)
        item_loader.add_value('property_type', 'apartment')
        description = ''.join(response.css('#main p::text').extract()[0].strip())
        item_loader.add_value('description', description)
        images = response.css('.img-thumbnail').xpath('@src').extract()
        item_loader.add_value('images', images)
        title = response.css('.page-header::text').extract()[0]
        item_loader.add_value('title', title)
        item_loader.add_value('currency', 'EUR')
        item_loader.add_value('landlord_name', 'agenzianewhouse.it')
        item_loader.add_value('landlord_phone', '+39050572879')
        item_loader.add_value('external_source', self.external_source)
        dt_details =response.css('li label::text').extract()
        stripped_details = [i.strip() if type(i) == str else str(i) for i in dt_details]
        dd_values = response.css('li span::text').extract()
        stripped_values = [i.strip() if type(i) == str else str(i) for i in dd_values]

        if 'Rif.:' in stripped_details:
            id_index = stripped_details.index('Rif.:')
            external_id = stripped_values[id_index]
            item_loader.add_value('external_id', external_id)
        if 'Arredato:' in stripped_details:
            fur_index = stripped_details.index('Arredato:')
            furnished = stripped_values[fur_index]
            if furnished == 'arredato':
                item_loader.add_value('furnished', True)
            else:
                item_loader.add_value('furnished', False)
        if 'Camere totali:' in stripped_details:
            room_index = stripped_details.index('Camere totali:')
            rooms = int(stripped_values[room_index])
            item_loader.add_value('room_count', rooms)
        if 'Bagni:' in stripped_details:
            bath_index = stripped_details.index('Bagni:')
            bath = int(stripped_values[bath_index])
            item_loader.add_value('bathroom_count', bath)
        if 'Piano:' in stripped_details:
            floor_index = stripped_details.index('Piano:')
            floor = stripped_values[floor_index]
            item_loader.add_value('floor', floor)

        if 'Superficie:' in stripped_details:
            sq_index = stripped_details.index('Superficie:')
            sq = stripped_values[sq_index]
            sq_meters = [int(s) for s in sq.split() if s.isdigit()][0]
            item_loader.add_value('square_meters', sq_meters)

        if 'Prezzo:' in stripped_details:
            rent_index = stripped_details.index('Prezzo:')
            rent = stripped_values[rent_index][2:]
            item_loader.add_value('rent_string', rent)

        yield item_loader.load_item()
