# -*- coding: utf-8 -*-
# Author: Noor
import scrapy
from ..loaders import ListingLoader


class MySpider(scrapy.Spider):
    name = 'bejimmobiliare_it'
    allowed_domains = ['bejimmobiliare.it']
    start_urls = [
        'https://www.bejimmobiliare.it/it/immobili?contratto=2&tipologia=1&provincia=&prezzo_min=&prezzo_max=&mq_min=&mq_max=&rif=&order_by=&order_dir=']
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    thousand_separator = '.'
    scale_separator = '.'

    def parse(self, response):
        pages_numbers = int(response.css('.pagination a::text').extract()[-3])
        for i in range(pages_numbers + 1):
            yield scrapy.Request(
                url='https://www.bejimmobiliare.it/it/immobili?contratto=2&tipologia=1&provincia=&prezzo_min=&prezzo_max=&mq_min=&mq_max=&rif=&order_by=&order_dir=&page=' + str(
                    i),
                callback=self.parse2,
                dont_filter=True)

    def parse2(self, response):
        links = response.css('.title a').xpath('@href').extract()
        for link in links:
            yield scrapy.Request(
                url='https://www.bejimmobiliare.it/it/' + link,
                callback=self.get_property_details,
                dont_filter=True)

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.url)
        address = ''.join(response.css('address ::text').extract()[2:3]).strip()
        item_loader.add_value('address', address)
        city=response.css('address ::text').extract()[3].strip()
        item_loader.add_value('city',city)
        item_loader.add_value('property_type', 'apartment')
        description = response.css('p::text').extract()[:1]
        item_loader.add_value('description', description)
        images = response.css('.img-thumbnail').xpath('@src').extract()
        item_loader.add_value('images', images)
        title = response.css('.page-header::text').extract()[0]
        item_loader.add_value('title', title)
        item_loader.add_value('currency', 'EUR')
        item_loader.add_value('landlord_name', 'B&J Immobiliare')
        item_loader.add_value('landlord_phone', '+39 055700186')
        item_loader.add_value('external_source', self.external_source)

        dt_details = response.css('th::text').extract()
        stripped_details = [i.strip() if type(i) == str else str(i) for i in dt_details]
        dd_values = response.css('td::text').extract()
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
        if 'Locali/vani:' in stripped_details:
            room_index = stripped_details.index('Locali/vani:')
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
        if 'Spese mensili:' in stripped_details:
            i=stripped_details.index('Spese mensili:')
            ut=stripped_values[i]
            item_loader.add_value('utilities',ut)
        location=response.xpath('//meta[@name="geo.position"]').xpath('@content').extract()
        latlng=location[0].split(';')
        item_loader.add_value('latitude',latlng[0])
        item_loader.add_value('longitude',latlng[1])
        yield item_loader.load_item()
