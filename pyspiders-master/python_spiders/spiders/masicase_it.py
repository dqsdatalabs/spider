# -*- coding: utf-8 -*-
# Author: Noor

import scrapy
from ..loaders import ListingLoader


class MySpider(scrapy.Spider):
    name = 'masicase_it'
    allowed_domains = ['masicase.it']
    start_urls = ['https://www.masicase.it/risultati-ricerca/?lang=']
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    thousand_separator = '.'
    scale_separator = '.'

    def parse(self, response):
        links = response.css('.cbuttontitle').xpath('@href').extract()
        for link in links:
            yield scrapy.Request(
                url=link,
                callback=self.get_property_details,
                dont_filter=True)

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        title = response.css('h1::text').extract()[0]
        item_loader.add_value('title', title)
        if 'affitto' in response.url and 'AFFITTO' in title:
            item_loader.add_value('external_link', response.url)
            item_loader.add_value('property_type', 'apartment')
            imgs = response.css('#carousel img').xpath('@src').extract()
            images = [i[10:] for i in imgs]
            item_loader.add_value('images', images)
            item_loader.add_value('currency', 'EUR')
            item_loader.add_value('landlord_name', 'masicase.it')
            item_loader.add_value('landlord_phone', '051-349692')
            item_loader.add_value('landlord_email', 'info@masicase.com')
            item_loader.add_value('external_source', self.external_source)
            rent = response.css('#price_detailpage::text').extract()[0].strip()
            item_loader.add_value('rent_string', rent)

            dt_details = response.css('td ::text').extract()
            stripped_details = [i.strip() if type(i) == str else str(i) for i in dt_details]

            if 'Arredato' in stripped_details:
                fur_index = stripped_details.index('Arredato')
                furnished = stripped_details[fur_index + 1]
                if furnished == 'Completamente arredato':
                    item_loader.add_value('furnished', True)
                else:
                    item_loader.add_value('furnished', False)
            if 'Ascensore' in stripped_details:
                elev_index = stripped_details.index('Ascensore')
                elevator = stripped_details[elev_index + 1]
                if elevator == 'Con ascensore':
                    item_loader.add_value('elevator', True)
                else:
                    item_loader.add_value('elevator', False)
            if 'Balconi' in stripped_details:
                balc_index = stripped_details.index('Balconi')
                balcony = stripped_details[balc_index + 1]
                if balcony == 'Con balcone':
                    item_loader.add_value('balcony', True)
                else:
                    item_loader.add_value('balcony', False)
            if 'Piano' in stripped_details:
                floor_index = stripped_details.index('Piano')
                floor = stripped_details[floor_index + 1]
                item_loader.add_value('floor', floor)
            if 'Classe energetica' in stripped_details:
                energy_index = stripped_details.index('Classe energetica')
                energy_label = stripped_details[energy_index + 1]
                item_loader.add_value('energy_label', energy_label)

            dt_details2 = response.css('.sidebarwidget ul li ::text').extract()
            stripped_details2 = [i.strip() if type(i) == str else str(i) for i in dt_details2]

            if 'Metratura' in stripped_details2:
                sq_index = stripped_details2.index('Metratura')
                sq = int(stripped_details2[sq_index + 2][:-3].strip())
                item_loader.add_value('square_meters', sq)
            if 'Metratura Lotto' in stripped_details2:
                sq_index = stripped_details2.index('Metratura Lotto')
                sq = int(stripped_details2[sq_index + 2][:-3].strip())
                item_loader.add_value('square_meters', sq)
            if 'Vani' in stripped_details2:
                room_index = stripped_details2.index('Vani')
                room_count = int(stripped_details2[room_index + 2])
                item_loader.add_value('room_count', room_count)
            if 'Bagni' in stripped_details2:
                bathroom_index = stripped_details2.index('Bagni')
                bathroom_count = int(stripped_details2[bathroom_index + 2])
                item_loader.add_value('bathroom_count', bathroom_count)

            yield item_loader.load_item()
