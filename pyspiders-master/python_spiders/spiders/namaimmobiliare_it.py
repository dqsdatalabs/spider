
# -*- coding: utf-8 -*-
# Author: Noor

import scrapy
from ..loaders import ListingLoader


class MySpider(scrapy.Spider):
    name = 'namaimmobiliare_it'
    allowed_domains = ['namaimmobiliare.it']
    start_urls = ['https://www.namaimmobiliare.it/?s&property_status=rent&property_type&post_type=tm-property']
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    thousand_separator = '.'
    scale_separator = '.'

    def parse(self, response):
        pages_number = int(response.css('.pagination a::text').extract()[-2])
        for i in range(1, pages_number+1):
            yield scrapy.Request(
                url='https://www.namaimmobiliare.it/page/'+str(i)+'/?s&property_status=rent&property_type&post_type=tm-property',
                callback=self.parse2,
                dont_filter=True)

    def parse2(self, response):
        links = response.css('.tm-property__more').xpath('@href').extract()
        for link in links:
            yield scrapy.Request(
                url=link,
                callback=self.get_property_details,
                dont_filter=True)

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        if 'negozio' not in response.url:
            title = response.css('.entry-title ::text').extract()[-1]
            item_loader.add_value('title', title)
            item_loader.add_value('external_link', response.url)
            item_loader.add_value('property_type', 'apartment')
            images = response.css('.swiper-slide img').xpath('@src').extract()
            item_loader.add_value('images', images)
            item_loader.add_value('currency', 'EUR')
            item_loader.add_value('landlord_name', 'namaimmobiliare.it')
            item_loader.add_value('landlord_phone', '39 08118912648')
            item_loader.add_value('landlord_email', 'namaimmobiliare9@gmail.com')
            item_loader.add_value('external_source', self.external_source)
            rent = response.css('#main .tm-property__price-value::text').extract()[0]
            item_loader.add_value('rent_string', rent)
            desc = response.css('.tm-property__subtitle+ p::text').extract()[0]
            item_loader.add_value('description', desc)

            dt_details = response.css('dt ::text').extract()
            stripped_details = [i.strip().lower() if type(i) == str else str(i) for i in dt_details]
            dt_values = response.css('dd ::text').extract()
            stripped_values = [i.strip().lower() if type(i) == str else str(i) for i in dt_values]

            if 'piano:' in stripped_details:
                floor_index = stripped_details.index('piano:')
                floor = stripped_values[floor_index]
                item_loader.add_value('floor', floor)
            if 'classe energetica:' in stripped_details:
                energy_index = stripped_details.index('classe energetica:')
                energy_label = stripped_values[energy_index]
                item_loader.add_value('energy_label', energy_label)
            if 'superficie:' in stripped_details:
                sq_index = stripped_details.index('superficie:')
                sq = int(stripped_values[sq_index][:stripped_values[sq_index].index('.')])
                item_loader.add_value('square_meters', sq)
            if 'camere da letto:' in stripped_details:
                room_index = stripped_details.index('camere da letto:')
                if stripped_values[room_index][0].isdigit():
                    room_count = int(stripped_values[room_index][0])
                    item_loader.add_value('room_count', room_count)
            elif 'Bilocale' in title:
                item_loader.add_value('room_count',2)
            if 'bagni:' in stripped_details:
                bathroom_index = stripped_details.index('bagni:')
                bathroom_count = int(stripped_values[bathroom_index])
                item_loader.add_value('bathroom_count', bathroom_count)
            if 'cod. rif:'  in stripped_details:
                index = stripped_details.index('cod. rif:')
                id = stripped_values[index]
                item_loader.add_value('external_id', id)

            yield item_loader.load_item()
