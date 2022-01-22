# -*- coding: utf-8 -*-
# Author: Noor

import scrapy
from ..loaders import ListingLoader


class MySpider(scrapy.Spider):
    name = 'spaziocasa2000_it'
    allowed_domains = ['spaziocasa2000.it']
    start_urls = [
        'https://www.spaziocasa2000.it/ricerca-avanzata/page/2/?wpestate_regular_search_nonce=0c0a780b99&_wp_http_referer=%2Fricerca-avanzata%2F%3Ffilter_search_action%255B%255D%3D%26filter_search_type%255B%255D%3Dappartamento%26advanced_city%3D%26advanced_area%3D%26camere%3D%26bagni%3D%26prezzo-min-senza-punti%3D%26prezzo-max-senza-punti%3D%26submit%3DRICERCA%2BIMMOBILE%26wpestate_regular_search_nonce%3D0c0a780b99%26_wp_http_referer%3D%252Fcategoria%252Faffitto%252F&filter_search_action%5B0%5D=affitto&filter_search_type%5B0%5D=appartamento&advanced_city&advanced_area&camere&bagni&prezzo-min-senza-punti&prezzo-max-senza-punti']
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    thousand_separator = '.'
    scale_separator = '.'

    def parse(self, response):
        pages_number = int(response.css('.pagination_nojax a::text').extract()[-1])
        start_urls = []
        for i in range(pages_number + 1):
            start_urls.append('https://www.spaziocasa2000.it/ricerca-avanzata/page/' + str(
                i) + '/?wpestate_regular_search_nonce=0c0a780b99&_wp_http_referer=%2Fricerca-avanzata%2F%3Ffilter_search_action%255B%255D%3D%26filter_search_type%255B%255D%3Dappartamento%26advanced_city%3D%26advanced_area%3D%26camere%3D%26bagni%3D%26prezzo-min-senza-punti%3D%26prezzo-max-senza-punti%3D%26submit%3DRICERCA%2BIMMOBILE%26wpestate_regular_search_nonce%3D0c0a780b99%26_wp_http_referer%3D%252Fcategoria%252Faffitto%252F&filter_search_action%5B0%5D=affitto&filter_search_type%5B0%5D=appartamento&advanced_city&advanced_area&camere&bagni&prezzo-min-senza-punti&prezzo-max-senza-punti')
        for url in start_urls:
            yield scrapy.Request(
                url=url,
                dont_filter=True,
                callback=self.parse2,
            )

    def parse2(self, response):
        links = response.css('h4  a').xpath('@href').extract()
        for link in links:
            yield scrapy.Request(
                url=link,
                callback=self.get_property_details,
                dont_filter=True)

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        title = response.css('.entry-prop::text').extract()[0]
        item_loader.add_value('title', title)
        item_loader.add_value('external_link', response.url)
        address = response.css('.col-md-4 a::text').extract()
        item_loader.add_value('address', address[0] + ', ' + address[1])
        item_loader.add_value('city',address[0])
        item_loader.add_value('property_type', 'apartment')
        description = response.css('.wpestate_property_description p ::text').extract()[0]
        item_loader.add_value('description', description)
        images = response.css('.prettygalery').xpath('@href').extract()
        item_loader.add_value('images', images)
        item_loader.add_value('currency', 'EUR')
        item_loader.add_value('landlord_name', 'spaziocasa.it')
        item_loader.add_value('landlord_phone', '051 309637')
        item_loader.add_value('landlord_email', 'info@spaziocasa2000.it')
        item_loader.add_value('external_source', self.external_source)

        dt_details = response.css('.panel-body ::text').extract()
        stripped_details = [i.strip() if type(i) == str else str(i) for i in dt_details]
        if 'ID proprietà:' in stripped_details:
            id_index = stripped_details.index('ID proprietà:')
            id = stripped_details[id_index + 1]
            item_loader.add_value('external_id', id)
        if 'Prezzo:' in stripped_details:
            rent_index = stripped_details.index('Prezzo:')
            rent = stripped_details[rent_index + 1][2:]
            item_loader.add_value('rent_string', rent)
        if 'Dimensioni:' in stripped_details:
            sq_index = stripped_details.index('Dimensioni:')
            sq = stripped_details[sq_index + 1]
            sq_meters = sq[:-4]
            item_loader.add_value('square_meters', sq_meters)
        if 'Camere:' in stripped_details:
            room_index = stripped_details.index('Camere:')
            room_count = int(stripped_details[room_index + 1])
            item_loader.add_value('room_count', room_count)
        if 'Bagni:' in stripped_details:
            bathroom_index = stripped_details.index('Bagni:')
            bathroom_count = int(stripped_details[bathroom_index + 1])
            item_loader.add_value('bathroom_count', bathroom_count)
        if 'Classe energetica:' in stripped_details:
            energy_index = stripped_details.index('Classe energetica:')
            energy_label = stripped_details[energy_index + 1]
            item_loader.add_value('energy_label', energy_label)
        yield item_loader.load_item()
