# -*- coding: utf-8 -*-
# Author: Noor

import scrapy
from ..loaders import ListingLoader


class MySpider(scrapy.Spider):
    name = 'cornercasa_it'
    allowed_domains = ['cornercasa.it']
    start_urls = [
    'https://www.cornercasa.it/property-type/affitto/']
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    thousand_separator = '.'
    scale_separator = '.'

    # def parse(self, response):
    #     pages_number = int(response.css('.page-numbers::text').extract()[-2])
    #     start_urls = []
    #     for i in range(pages_number+1):
    #         start_urls.append('https://www.cornercasa.it/properties/page/'+str(i)+'/?filter-location&filter-amenity&filter-status=96')
    #
    #     for url in start_urls:
    #         yield scrapy.Request(
    #             url=url,
    #             dont_filter=True,
    #             callback=self.parse2,
    #         )

    def parse(self, response):
        links = response.css('.property-box-read-more a').xpath('@href').extract()
        rents=response.css('.property-box-price::text').extract()
        titles=response.css('.property-box-title h3 a::text').extract()
        for link in links:
            i=links.index(link)
            rent=rents[i].strip()
            title=titles[i]
            yield scrapy.Request(
                url= link,
                callback=self.get_property_details,
                dont_filter=True,
            cb_kwargs={'rent':rent,'title':title})

    def get_property_details(self, response,rent,title):
        if 'appartament' in title.lower():
            item_loader = ListingLoader(response=response)
            item_loader.add_value('title',title)
            item_loader.add_value('external_link', response.url)
            title_info=title.split('–')
            city=title_info[1]
            item_loader.add_value('city',city)

            external_id = title_info[-1].strip()
            item_loader.add_value('external_id', external_id)
            item_loader.add_value('rent_string', rent)
            item_loader.add_value('property_type', 'apartment')
            description =  response.css('.property-description p::text').extract()[0]
            item_loader.add_value('description', description)

            images = response.css('#immagini-immobile img').xpath('@src').extract()
            item_loader.add_value('images', images)

            item_loader.add_value('currency', 'EUR')
            item_loader.add_value('landlord_name', 'cornercasa.it')
            item_loader.add_value('landlord_phone', '+39 041 5240883')
            item_loader.add_value('landlord_email', 'info@cornercasa.it')
            item_loader.add_value('external_source', self.external_source)

            dt_details = response.css('dt::text').extract()
            stripped_details = [i.strip() if type(i) == str else str(i) for i in dt_details]
            dd_values = response.css('dd::text').extract()
            stripped_values = [i.strip() if type(i) == str else str(i) for i in dd_values]

            if 'Mq' in stripped_details:
                sq_index = stripped_details.index('Mq')
                sq = stripped_values[sq_index]
                sq_meters = [int(s) for s in sq.split() if s.isdigit()][0]
                item_loader.add_value('square_meters', sq_meters)

            if 'Camere' in stripped_details:
                room_index=stripped_details.index('Camere')
                room_count=int(stripped_values[room_index])
                item_loader.add_value('room_count', room_count)

            yield item_loader.load_item()
