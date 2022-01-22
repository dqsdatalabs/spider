# -*- coding: utf-8 -*-
# Author: Noor

import scrapy
from ..loaders import ListingLoader


class MySpider(scrapy.Spider):
    name = 'avmilano_com'
    allowed_domains = ['avmilano.com']
    start_urls = [
        'https://www.avmilano.com/properties-search-results-2/?sort=newest&search_city&search_lat&search_lng&search_category=0&search_type=0&search_min_price&search_max_price']
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    thousand_separator = '.'
    scale_separator = '.'

    def parse(self, response):
        pages_number = int(response.css('.pagination a::text').extract()[-1])
        start_urls = []
        for i in range(pages_number + 1):
            start_urls.append(
                'https://www.avmilano.com/properties-search-results-2/page/' + str(
                    i) + '/?sort=newest&search_city&search_lat&search_lng&search_category=0&search_type=0&search_min_price&search_max_price')
        for url in start_urls:
            yield scrapy.Request(
                url=url,
                dont_filter=True,
                callback=self.parse2,
            )

    def parse2(self, response):
        links = response.css('.card').xpath('@href').extract()
        for link in links:
            yield scrapy.Request(
                url=link,
                callback=self.get_property_details,
                dont_filter=True)

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)

        rent = response.css('.listPrice::text').extract()[0].strip().replace('.','').replace('€','')
        rent_int=[int(s) for s in rent.split() if s.isdigit()]
        if rent_int and rent_int[0]:
            item_loader.add_value('rent_string', str(rent_int[0]))
            if rent_int[0]<5000 and 'local' in response.url:
                item_loader.add_value('external_link', response.url)
                address = ''.join(response.css('.address::text').extract()).strip()
                item_loader.add_value('address', address)
                splitted_address=address.split(',')
                item_loader.add_value('city',splitted_address[1].strip())
                features = response.css('.features li ::text').extract()
                if features:
                    if features[0]:
                        room = features[0]
                        room_count = [int(s) for s in room.split() if s.isdigit()][0]
                        item_loader.add_value('room_count', room_count)
                    if features[1]:
                        bathroom = features[1]
                        bathroom_count = [int(s) for s in bathroom.split() if s.isdigit()][0]
                        item_loader.add_value('bathroom_count', bathroom_count)
                    if features[2]:
                        sq = features[2]
                        sq_meters = [int(s) for s in sq.split() if s.isdigit()][0]
                        item_loader.add_value('square_meters', sq_meters)
                external_id = response.css('.property-id::text').extract()[0].strip()[13:]
                item_loader.add_value('external_id', external_id)
                item_loader.add_value('property_type', 'apartment')
                description = "".join(response.css('.entry-content > p ::text').extract())
                item_loader.add_value('description', description)
                images = response.css('.carousel-inner a').xpath('@href').extract()
                item_loader.add_value('images', images)
                title = response.css('.pageTitle::text').extract()[0]
                item_loader.add_value('title', title)
                item_loader.add_value('currency', 'EUR')
                item_loader.add_value('landlord_name', 'AV Milano')
                item_loader.add_value('landlord_email', 'info@avmilano.com')
                item_loader.add_value('landlord_phone', '+39 371 14 61 477')
                item_loader.add_value('external_source', self.external_source)
                services=response.css('.amenities div div::text').extract()
                if ' parcheggio privato' in services:
                    item_loader.add_value('parking',True)
                else:
                    item_loader.add_value('parking',False)

                additional = response.css('.additional div div ::text').extract()
                if 'Arredamento' in additional:
                    indx=additional.index('Arredamento')+1
                    if additional[indx]==': Arredata':
                        item_loader.add_value('furnished',True)
                    else:
                        item_loader.add_value("furnished",False)
                if 'Spese' in additional:
                    i=additional.index('Spese')+1
                    item_loader.add_value('utilities',additional[i])
                if 'Piano' in additional:
                    i=additional.index('Piano')+1
                    item_loader.add_value('floor',additional[i][1:].strip())




                yield item_loader.load_item()
