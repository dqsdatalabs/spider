# -*- coding: utf-8 -*-
# Author: Noor

import scrapy
from ..loaders import ListingLoader


class MySpider(scrapy.Spider):
    name = 'studiocasaimmobiliare_it'
    allowed_domains = ['studiocasaimmobiliare.it']
    start_urls = [
        'https://www.studiocasaimmobiliare.it/it/immobili?contratto=2&tipologia%5B0%5D=1&provincia=&prezzo_min=&prezzo_max=&rif=']
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    thousand_separator = '.'
    scale_separator = '.'

    def parse(self, response):
        pages_number = int(response.css('#property-listing li a::text').extract()[-3])
        start_urls = []
        for i in range(pages_number+1):
            start_urls.append(
                'https://www.studiocasaimmobiliare.it/it/immobili?contratto=2&tipologia%5B0%5D=1&provincia=&prezzo_min=&prezzo_max=&rif=&page=' + str(
                    i))
        for url in start_urls:
            yield scrapy.Request(
                url=url,
                dont_filter=True,
                callback=self.parse2,
            )

    def parse2(self, response):
        links = response.css('.property-details a').xpath('@href').extract()
        for link in links:
            yield scrapy.Request(
                url='https://www.studiocasaimmobiliare.it/' + link,
                callback=self.get_property_details,
                dont_filter=True)

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.url)
        address = response.css('p span::text').extract()[0]
        item_loader.add_value('address', address)
        item_loader.add_value('city',address[-4:])
        sq = response.css('.property-header li::text').extract()[2]
        sq_meters = [int(s) for s in sq.split() if s.isdigit()][0]
        item_loader.add_value('square_meters', sq_meters)
        id = response.css('.property-header li::text').extract()[1]
        external_id = str([int(s) for s in id.split() if s.isdigit()][0])
        item_loader.add_value('external_id', external_id)
        rent = response.css('.property-header li::text').extract()[0][2:]
        item_loader.add_value('rent_string', rent)
        item_loader.add_value('property_type', 'apartment')
        description = response.css('.single-property-details p::text').extract()[0]
        item_loader.add_value('description', description)
        images = response.css('.item a img').xpath('@src').extract()
        item_loader.add_value('images', images)
        title = response.css('.banner-inner h2::text').extract()[0]
        item_loader.add_value('title', title)
        item_loader.add_value('currency', 'EUR')
        item_loader.add_value('landlord_name', 'studiocasaimmobiliare')
        item_loader.add_value('landlord_phone', '050970896')
        item_loader.add_value('external_source', self.external_source)

        bathrooms = response.css('.property-header li::text').extract()[4]
        bathroom_count = [int(s) for s in bathrooms.split() if s.isdigit()]
        if bathroom_count and bathroom_count[0]:
            item_loader.add_value('bathroom_count', bathroom_count[0])

        features=response.css('.amenities-checkbox ::text').extract()
        if 'Locali/vani: ' in features:
            indx=features.index('Locali/vani: ')
            item_loader.add_value('room_count',features[indx+1])

        if 'Posti auto: ' in features:
            indx=features.index('Posti auto: ')
            prking=features[indx+1]
            if prking in ['1','2','3']:
                item_loader.add_value('parking',True)
            else:
                item_loader.add_value('parking',False)

        if 'Spese mensili: ' in features:
            indx=features.index('Spese mensili: ')
            item_loader.add_value('utilities',features[indx+1][:2])

        yield item_loader.load_item()