# -*- coding: utf-8 -*-
# Author: Noor

import scrapy
from ..loaders import ListingLoader


class MySpider(scrapy.Spider):
    name = 'immobiliaremolinaro_com'
    allowed_domains = ['immobiliaremolinaro.com']
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    thousand_separator = '.'
    scale_separator = '.'
    start_urls = [
        'https://www.immobiliaremolinaro.com/affitto#/residenziale-c217/sort=p.date_added/order=DESC/limit=22']

    def parse(self, response):
        main_links = response.css('.name a').xpath('@href').extract()
        for main_link in main_links:
            yield scrapy.Request(url=main_link, callback=self.parse2,
                                 dont_filter=True)

    def parse2(self, response):
        item_loader = ListingLoader(response=response)
        title = response.css('.heading-title::text').extract()[0]
        if 'cielo' not in title.lower():
            if 'uffic' not in response.url and 'luxur' not in response.url and 'negozio' not in response.url and 'commercial' not in response.url:
                item_loader.add_value('title', title)
                features=response.css('.options img').extract()
                if '<img src="https://www.immobiliaremolinaro.com/image/cache//servizi/parking-cr-30x30.jpg" alt="parcheggio" class="img-thumbnail">' in features:
                    item_loader.add_value('parking',True)
                else:
                    item_loader.add_value('parking',False)
                item_loader.add_value('external_link', response.url)
                external_id = response.css('.p-model .p-model::text').extract()[0]
                item_loader.add_value('external_id', external_id)
                city = response.css('.p-brand a::text').extract()[0]
                item_loader.add_value('city', city)
                address=response.css('.form-group div ::text').extract()
                item_loader.add_value('address',address[-9].strip()+','+address[-7].strip())

                rent_string = response.css('.product-price::text').extract()[0][:-1]
                item_loader.add_value('rent_string', rent_string)
                if 'studio' in response.url:
                    item_loader.add_value('property_type', 'studio')
                else:
                    item_loader.add_value('property_type', 'apartment')
                description = ''.join(response.css('.journal-custom-tab p::text').extract()).strip()
                item_loader.add_value('description', description)
                images = response.css('#product-gallery img').xpath('@src').extract()
                item_loader.add_value('images', images)
                item_loader.add_value('currency', 'EUR')
                item_loader.add_value('landlord_name', 'Molinaro Real Estate Studio')
                item_loader.add_value('landlord_email', 'info@immobiliaremolinaro.com')
                item_loader.add_value('landlord_phone', '+390632650246')
                item_loader.add_value('external_source', self.external_source)

                features=response.css('.tags li ::text').extract()
                if'Camere'in features:
                    i=features.index('Camere')
                    rooms=int(features[i+2])
                    item_loader.add_value("room_count",rooms)
                if 'Mq Commerciali' in features:
                    i = features.index('Mq Commerciali')
                    sq = int(features[i + 2][:-3].strip())
                    item_loader.add_value("square_meters", sq)
                if 'Piano' in features:
                    i = features.index('Piano')
                    floor = features[i + 2]
                    item_loader.add_value("floor", floor)

                yield item_loader.load_item()