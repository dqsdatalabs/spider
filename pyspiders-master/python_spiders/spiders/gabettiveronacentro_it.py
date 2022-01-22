# -*- coding: utf-8 -*-
# Author: Noor
import scrapy
from ..loaders import ListingLoader
import re


class MySpider(scrapy.Spider):
    name = 'gabettiveronacentro_it'
    allowed_domains = ['gabettiveronacentro.it']
    start_urls = [
        'https://www.gabettiveronacentro.it/it/immobili/pg~1/?__id_reg=&cerca=cerca&__id_cat=0&__id_cont=A&__id_com=0&__id_zon=0&__id_tipo=57&__id_sup=0&__id_prezzo_a=0&__keywords=']
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    thousand_separator = '.'
    scale_separator = ','

    def parse(self, response):
        links = response.css('.text-center a').xpath('@href').extract()
        for link in links:
            yield scrapy.Request(
                url=link,
                callback=self.get_property_details,
                dont_filter=True)

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.url)
        item_loader.add_value('property_type', 'apartment')
        description = response.css('.testo-interno-annuncio p::text').extract()[0].strip()
        item_loader.add_value('description', description)
        images = response.css('.sin-slide img').xpath('@src').extract()
        item_loader.add_value('images', images)
        title = response.css('.mr-30-min ::text').extract()[1]
        item_loader.add_value('title', title)
        city = response.css('.mr-30-min ::text').extract()[3]
        item_loader.add_value('city', city)
        item_loader.add_value('currency', 'EUR')
        item_loader.add_value('landlord_name', 'gabettiveronacentro')
        item_loader.add_value('landlord_email', 'info@gabettiveronacentro.it')
        item_loader.add_value('landlord_phone', '045 8032023')
        item_loader.add_value('external_source', self.external_source)
        id = response.css('.location::text').extract()[0][6:]
        item_loader.add_value('external_id', id)
        address=response.css('.subtitle+ h3::text').extract()[0].strip()
        item_loader.add_value('address',address)

        features = response.css('.summery li ::text').extract()
        if 'Prezzo' in features:
            indx = features.index('Prezzo')
            rent = features[indx + 1]
            item_loader.add_value('rent_string', rent[2:])

        ut=response.css('.feature li ::text').extract()
        if ut and ut[0]:
            utility = ut[0]
            utilities = utility[utility.rfind('€') + 1:].strip()
            item_loader.add_value('utilities', utilities)

        yield item_loader.load_item()
