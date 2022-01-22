# -*- coding: utf-8 -*-
# Author: Noor

import scrapy
from ..loaders import ListingLoader


class MySpider(scrapy.Spider):
    name = 'hometorino_com'
    allowed_domains = ['hometorino.com']
    start_urls = ['https://www.hometorino.com/residenziale-affitto/']
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    thousand_separator = '.'
    scale_separator = ','

    def parse(self, response):
        links=response.css('.entry-title a').xpath('@href').extract()
        for link in links:
            yield scrapy.Request(url=link, callback=self.get_property_details, cb_kwargs={'link': link},
                                 dont_filter=True)

    def get_property_details(self, response, link):
        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', link)
        title =  response.css('.active::text').extract()[0]
        item_loader.add_value('title', title)
        item_loader.add_value('city',title.split(',')[1])
        item_loader.add_value('currency', 'EUR')
        address = response.css('.icon-location+ .meta-inner-wrapper .meta-item-value::text').extract()[0]
        item_loader.add_value('address', address)
        rooms=response.css('.icon-bed+ .meta-inner-wrapper .meta-item-value ::text').extract()[0]
        item_loader.add_value('room_count', int(rooms))
        bath=response.css('.icon-bath+ .meta-inner-wrapper .meta-item-value ::text').extract()[0]
        item_loader.add_value('bathroom_count', int(bath))
        sq=response.css('.icon-area+ .meta-inner-wrapper .meta-item-value ::text').extract()[0]
        item_loader.add_value('square_meters', int(sq))
        desc=''.join(response.css('#main p ::text').extract()).strip()
        item_loader.add_value('description',desc)
        images = response.css('.swipebox img').xpath('@src').extract()
        item_loader.add_value('images', images)
        rent_string = response.css('.price::text').extract()[0][1:-4].strip()
        item_loader.add_value('rent_string', rent_string)
        item_loader.add_value('landlord_name', 'hometorino')
        item_loader.add_value('landlord_phone', '347 7933475')
        item_loader.add_value('landlord_email', 'home.mediazione@gmail.com')
        item_loader.add_value('external_source', self.external_source)

        l=response.css('.property-location-section script::text').extract()[0]
        ll=l[l.index('lat')+1:l.index('","i')]
        lat=ll[ll.index('":"')+3:ll.index('","')]
        lng=ll[ll.rfind('":"')+4:]
        item_loader.add_value('latitude',lat)
        item_loader.add_value('longitude',lng)
        item_loader.add_value('property_type','apartment')

        details = response.css('.property-additional-details-list li ::text').extract()
        if 'Piano' in details:
            i=details.index('Piano')
            item=details[i+1]
            item_loader.add_value('floor',item)
        if 'Spese Condominiali' in details:
            i=details.index('Spese Condominiali')
            item=details[i+1]
            item_loader.add_value('utilities',item.split(' ')[0])
        if 'Ascensore' in details:
            i=details.index('Ascensore')
            item=details[i+1]
            elev=True if item =='si' else False
            item_loader.add_value('elevator',elev)
        if 'Classe energetica' in details:
            i=details.index('Classe energetica')
            item=details[i+1]
            item_loader.add_value('energy_label',item[0])
        if 'Balcone' in details:
            i=details.index('Balcone')
            item=details[i+1]
            b=True if 'si'in item else False
            item_loader.add_value('balcony',b)

        yield item_loader.load_item()
