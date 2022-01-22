# -*- coding: utf-8 -*-
# Author: Noor
import scrapy
from ..loaders import ListingLoader

class MySpider(scrapy.Spider):
    name = 'studiopedone_com'
    allowed_domains = ['studiopedone.com']
    start_urls = ['https://www.studiopedone.com/cerca-immobili/?type%5B%5D=appartamento-2&status=a']
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    thousand_separator = '.'
    scale_separator = ','

    def parse(self, response):
        links =response.css('.rh_list_card__details a').xpath('@href').extract()
        for link in links:
            yield scrapy.Request(
                url=link,
                callback=self.get_property_details,
                dont_filter=True)

    def get_property_details(self, response):
        ##
        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.url)
        address =response.css('.rh_page__property_address::text').extract()[0].strip()
        item_loader.add_value('address', address)
        item_loader.add_value('city',address.split(' ')[-1])
        item_loader.add_value('zipcode',address.split(' ')[-2])
        item_loader.add_value('property_type', 'apartment')
        description =response.css('.rh_content p::text').extract()[0].strip()
        item_loader.add_value('description', description)
        images = response.css('#property-detail-flexslider img').xpath('@src').extract()
        item_loader.add_value('images', images)
        title = response.css('.rh_page__title::text').extract()[0].strip()
        item_loader.add_value('title', title)
        item_loader.add_value('currency', 'EUR')
        item_loader.add_value('landlord_name', 'PEDONE')
        item_loader.add_value('landlord_phone', '+39 011 8127420')
        item_loader.add_value('landlord_email','segreteria@studiopedone.com')
        item_loader.add_value('external_source', self.external_source)
        external_id = response.css('.id::text').extract()[0].strip()
        item_loader.add_value('external_id', external_id)
        rent = response.css('.price::text').extract()[0][2:]
        item_loader.add_value('rent_string', rent)

        _features=response.css('.rh_property__meta_wrap ::text').extract()
        features=[]
        for i in _features:
            if i != ' ':
                features.append(i)
        if ' Camere da letto ' in features:
            i=features.index(' Camere da letto ')
            item_loader.add_value('room_count',int(features[i+1]))
        if ' Bagni ' in features:
            i=features.index(' Bagni ')
            item_loader.add_value('bathroom_count',int(features[i+1]))
        if ' Area ' in features:
            i=features.index(' Area ')
            item_loader.add_value('square_meters',int(features[i+1]))



        dt_details =response.css('li span ::text').extract()
        stripped_details = [i.strip() if type(i) == str else str(i) for i in dt_details]

        if 'Arredato:' in stripped_details:
            fur_index = stripped_details.index('Arredato:')
            furnished = stripped_details[fur_index+1]
            if furnished not in['Riscaladamento:','']:
                if furnished == 'completo':
                    item_loader.add_value('furnished', True)
                else:
                    item_loader.add_value('furnished', False)
        if 'Ascensore:' in stripped_details:
            index = stripped_details.index('Ascensore:')
            elev = stripped_details[index+1]
            if elev not in['Riscaladamento:','']:
                if elev != 'No':
                    item_loader.add_value('elevator', True)
                else:
                    item_loader.add_value('elevator', False)
        if 'Piano:' in stripped_details:
            floor_index = stripped_details.index('Piano:')
            floor = stripped_details[floor_index+1]
            if floor!='' and floor!='Ascensore:':
                item_loader.add_value('floor', floor)
        if 'Nr locali:' in stripped_details:
            index = stripped_details.index('Nr locali:')
            rooms = stripped_details[index+1]
            if rooms not in ['Nr vani:', '']:
                item_loader.add_value('room_count',int(rooms))
            #long_lat cant scraped
        yield item_loader.load_item()
