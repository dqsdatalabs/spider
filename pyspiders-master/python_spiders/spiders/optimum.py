# -*- coding: utf-8 -*-
# Author: Pankaj Kalania
# Team: Sabertooth

import scrapy
from ..loaders import ListingLoader
from ..helper import  extract_number_only
from scrapy.http import FormRequest
from ..user_agents import random_user_agent
import copy


class OptimumSpider(scrapy.Spider):
    name = 'optimum_be'
    allowed_domains = ['www.optimum.be']
    start_urls = ['http://www.optimum.be/']
    execution_type = 'testing'
    country = 'belgium'
    locale = 'fr'
    thousand_separator = '.'
    scale_separator = ','
    position = 0
    form_data = {"action": "omnicasaAjaxSearchSale",
                 "type_property": 0,
                 "type": "rent",
                 "cursor": 0}
    api_url = "https://www.optimum.be/wp-admin/admin-ajax.php"

    def start_requests(self):
        start_urls = [{
            'url': 'https://www.optimum.be/nos-biens/?sr-type=rent&sr-properties=1',
            'property_type': 'house',
            "type_property": 1,
            },
            {
            'url': 'https://www.optimum.be/nos-biens/?sr-type=rent&sr-properties=2',
            'property_type': 'apartment',
            "type_property": 2
            }
        ]

        for url in start_urls:
            self.form_data['type_property'] = str(url.get('type_property'))
            self.form_data['cursor'] = '0'
            yield FormRequest(url=self.api_url,
                              formdata=self.form_data,
                              callback=self.parse,
                              meta={'cursor': 0,
                                    'response_url': self.api_url,
                                    'property_type': url.get('property_type')
                                    }
                              )

    def parse(self, response, **kwargs):
        listings = response.xpath('.//a[contains(@href, "/property/")]/@href').extract()
        for url in listings:
            yield scrapy.Request(url=url,
                                 callback=self.get_property_details,
                                 meta={'response_url': url,
                                       'property_type': response.meta.get('property_type')})

        if len(listings) > 0:
            form_data1 = copy.deepcopy(self.form_data)
            form_data1['cursor'] = str(response.meta['cursor']+12)
            yield FormRequest(url=self.api_url,
                              formdata=form_data1,
                              callback=self.parse,
                              meta={'cursor': response.meta["cursor"]+12,
                                    'response_url': self.api_url,
                                    'property_type': response.meta.get('property_type')})

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.meta.get('response_url'))
        item_loader.add_value('external_id', response.meta.get('response_url').split('/')[-5])

        item_loader.add_xpath('title', './/div[contains(@class,"type")]//span/text()')
        item_loader.add_xpath('description', './/div[contains(@class,"description-section")]/p/text()')
        item_loader.add_xpath('rent_string', './/div[contains(@class,"price")]//span/text()')

        item_loader.add_xpath('room_count', './/div[contains(@class,"bedrooms")]/span[contains(@class,"value")]/text()')
        item_loader.add_xpath('bathroom_count', './/div[contains(@class,"bathrooms")]/span[contains(@class,"value")]/text()')
        item_loader.add_xpath('square_meters', '//div[contains(@class,"superficy")]/span[contains(@class,"value")]/text()')
        item_loader.add_value('property_type', response.meta.get('property_type'))
        item_loader.add_xpath('energy_label', './/div[contains(@class,"energy")]//span/text()')
        item_loader.add_xpath('address', './/div[contains(@class,"address")]//span/text()')
        if item_loader.get_output_value('address') and len(item_loader.get_output_value('address')) > 2:
            item_loader.add_value('city', item_loader.get_output_value('address').split(' ')[-1])
            item_loader.add_value('zipcode', item_loader.get_output_value('address').split(' ')[-2])

        item_loader.add_xpath('latitude', './/div[contains(@id,"lat")]/text()')
        item_loader.add_xpath('longitude', './/div[contains(@id,"lon")]/text()')
        item_loader.add_xpath('images', './/li[contains(@class,"image")]//img/@src')

        # https://www.optimum.be/property/8522/Appartement%20Loft/IXELLES/1050/
        furnished = response.xpath('.//div[contains(@class,"description-section")]/div//*[contains(text(),"meublée ")]').extract_first()
        if furnished:
            item_loader.add_value('furnished', True)

        # https://www.optimum.be/property/8523/Appartement%20Loft/MONTEGN%C3%89E/4420/
        elevator = response.xpath('.//div[contains(@class,"description-section")]/div//*[contains(text(),"ascenseur ")]').extract_first()
        if elevator:
            item_loader.add_value('elevator', True)

        # https://www.optimum.be/property/8523/Appartement%20Loft/MONTEGN%C3%89E/4420/
        parking = response.xpath('.//div[contains(@class,"description-section")]/div//*[contains(text(),"parking ")]').extract_first()
        if parking:
            item_loader.add_value('parking', True)

        # https://www.optimum.be/property/8522/Appartement%20Loft/IXELLES/1050/
        dishwasher = response.xpath('.//div[contains(@class,"description-section")]/div//*[contains(text(),"lave-vaisselle")]').extract_first()
        if dishwasher:
            item_loader.add_value('dishwasher', True)

        # https://www.optimum.be/property/8523/Appartement%20Loft/MONTEGN%C3%89E/4420/
        terrace = response.xpath('.//div[contains(@class,"description-section")]/div//*[contains(text(),"terrasse ")]').extract_first()
        if terrace:
            item_loader.add_value('terrace', True)

        # https://www.optimum.be/property/8362/Appartement%20Loft/LI%C3%88GE/4000/
        utilities = response.xpath('.//div[contains(@class,"description-section")]/div//*[contains(text(),"charges communes")]').extract_first()
        if utilities:
            item_loader.add_value('utilities', extract_number_only(utilities))

        # https://www.optimum.be/property/8362/Appartement%20Loft/LI%C3%88GE/4000/
        rental_guarantee = response.xpath('.//div[contains(@class,"description-section")]/div//*[contains(text(),"GARANTIE LOCATIVE")]').extract_first()
        if rental_guarantee:
            item_loader.add_value('prepaid_rent', int(extract_number_only(rental_guarantee))*int(extract_number_only(response.xpath('.//div[contains(@class,"price")]//span/text()').extract_first())))

        item_loader.add_value('landlord_name', 'Optimum')
        item_loader.add_value('landlord_phone', '04/221 48 75')
        item_loader.add_value('landlord_email', 'info@optimum.be')

        self.position += 1
        item_loader.add_value('position', self.position )
        item_loader.add_value("external_source", "Optimum_PySpider_{}_{}".format(self.country, self.locale))
        yield item_loader.load_item()
