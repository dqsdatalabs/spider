# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import re
import scrapy
from ..loaders import ListingLoader
from datetime import datetime
from ..helper import extract_number_only, extract_rent_currency


class NextmovepropertiesComSpider(scrapy.Spider):
    name = 'nextmoveproperties_com'
    allowed_domains = ["nextmoveproperties.com"]
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0
    external_source = "Nextmoveproperties_PySpider_united_kingdom_en"

    def start_requests(self):
        start_urls = [
            # student_properties

            # House
            {'url': 'https://nextmoveproperties.com/properties/?tenant_type=Student&area=&price_s=&price_p=&bedrooms=&type=House',
             'property_type': 'house'},
            # shared_house
            {'url': 'https://nextmoveproperties.com/properties/?tenant_type=Student&area=&price_s=&price_p=&bedrooms=&type=Shared+house',
             'property_type': 'house'},
            # Flat
            {'url': 'https://nextmoveproperties.com/properties/?tenant_type=Student&area=&price_s=&price_p=&bedrooms=&type=Flat',
             'property_type': 'student_apartment'},

            # Professional_properties

            # House
            {'url': 'https://nextmoveproperties.com/properties/?tenant_type=Professional&area=&price_s=&price_p=&bedrooms=&type=House',
             'property_type': 'house'},
            # shared_house
            {'url': 'https://nextmoveproperties.com/properties/?tenant_type=Professional&area=&price_s=&price_p=&bedrooms=&type=Shared+house',
             'property_type': 'house'},
            # Flat
            {'url': 'https://nextmoveproperties.com/properties/?tenant_type=Professional&area=&price_s=&price_p=&bedrooms=&type=Flat',
             'property_type': 'apartment'}
        ]

        for url in start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse,
                meta={'property_type': url.get('property_type')})

    def parse(self, response, **kwargs):
       
        listings = response.xpath("//div[@class='block_item']")
        for property_item in listings:
            # Let agreed check
            let_check = len(property_item.xpath(".//span[@class='block_item_label']/text()"))
            if let_check == 0:
                property_url = property_item.xpath(".//a[@class='block_item_image_link']/@href").extract_first()
                
                yield scrapy.Request(
                    url=property_url,
                    callback=self.get_property_details,
                    meta={'request_url': property_url,
                          'property_type': response.meta.get('property_type')})
   
        next_page_url = response.xpath("//div[@class='pagination_next']/a/@href").extract_first()
        if next_page_url:
            yield scrapy.Request(
                url=next_page_url,
                callback=self.parse,
                meta={'request_url': next_page_url,
                      'property_type': response.meta.get('property_type')})

    def get_property_details(self, response):

        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.meta.get('request_url'))
        item_loader.add_xpath('external_id', ".//th[contains(text(),'Ref')]/following-sibling::td/text()")
        item_loader.add_xpath('title', "//h1[@class='property_heading_title']/text()")
        item_loader.add_xpath('room_count', ".//th[contains(text(),'Bedrooms')]/following-sibling::td/text()")
        item_loader.add_value('property_type', response.meta.get('property_type'))
        #All the properties in the website are in Newcastle upon Tyne
        item_loader.add_value('city', 'Newcastle Upon Tyne')
        item_loader.add_value('address', item_loader.get_output_value('title').split(' (')[0] + ", " + item_loader.get_output_value('city'))
        item_loader.add_xpath('energy_label', ".//th[contains(text(),'EPC Rating')]/following-sibling::td/text()")

        deposit = extract_number_only(response.xpath(".//th[contains(text(),'Deposit')]/following-sibling::td/text()").extract_first().split(".")[0])
        item_loader.add_value('deposit', deposit)

        rent_string = response.xpath(".//th[contains(text(),'Price')]/following-sibling::td/text()").extract_first()
        if rent_string:
            if 'pw' in rent_string:
                rent = rent_string.split("£")[-1].split("p")[0]
                item_loader.add_value('rent', str(int(float(rent))*4))
                item_loader.add_value('currency', "GBP")
            else:
                item_loader.add_value('rent_string', rent_string)
        # rent_type = rent_string.split()[-1]
        # rent, _ = extract_rent_currency(rent_string.split()[0], NextmovepropertiesComSpider)
        # currency = rent_string[0]
        # if rent_type == 'pppw':
        #     item_loader.add_value('rent_string', currency + str(rent*4))
        # elif rent_type == 'pcm':
        #     item_loader.add_value('rent_string', currency + str(rent))
            
        item_loader.add_xpath('description', ".//div[@class='column column_half']/p/text()")

        if "available" in item_loader.get_output_value('description').lower():

            check = re.findall(r'(\d{1,2})(?:\w{2})? ?(?:of)? (\w+) (\d{4})', item_loader.get_output_value('description').lower(), re.IGNORECASE)
            if check:
                available_date = list(check[0])
                available_date = " ".join(available_date)
                available_date = datetime.strptime(available_date, "%d %B %Y").strftime('%Y-%m-%d')
                item_loader.add_value('available_date', available_date)
        
        item_loader.add_xpath('images', './/a[@data-lightbox="gallery"]/@href')

        features = " ".join(response.xpath(".//div[@class='column column_half']/p/text()").extract())

        # https://nextmoveproperties.com/properties/osborne-terrace-jesmond-2/?tenant_type=Student&area&price_s&price_p&bedrooms&type=Flat
        if "parking" in features.lower() or 'garage' in features.lower():
            item_loader.add_value('parking', True)

        if "terrace" in features.lower():
            item_loader.add_value('terrace', True)

        # https://nextmoveproperties.com/properties/baltic-quay-gateshead-quayside-2/?tenant_type=Student&area&price_s&price_p&bedrooms&type=Flat
        if "balcony" in features.lower():
            item_loader.add_value('balcony', True)
        
        # https://nextmoveproperties.com/properties/hazelwood-avenue-jesmond/?tenant_type=Student&area&price_s&price_p&bedrooms&type=Flat
        if "washing machine" in features.lower():
            item_loader.add_value('washing_machine', True)
        
        # https://nextmoveproperties.com/properties/leazes-crescent-city-centre/?tenant_type=Student&area&price_s&price_p&bedrooms&type=Flat
        if "dish washer" in features.lower() or 'dishwasher' in features.lower():
            item_loader.add_value('dishwasher', True)
        
        if " furnished" in features.lower() and "unfurnished" not in features.lower():
            item_loader.add_value('furnished', True)
        elif " furnished" not in features.lower() and "unfurnished" in features.lower():
            item_loader.add_value('furnished', False)
        
        if "elevator" in features.lower() or "lift" in features.lower():
            item_loader.add_value('elevator', True)

        if "swimming pool" in features.lower():
            item_loader.add_value('swimming_pool', True)

        item_loader.add_value('landlord_name', "Next move properties")
        item_loader.add_value('landlord_email', "info@nextmoveproperties.com")
        item_loader.add_value('landlord_phone', "0191 281 9090")

        self.position += 1
        item_loader.add_value("position", self.position)
        item_loader.add_value("external_source", self.external_source)
        yield item_loader.load_item()
