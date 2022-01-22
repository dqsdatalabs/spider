# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import copy
import json
import urllib
from datetime import date 
import scrapy
from ..helper import extract_number_only, remove_unicode_char, remove_white_spaces, extract_rent_currency
from ..loaders import ListingLoader


class NextPropertySpider(scrapy.Spider):
    name = 'nextproperty_co_uk'
    allowed_domains = ['nextproperty.co.uk']
    start_urls = ['https://www.nextproperty.co.uk/']
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0
    external_source="Nextproperty_PySpider_united_kingdom_en"
    api_url = 'https://www.nextproperty.co.uk/api/set/results/grid'
    params = {'sortorder': 'price-desc',
              'RPP': '12',
              'OrganisationId': '14ff91bc-a242-48b3-bd13-2b0d14bd252c',
              'Status': '{2a50fde6-8f09-4d01-9514-7a856e206d04},{e9617465-c405-4b6a-abc9-fdbfc499145c},{59c95297-2dca-4b55-9c10-220a8d1a5bed}',
              'WebdadiSubTypeName': 'Rentals',
              'includeSoldButton': 'true',
              'page': '1',
              'incsold': 'true'}

    def start_requests(self):
        start_urls = [self.api_url + "?" + urllib.parse.urlencode(self.params)]
        for url in start_urls:
            yield scrapy.FormRequest(url=self.api_url,
                                     callback=self.parse,
                                     method='POST',
                                     formdata=self.params,
                                     meta={'request_url': url,
                                           'params': self.params,
                                           'page': 1})

    def parse(self, response, **kwargs):
        for property_url in response.xpath('.//a[@class="property-description-link"]/@href').extract():
            property_url = response.urljoin(property_url)
            yield scrapy.Request(
                url=property_url,
                callback=self.get_property_details,
                meta={'request_url': property_url}
            )

        if len(response.xpath('.//a[@class="property-description-link"]')) > 0:
            current_page = response.meta["params"]["page"]
            params = copy.deepcopy(response.meta["params"])
            params["page"] = str(int(current_page) + 1)
            next_page_url = 'https://www.nextproperty.co.uk/let/property-to-let?page='+str(params["page"])
            yield scrapy.FormRequest(
                url=self.api_url,
                callback=self.parse,
                formdata=params,
                meta={'request_url': next_page_url,
                      'params': params}
            )
            
    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.meta["request_url"])
        title = response.xpath('.//h2[@class="color-primary mobile-left"]/text()').extract_first()
        item_loader.add_value('title', remove_white_spaces(title))
        item_loader.add_xpath('description', './/*[@id="description"]//p/text()')
        apartment_types = ["appartement", "apartment", "flat",
                           "penthouse", "duplex", "triplex"]
        house_types = ['chalet', 'bungalow', 'maison', 'house', 'home', 'villa']
        studio_types = ["studio"]
        if any(i in title.lower() for i in studio_types):
            item_loader.add_value('property_type', 'studio')
        elif any(i in title.lower() for i in apartment_types):
            item_loader.add_value('property_type', 'apartment')
        elif any(i in title.lower() for i in house_types):
            item_loader.add_value('property_type', 'house')
        else:
            return

        item_loader.add_xpath('images', './/*[@id="gallery"]//@data-bg')
        floor_plan = response.xpath('.//*[contains(@class,"image-wrapper-floorplan")]//img/@data-src').extract()
        if floor_plan:
            item_loader.add_value('floor_plan_images', floor_plan)

        item_loader.add_xpath('bathroom_count', './/img[contains(@src,"bathrooms")]/preceding-sibling::span/text()')
        bedroom_count = response.xpath('.//img[contains(@src,"bedrooms")]/preceding-sibling::span/text()').extract_first()
        if bedroom_count == '0':
            item_loader.add_value('room_count', '1')
        else:
            item_loader.add_value('room_count', bedroom_count)

        rent_string = response.xpath('.//h2//*[@class="nativecurrencyvalue"]/text()').extract_first()
        rent_symbol = response.xpath('.//h2//*[@class="nativecurrencysymbol"]/text()').extract_first()
        rent_int = response.xpath('.//h4//*[@class="nativecurrencyvalue"]/following-sibling::text()').extract_first()
        if rent_int and rent_string and "per week" in rent_int:
            item_loader.add_value('rent_string', rent_symbol+str(int(float(rent_string.replace(',', ''))*4)))
        elif not rent_int:
            # https://www.nextproperty.co.uk/property/nepr0_17709/e14/london/botanic-square/flat/1-bedroom
            rent_int = response.xpath('.//*[contains(@class,"property-price")]/h2/text()').extract_first()
            if rent_int and "per week" in rent_int.lower():
                item_loader.add_value('rent', rent_int.split("£")[1].split("per")[0])
            elif rent_int: 
                item_loader.add_value('rent_string', rent_int)
        else:
            item_loader.add_value('rent_string', rent_symbol + rent_string)
            
        postalcode = response.xpath('.//*[@class="displayPostCode"]/text()').extract_first()
        if postalcode:
            item_loader.add_value('zipcode', remove_white_spaces(postalcode))
            
        city = response.xpath('.//*[@class="city"]/text()').extract_first()
        if city:
            item_loader.add_value('city', remove_white_spaces(city.replace(',', '')))
        
        house_num = response.xpath('.//*[@class="houseNo"]/text()').extract_first()
        add1 = response.xpath('.//*[@class="address1"]/text()').extract_first()
        add2 = response.xpath('.//*[@class="address2"]/text()').extract_first()
        
        address = response.xpath('.//div[@class="title-wrapper"]/h2/text()').extract_first()
        if address:
            item_loader.add_value('address', remove_white_spaces(address))
        else:
            address = house_num+add1+add2+city+postalcode.strip()
            item_loader.add_value('address', address)

        # https://www.nextproperty.co.uk/property/nepr0_20402/w1k/london/grosvenor-hill/flat/2-bedrooms
        if response.xpath('.//li[contains(text(),"Parking") or contains(text(),"parking") ]').extract() or 'parking' in item_loader.get_output_value('description').lower():
            item_loader.add_value('parking', True)

        # https://www.nextproperty.co.uk/property/nepr0_20367/w1j/london/hill-street/flat/1-bedroom
        if response.xpath('.//li[contains(text()," Furnished ") or contains(text()," furnished ") ]').extract() or ' furnished ' in item_loader.get_output_value('description').lower():
            item_loader.add_value('furnished', True)

        # https://www.nextproperty.co.uk/property/nepr0_20367/w1j/london/hill-street/flat/1-bedroom
        if response.xpath('.//li[contains(text(),"Lift") or contains(text(),"lift") ]').extract() or 'lift' in item_loader.get_output_value('description').lower():
            item_loader.add_value('elevator', True)

        # https://www.nextproperty.co.uk/property/nepr0_20416/w1j/london/hill-street/flat/1-bedroom
        if response.xpath('.//li[contains(text(),"Available now") or contains(text(),"Available Now") ]').extract():
            item_loader.add_value('available_date', date.today().strftime("%Y-%m-%d"))
            
        # if 'Bedroom' in title:
        #     room_count = re.search(r'\d+\s(?=Bedroom)',title)
        #     if room_count and room_count.group().strip().isnumeric():
        #         item_loader.add_value('room_count',room_count.group().strip())
        # #https://www.nextproperty.co.uk/property/nepr0_20691/w1t/london/cleveland-street/studio/studio
        # #room_count
        # elif any (i in title.lower() for i in studio_types):
        #     item_loader.add_value('room_count','1')
                
        javascript = response.xpath('.//*[@id="maps"]/@data-cords').extract_first()
        if javascript:
            lat_lng = json.loads(javascript)
            item_loader.add_value('latitude', lat_lng['lat'])
            item_loader.add_value('longitude', lat_lng['lng'])
             
        item_loader.add_value('landlord_phone', '0207 118 0000')
        item_loader.add_value('landlord_email', 'info@nextproperty.co.uk')
        item_loader.add_value('landlord_name', 'NEXT PROPERTY')

        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source",self.external_source)
        yield item_loader.load_item()
