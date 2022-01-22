# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy, copy, urllib
from ..loaders import ListingLoader
from ..helper import remove_white_spaces, extract_rent_currency, convert_string_to_numeric
import re
import math


class BlackstoneresidentialSpider(scrapy.Spider):
    name = "blackstoneresidential_com"
    allowed_domains = ["blackstonesresidential.com"]
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    external_source='Blackstonesresidential_PySpider_united_kingdom_en'
    thousand_separator = ','
    scale_separator = '.'
    api_url = 'https://blackstonesresidential.com/properties/'
    params = {'pageno': 1,
              'propType': '',
              'propind': 'L'}
    # listing_new = []
    position = 0

    def start_requests(self):
        start_urls = [
                        {'propType': 32,
                         "property_type": "apartment"},
                        {'propType': 17,
                         "property_type": "apartment"},
                        {'propType': 21,
                         "property_type": "house"},
                        {'propType': 29,
                         "property_type": "house"},
                        {'propType': 13,
                         "property_type": "house"},
                        {'propType': 47,
                         "property_type": "room"},
                        {'propType': 20,
                         "property_type": "house"},
                        {'propType': 12,
                         "property_type": "house"},
                        {'propType': 46,
                         "property_type": "room"},
                        {'propType': 18,
                         "property_type": "studio"},
                        {'propType': 10,
                         "property_type": "house"}
                    ]
        for url in start_urls:
            params1 = copy.deepcopy(self.params)
            params1["propType"] = url["propType"]
            yield scrapy.Request(url=self.api_url + "?" + urllib.parse.urlencode(params1),
                                 callback=self.parse,
                                 meta={'request_url': self.api_url + "?" + urllib.parse.urlencode(params1),
                                       'params': params1,
                                       'property_type': url.get("property_type")})
                
    def parse(self, response, **kwargs):
        listing = response.xpath('.//*[@class="detlink"]/@href').extract()
        for property_url in listing:
            bathroom_count = response.xpath('.//*[@class="bathrooms"]/text()').extract_first()
            room_count = response.xpath('.//*[@class="beds"]/text()').extract_first()
            yield scrapy.Request(
                url=response.urljoin(property_url),
                callback=self.get_property_details,
                meta={'request_url': response.urljoin(property_url),
                      "property_type": response.meta["property_type"],
                      'room_count': room_count,
                      'bathroom_count': bathroom_count})

        if len(response.xpath('.//*[@class="detlink"]')) > 0:
            current_page = response.meta["params"]["pageno"]
            params1 = copy.deepcopy(response.meta["params"])
            params1["pageno"] = current_page + 1
            next_page_url = self.api_url + "?" + urllib.parse.urlencode(params1)
            yield scrapy.Request(
                url=self.api_url + "?" + urllib.parse.urlencode(params1),
                callback=self.parse,
                meta={'request_url': next_page_url,
                      'params': params1,
                      "property_type": response.meta["property_type"]})
                   
    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        let_agreed = response.xpath("//div[@class='status']//img//@alt").get()
        if let_agreed:           
            return
        item_loader.add_value("external_source", self.external_source)
        external_link = response.meta.get('request_url')
        item_loader.add_value('external_link', external_link)

        property_type = response.meta.get('property_type')
        item_loader.add_value('property_type', response.meta.get('property_type'))

        ids = response.xpath('.//*[@class="reference"]/text()').extract_first()
        if ids:
            external_id = remove_white_spaces(ids.split(':')[-1])
            item_loader.add_value('external_id', external_id)

        titles = response.xpath('.//*[@class="bedswithtype"]//text()').extract()
        if titles:
            item_loader.add_value('title', ''.join(titles))

        item_loader.add_xpath('description', './/*[@class="description"]//text()')

        address = response.xpath('.//*[@class="address"]/text()').extract_first()
        if address:
            item_loader.add_value('address', address)
            zipcode = item_loader.get_output_value('address').split(', ')[-1].split(' ')[-1]
            if not zipcode.isalpha():
                item_loader.add_value('zipcode', zipcode)

        city = response.xpath("substring-after(substring-after(//div[@class='address']/text(),','),', ')").extract_first()
        address_city =  city.replace(item_loader.get_output_value('zipcode'),"").strip()
        if address_city:
            if address_city !="":
                item_loader.add_value('city',address_city)
        else:
            city2 = response.xpath("substring-after(//div[@class='address']/text(),',')").extract_first()
            if city2:
                address_city2 = city2.strip().replace(item_loader.get_output_value('zipcode'),"").strip().replace(",","")
                if address_city2 !="":
                    item_loader.add_value('city',address_city2)
                # else:
                #     if city.isalpha():
                #         item_loader.add_value('city',city2)
            else:
                city2 = response.xpath("substring-after(substring-after(//div[@class='address']/text(),' '),' ')").extract_first()
                address_city2 =  city2.strip().replace(item_loader.get_output_value('zipcode'),"").strip().split(' ')[2]
                if address_city2 !="":
                    item_loader.add_value('city',address_city2)
                # else:
                #     if city.isalpha():
                #         item_loader.add_value('city',city2)


        price_qualifier = response.xpath('.//*[@class="displaypricequalifier"]/text()').extract_first()
        if price_qualifier and 'pcm' in price_qualifier:
            rent_string = response.xpath('.//*[@class="displayprice"]/text()').extract_first()
            rent = rent_string.replace(",","").replace("£","")
            item_loader.add_value("rent",int(float(rent)))
        elif price_qualifier and 'pw' in price_qualifier or 'pppw' in price_qualifier:
            rent_string = response.xpath('.//*[@class="displayprice"]/text()').extract_first()
            rent = "".join(filter(str.isnumeric, rent_string.replace(',', '').replace('\xa0', '')))
            item_loader.add_value("rent", str(int(float(rent)*4)))
        item_loader.add_value('currency', 'GBP')
        room_count = response.meta.get('room_count')
        if room_count and room_count != '0':
            item_loader.add_value('room_count', room_count)
        elif room_count == '0' or not room_count and property_type == 'studio':
            item_loader.add_value('room_count', '1')

        bathroom_count = response.meta.get('bathroom_count')
        if bathroom_count and bathroom_count != '0':
            item_loader.add_value('bathroom_count', bathroom_count)

        item_loader.add_xpath('images', './/*[@id="photocontainer"]//*[@class="propertyimage"]/@src')
        item_loader.add_xpath('floor_plan_images', './/*[@id="hiddenfloorplan"]//*[@class="propertyimage"]/@src')

        features = ', '.join(response.xpath('.//*[@class="features"]//li/text()').extract()).lower()
         
        area = re.search(r'\d+.{0,1}\d+(?=.sq ft)', features)
        area_sqm = re.search(r'\d+.{0,1}\d+(?=.sq m)', features)
        if area:
            areas = math.ceil(float(area.group())*0.092903)
            item_loader.add_value('square_meters', str(areas))
        elif area_sqm:
            item_loader.add_value('square_meters', str(math.ceil(float(area_sqm.group()))))

        floor = re.search(r'\d{1,2}(?=\w{2}\sfloor)', features)
        if floor and floor.group().isdigit():
            item_loader.add_value('floor', floor.group())

        if 'unfurnish' in features and 'unfurnished/furnished' not in features:
            item_loader.add_value('furnished', False)
        # https://blackstonesresidential.com/property-details/?id=2426&propind=L&proptype=32&orderdirection=DESC&pageno=11&pagesize=10&searchbymap=false
        elif 'furnish' in features and 'unfurnished/furnished' not in features:
            item_loader.add_value('furnished', True)
        if 'terrace' in features or 'terrace' in ''.join(titles):
            item_loader.add_value('terrace', True)
        # https://blackstonesresidential.com/property-details/?id=2426&propind=L&proptype=32&orderdirection=DESC&pageno=11&pagesize=10&searchbymap=false
        if 'balcony' in features:
            item_loader.add_value('balcony', True)
        # https://blackstonesresidential.com/property-details/?id=2426&propind=L&proptype=32&orderdirection=DESC&pageno=11&pagesize=10&searchbymap=false
        if 'swimming pool' in features or 'pool' in features:
            item_loader.add_value('swimming_pool', True)
        # https://blackstonesresidential.com/property-details/?id=1064&propind=L&proptype=32&orderdirection=DESC&pageno=11&pagesize=10&searchbymap=false
        if 'parking' in features:
            item_loader.add_value('parking', True)
        if 'dishwasher' in features:
            item_loader.add_value('dishwasher', True)
        # https://blackstonesresidential.com/property-details/?id=2304&propind=L&proptype=32&orderdirection=DESC&pageno=4&pagesize=10&searchbymap=false
        if 'lift' in features or 'elevator' in features:
            item_loader.add_value('elevator', True)

        lat_lng = response.xpath('.//*[@class="mapwrap"]//@data-map').extract_first()
        if lat_lng:

            item_loader.add_value('latitude', lat_lng.split(',')[0].strip('('))
            item_loader.add_value('longitude', lat_lng.split(',')[1])

        energy_level = response.xpath('.//*[@id="hiddenepc"]//*[@class="propertyimage"]/@title').extract_first()
        if energy_level:
            energy_label = convert_string_to_numeric(energy_level, BlackstoneresidentialSpider)
            if 92 <= energy_label <= 100:
                item_loader.add_value('energy_label', 'A')
            if 81 <= energy_label <= 91:
                item_loader.add_value('energy_label', 'B')
            if 69 <= energy_label <= 80:
                item_loader.add_value('energy_label', 'C')
            if 55 <= energy_label <= 68:
                item_loader.add_value('energy_label', 'D')
            if 39 <= energy_label <= 54:
                item_loader.add_value('energy_label', 'E')
            if 21 <= energy_label <= 38:
                item_loader.add_value('energy_label', 'F')
            if 1 <= energy_label <= 20:
                item_loader.add_value('energy_label', 'G')

        item_loader.add_value('landlord_name', 'Blackstones Residential')
        item_loader.add_value('landlord_phone', '0203 129 1870')
        item_loader.add_value('landlord_email', 'contact@blackstonesresidential.com')

        self.position += 1
        item_loader.add_value('position', self.position)

        yield item_loader.load_item()
