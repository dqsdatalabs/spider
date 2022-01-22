# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
import re
from python_spiders.helper import extract_rent_currency, extract_number_only, remove_white_spaces
from ..loaders import ListingLoader
import math

class SothebysrealtySpider(scrapy.Spider):
    name = 'sothebysrealty_co_uk'
    allowed_domains = ['www.sothebysrealty.co.uk']
    start_urls = ['https://www.sothebysrealty.co.uk/']
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator=','
    scale_separator='.'
    position = 0

    def start_requests(self):
        start_urls = [
            {'url': 'https://www.sothebysrealty.co.uk/enb/rentals/1-page/house-type/london-alt',
                'property_type': 'house'},
            {'url': 'https://www.sothebysrealty.co.uk/enb/rentals/1-page/apartment-type/london-alt',
              'property_type': 'apartment'},
            {'url': 'https://www.sothebysrealty.co.uk/enb/rentals/int/1-page/house-type',
             'property_type': 'house'},
            {'url': 'https://www.sothebysrealty.co.uk/enb/rentals/int/1-page/apartment-type',
             'property_type': 'apartment'}
        ]
        for url in start_urls:
            yield scrapy.Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'request_url': url.get('url'),
                                       'property_type': url.get('property_type')})

    def parse(self, response, **kwargs):
        room_count_lst = []
        bathroom_count_lst = []
        listing = response.xpath('.//*[@class = "u-ar__content"]//a/@href').extract()
        property_details = response.xpath('.//*[@class="listing-item__features-container__wrapper"]')
        for i in property_details:
            room_count = i.xpath('.//*[@content="beds"]/preceding-sibling::*[@itemprop="value"]/text()').extract_first()
            bathroom_count = i.xpath('.//*[@content="baths"]/preceding-sibling::*[@itemprop="value"]/text()').extract_first()
            room_count_lst.append(room_count)
            bathroom_count_lst.append(bathroom_count)
        new_list = zip(room_count_lst,bathroom_count_lst,listing)
        for x,y,z in new_list:
            yield scrapy.Request(
                url='https://www.sothebysrealty.co.uk'+z,
                callback=self.get_property_details,
                meta={'request_url': 'https://www.sothebysrealty.co.uk'+z,
                      'property_type': response.meta["property_type"],
                      'bathroom_count':y,
                      'room_count':x}
            )


        if len(response.xpath('.//link[@itemprop="mainEntityOfPage"]/@href')) > 0:
            current_page = re.findall(r"\d(?=-page)", response.meta["request_url"])[0]
            next_page_url = re.sub(r"\d(?=-page)", str(int(current_page) + 1), response.meta["request_url"])
            yield scrapy.Request(
                url=response.urljoin(next_page_url),
                callback=self.parse,
                meta={'request_url': next_page_url,
                      'property_type': response.meta["property_type"]}
            )

    def get_property_details(self, response):
        external_link = response.meta.get('request_url')
        property_type = response.meta.get('property_type')
        room_count = response.meta.get('room_count')
        bathroom_count = response.meta.get('bathroom_count')
        title = response.xpath('.//meta[@property="og:title"]/@content').extract_first()
        square_meters_in_ft = response.xpath('.//dd[contains(text(),"Sq Ft.")]/text()').extract_first()
        images = response.xpath('.//*[contains(@class,"carousel__link")]//img/@src').extract()
        floor_plan_images = response.xpath('.//*[contains(@id,"floorplan")]//a/@href').extract()
        javascript = response.xpath('.//script[contains(text(),"Latitude")]/text()').extract_first()
        address = response.xpath('.//*[@class = "main-address"]/text()').extract_first()
        city = address.split(',')[-2].strip()
        zipcode = address.split(',')[-1].strip()

        item_loader = ListingLoader(response=response)
        
        item_loader.add_value('property_type', property_type)
        item_loader.add_value('external_link', external_link)
        item_loader.add_value('images',list(set(images)))
        item_loader.add_value('floor_plan_images',list(set(floor_plan_images)))
        item_loader.add_value('zipcode', zipcode)
        item_loader.add_value('city', city)
        item_loader.add_value('title', remove_white_spaces(title.split(' – ')[0].split(' - ')[0]))
        item_loader.add_xpath('address', './/*[@class = "main-address"]/text()')
        item_loader.add_xpath('description', './/*[@class = "p"]//text()')
        # item_loader.add_xpath('landlord_name', './/*[contains(@class,"broker-name")]//text()')
        # item_loader.add_xpath('landlord_phone', './/*[contains(@class,"phone-number")]//text()')
        
        if room_count != None or room_count != '0':
            item_loader.add_value('room_count',room_count)
        elif room_count == None or room_count == '0':
            rooms = response.xpath('.//dt[contains(text(),"Bedroom")]/../dd/text()').extract_first()
            if rooms!=None or rooms!='0':
                item_loader.add_value('room_count',rooms)
                
        if bathroom_count != None or bathroom_count != '0':
            item_loader.add_value('bathroom_count',bathroom_count)
            
        
        description = item_loader.get_output_value('description')
        floor = re.search(r'\d+(st|nd|rd|th)*\s(?=floor)',description.lower())
        if floor:
            item_loader.add_value('floor',extract_number_only(floor.group()))

        if re.search(r'un[^\w]*furnish',description.lower()):
            item_loader.add_value('furnished',False)
        elif re.search(r'not[^\w]*furnish',description.lower()):
            item_loader.add_value('furnished',False)
        elif re.search(r'furnish',description.lower()):
            item_loader.add_value('furnished',True)
            
        if javascript:
            latitude = re.search(r'(?<=Latitude:").{0,1}\d+.\d+',javascript).group()
            longitude = re.search(r'(?<=Longitude:").{0,1}\d+.\d+',javascript).group()
            if latitude and longitude:
                item_loader.add_value('latitude', latitude)
                item_loader.add_value('longitude',longitude)
        
        item_loader.add_value('external_images_count', len(set(images))+len(set(floor_plan_images)))
        item_loader.add_xpath('external_id', './/span[contains(text(), "Property ID")]/following-sibling::span/text()')

        # rent
        # rent_string = " ".join(response.xpath('.//div[contains(@class, "listing-info")]//div[contains(@class, "c-price")]//text()').extract())
        # if "week" in rent_string.lower():
        #     rent_string = rent_string.split(" ")[0].replace(",",".").replace("£","")
        #     rent = int(float(rent_string))*4
        #     item_loader.add_value('rent', rent)
        # if "month" in rent_string.lower():
        #     rent_string = rent_string.split("£")[1].strip().split(" ")[0].replace(",",".")
        #     item_loader.add_value('rent', int(float(rent_string)))

        rent = response.xpath("//div[contains(@id,'listinginfo')]//div[contains(@class,'price__value')]/text()").get()
        term = response.xpath("//div[contains(@id,'listinginfo')]//div[contains(@class,'period')]/text()").get()
        if rent and term:
            if "week" in term.lower():
                rent = int("".join(filter(str.isnumeric, rent)))
                item_loader.add_value("rent", str(rent * 4))
                item_loader.add_value("currency", "GBP")
            elif "month" in term.lower():
                item_loader.add_value("rent", "".join(filter(str.isnumeric, rent)))
                item_loader.add_value("currency", "GBP")

        item_loader.add_value('landlord_name', 'Sotheby’s International Realty')
        item_loader.add_value('landlord_phone', '+44 (0)1932 860537')
        item_loader.add_value('landlord_email', 'country@sothebysrealty.co.uk')
        
        if square_meters_in_ft:
            square_meter = square_meters_in_ft.split()[0]
            square_meters = int(square_meter.replace(',', ''))*0.09290304
            item_loader.add_value('square_meters', str(math.ceil(square_meters)))

        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", "Sothebysrealty_PySpider_{}_{}".format(self.country, self.locale))
        yield item_loader.load_item()
