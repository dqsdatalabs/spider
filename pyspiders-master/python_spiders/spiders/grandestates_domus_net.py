# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import re
import scrapy
from ..helper import extract_rent_currency, format_date
import js2xml
import lxml.etree
from scrapy import Selector
from ..loaders import ListingLoader
from word2number import w2n


class GrandestatesDomusNetSpider(scrapy.Spider):
    name = 'grandestates_domus_net'
    allowed_domains = ['grandestates.domus.net']
    start_urls = ['https://grandestates.domus.net/']
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0

    def start_requests(self):

        start_url = ["https://grandestates.domus.net/site/go/search?sales=false&items=12&includeUnavailable=false"]
        for url in start_url:
            yield scrapy.Request(url=url,
                                 callback=self.parse)

    def parse(self, response, **kwargs):
        listings = response.xpath("//div[@class='searchResult']")
        for property_item in listings:
            external_id = property_item.xpath(".//div[@class='searchResultPhoto']/a/@href").extract_first().split('=')[1]
            url = response.urljoin(property_item.xpath(".//div[@class='searchResultPhoto']/a/@href").extract_first())
            
            yield scrapy.Request(
                url=url,
                callback=self.get_property_details,
                meta={'request_url': url,
                      'external_id': external_id})

        next_page = response.xpath("//div[@id='pageList']/a[contains(text(),'next')]/@href").extract_first()  
        if next_page:
            next_page_url = response.urljoin(next_page)
            yield response.follow(
                url=next_page_url,
                callback=self.parse)

    def get_property_details(self, response):

        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.meta.get('request_url'))
        item_loader.add_value('external_id', response.meta.get('external_id'))
        title = response.xpath(".//title").extract_first().split(" | ")[0]
        item_loader.add_value("title", title)
        images = response.xpath(".//div[@class='sp-slide']/img/@src").extract()
        images = [response.urljoin(image) for image in images]
        item_loader.add_value('images', images)
        rent = response.xpath(".//span[@id='price']/text()").extract_first()
        if rent:
            price = rent.split("£")[1].strip().split(" ")[0].replace(",","")
            item_loader.add_value('rent_string', price+"£")
            
        bedrooms = response.xpath(".//span[@class='bedroomsType']/text()").extract_first().split()[0]
        item_loader.add_xpath('room_count', bedrooms)
        item_loader.add_xpath('address', "//span[@class='propertyAddress']/text()")
        item_loader.add_value('zipcode', item_loader.get_output_value('address').split(", ")[-1])
        item_loader.add_value('city', item_loader.get_output_value('address').split(", ")[-2])

        description = " ".join(response.xpath(".//div[@id='description']/p/text()").extract()[:-1])
        item_loader.add_value('description', description)

        javascript = response.xpath('.//*[contains(text(),"map")]/text()').extract_first()
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            selector = Selector(text=xml)
            latitude = selector.xpath('.//array/number[1]/@value').get()
            longitude = selector.xpath('.//array/number[2]/@value').get()
            item_loader.add_value('latitude', latitude)
            item_loader.add_value('longitude', longitude)
        
        item_loader.add_value('landlord_name', 'Grand Estates')
        item_loader.add_value('landlord_phone', '02072285922')
        item_loader.add_value('landlord_email', 'info@grandestateslondon.com')
        
        features = " ".join(response.xpath("(//h2[contains(text(),'Key Features')]/../ul)[1]/li/span/text()").extract())
        #bathroom check
        num_dict = {'One':1,'Two':2,'Three':3,'Four':4,'Five':5}
        if 'available' in features.lower():
            available_date = re.findall(r'\d{2}[\/\-]\d{2}[\/\-]\d{4}', features.lower())
            if available_date:
                item_loader.add_value('available_date', format_date(available_date[0]))
        bathroom_count = "".join(response.xpath("//li[contains(.,'bathroom')]//text()").getall())
        if bathroom_count:
            bathroom_count = bathroom_count.strip().split(" ")[0]
            if bathroom_count.isdigit():
                item_loader.add_value("bathroom_count", bathroom_count)
            else:
                try:
                    item_loader.add_value("bathroom_count", w2n.word_to_num(bathroom_count))
                except: pass
            
        apartment_types = ["appartement", "apartment", "flat",
                           "penthouse", "duplex", "triplex", 'maisonette']
        house_types = ['chalet', 'bungalow', 'maison', 'house', 'home', ' villa ', 'holiday complex', 'cottage', 'semi-detached', ' detached ', ' terraced ', 'end terraced' ]
        studio_types = ["studio"]
        room_types = [" room "]
        if any(i in title.lower() for i in room_types):
            item_loader.add_value('property_type', 'room')
        elif any(i in title.lower() for i in studio_types):
            item_loader.add_value('property_type', 'studio')
        elif any(i in title.lower() for i in apartment_types):
            item_loader.add_value('property_type', 'apartment')
        elif any(i in title.lower() for i in house_types):
            item_loader.add_value('property_type', 'house')

        if "parking" in features.lower() or 'garage' in features.lower():
            item_loader.add_value('parking', True)

        if "terrace" in features.lower():
            item_loader.add_value('terrace', True)

        # https://grandestates.domus.net/site/go/viewParticulars?propertyID=413483
        if "balcony" in features.lower():
            item_loader.add_value('balcony', True)

        if "washing machine" in features.lower():
            item_loader.add_value('washing_machine', True)

        if "swimming" in features.lower() or "pool" in features.lower():
            item_loader.add_value('swimming_pool', True)

        # https://grandestates.domus.net/site/go/viewParticulars?propertyID=558886
        if " furnished" in features.lower() and "unfurnished" not in features.lower():
            item_loader.add_value('furnished', True)
        elif " furnished" not in features.lower() and "unfurnished" in features.lower():
            item_loader.add_value('furnished', False)
        
        # https://grandestates.domus.net/site/go/viewParticulars?propertyID=416936
        if "lift" in features.lower() and 'no lift' not in features.lower():
            item_loader.add_value('elevator', True)

        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", "GrandestatesDomus_PySpider_{}_{}".format(self.country, self.locale))
        
        return item_loader.load_item()
