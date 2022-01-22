# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re
from word2number import w2n
import dateparser

class MySpider(Spider):
    name = 'mtdresidentiallettings_co_uk'    
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://mtdresidentiallettings.co.uk/property-search?price_min=&price_max=&bedrooms=&furnishing=&type=Apartment&sort=desc",
                    "http://mtdresidentiallettings.co.uk/property-search?price_min=&price_max=&bedrooms=&furnishing=&type=Flat&sort=desc",
                ],
                "property_type" : "apartment"
            },
            {
                "url" : [
                    "http://mtdresidentiallettings.co.uk/property-search?price_min=&price_max=&bedrooms=&furnishing=&type=Town+House&sort=desc",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "http://mtdresidentiallettings.co.uk/property-search?price_min=&price_max=&bedrooms=&furnishing=&type=Studio&sort=desc",
                ],
                "property_type" : "studio"
            },
            
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='property-search-info']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
        next_page = response.xpath("//a[contains(.,'next')]/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse, meta={'property_type': response.meta.get('property_type')})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Mtdresidentiallettings_PySpider_"+ self.country + "_" + self.locale)

        external_id = response.url.split('/')[-2].strip()
        if external_id:
            item_loader.add_value("external_id", external_id)
        
        title = response.xpath("//h1/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        address = response.xpath("//a[contains(.,'View on Map')]/@href").get()
        if address:
            address = address.split('&q=')[-1].strip().replace('%20', ' ')
            city = address.replace("One","").strip().split(" ")[-3]
            zipcode = " ".join(address.strip().split(" ")[-2:])
            title_words = len(title.split('|')[0].strip().split(' '))
            address = " ".join(address.split(' ')[title_words:])
            item_loader.add_value("address", address.strip())
            if city not in ["Centre","Way"]:
                item_loader.add_value("city", city.strip())
            item_loader.add_value("zipcode", zipcode.strip())

        description = " ".join(response.xpath("//div[@class='field-item odd']/p/text()").getall()).strip()
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))
        
            if 'sq ft' in description.lower():
                square_meters = "".join(filter(str.isnumeric, description.lower().split('sq ft')[0].strip().split(' ')[-1]))
                item_loader.add_value("square_meters", square_meters)
        
        room_count = response.xpath("//li[contains(text(),'Price')]/following-sibling::li[1]/text()").get()
        if room_count:
            if room_count.strip().lower() == 'studio':
                item_loader.add_value("room_count", '1')
            else:
                room_count = room_count.lower().split('bedroom')[0].strip()
                if room_count.isnumeric():
                    item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//li[contains(text(), 'BATHROOM')]/text()").get()
        if bathroom_count:
            try:
                bathroom_count = w2n.word_to_num(bathroom_count.lower().split('bathroom')[0].strip())
                item_loader.add_value("bathroom_count", str(bathroom_count))
            except:
                pass
        elif 'bathroom' in description.lower():
            bathroom_count = "".join(filter(str.isnumeric, description.lower().split('bathroom')[0].strip().split(' ')[-1]))
            try:
                bathroom_count = w2n.word_to_num(bathroom_count.strip())
                item_loader.add_value("bathroom_count", str(bathroom_count))
            except:
                pass
        
        rent = response.xpath("//li[contains(text(), 'Price:')]/text()").get()
        if rent:
            rent = rent.split('£')[-1].strip().split(' ')[0].replace(',', '')
            if rent.isnumeric():
                item_loader.add_value("rent", rent)
                item_loader.add_value("currency", 'GBP')
        
        available_date = "".join(response.xpath("//li[contains(text(), 'Date Available:')]//text()").getall())
        if available_date:
            available_date = available_date.split(':')[-1].strip()
            date_parsed = dateparser.parse(available_date, languages=['en'])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        deposit = response.xpath("//li[contains(text(), 'Deposit:')]/text()").get()
        if deposit:
            deposit = deposit.split('£')[-1].strip().replace(',', '')
            if deposit.isnumeric() and deposit != '0':
                item_loader.add_value("deposit", deposit)
        
        images = [x for x in response.xpath("//div[contains(@class, 'panel-images-wrapper')]//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))

        floor = response.xpath("//li[contains(text(), 'FLOOR')]/text()").get()
        if floor:
            try:
                floor = w2n.word_to_num(floor.lower().split('floor')[0].strip())
                item_loader.add_value("floor", str(floor))
            except:
                pass
        elif 'floor' in description.lower():
            floor = "".join(filter(str.isnumeric, description.lower().split('floor')[0].strip().split(' ')[-1]))
            try:
                floor = w2n.word_to_num(floor.strip())
                item_loader.add_value("floor", str(floor))
            except:
                pass
        
        pets_allowed = response.xpath("//li[contains(text(), 'NO PET')]/text()").get()
        if pets_allowed or 'no pets' in description.lower():
            item_loader.add_value("pets_allowed", False)
        
        parking = response.xpath("//li[contains(text(), 'NO PARKING')]/text()").get()
        if parking or 'no parking' in description.lower():
            item_loader.add_value("parking", False)
        else:
            parking = response.xpath("//li[contains(text(), 'PARKING')]/text()").get()
            if parking or 'parking' in description.lower():
                item_loader.add_value("parking", True)
        
        balcony = response.xpath("//li[contains(text(), 'BALCONY')]/text()").get()
        if balcony or 'balcony' in description.lower():
            item_loader.add_value("balcony", True)
        
        furnished = response.xpath("//li[contains(text(), 'Furnishing:')]/text()").get()
        if furnished:
            if furnished.split(':')[-1].strip().lower() == 'furnished':
                item_loader.add_value("furnished", True)
            elif furnished.split(':')[-1].strip().lower() == 'unfurnished':
                item_loader.add_value("furnished", False)
        
        if description:
            if 'lift' in description.lower():
                item_loader.add_value("elevator", True)
            if 'dishwasher' in description.lower():
                item_loader.add_value("dishwasher", True)
            if 'washing machine' in description.lower():
                item_loader.add_value("washing_machine", True)

        item_loader.add_value("landlord_name", 'MTD Residential Lettings Ltd')
        item_loader.add_value("landlord_phone", '0151 282 1539')

        yield item_loader.load_item()
