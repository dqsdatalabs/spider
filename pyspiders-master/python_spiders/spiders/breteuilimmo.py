# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
import re
from ..loaders import ListingLoader

class BreteuilimmoSpider(scrapy.Spider):
    name = "breteuilimmo"
    allowed_domains = ["breteuilimmo.com"]
    start_urls = (
        'http://www.breteuilimmo.com/',
    )
    execution_type = 'testing'
    country = 'united_kingdom' 
    locale ='en'
    thousand_separator=','
    scale_separator='.'
    external_source="Breteuilimmo_PySpider_united_kingdom_en"

    def start_requests(self):
        start_urls = [
            {'url': 'https://breteuilimmo.com/en/location/londres/search?id_region=2&budget=2500&region=&action=&nbrChbr=&page=0'},
            {'url': 'https://breteuilimmo.com/en/location/paris/search?id_region=1&budget=5000&region=&action=&nbrChbr=&page=0'},
            {'url': 'https://breteuilimmo.com/en/location/lisbonne/search?id_region=40&budget=4000&region=&action=&nbrChbr=&page=0'},
        ]
        for url in start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse, 
                dont_filter=True
            )

    def parse(self, response, **kwargs):
        for i in range(0, 3):
            link = response.url.replace('page=0', 'page={}'.format(i))
            
            yield scrapy.Request(url=link, callback=self.get_detail_urls, dont_filter=True)
   
    def get_detail_urls(self, response):
        links = response.xpath('//div[contains(@class, "result-gallery")]/a')
        for link in links: 
            url = response.urljoin(link.xpath('./@href').extract_first())
            yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True)
    
    def get_property_details(self, response):
        description = ''.join(response.xpath('//div[@class="detail-item"]/p//text()').extract())
        if 'house' in description.lower():
            property_type = 'house'
        elif 'apartment' in description.lower():
            property_type = 'apartment'
        elif 'flat' in description.lower(): 
            property_type = 'apartment' 
        else:
            property_type = ''
        if property_type: 
            external_link = response.url
            address = response.xpath('//h1[contains(@class,"title-biens")]/text()').get()
            room_count_text = response.xpath('//span[contains(text(), "Bedroom")]/text()').extract_first('').strip() 
            if room_count_text:
                try:
                    room_count = re.findall(r'\d+', room_count_text)[0]
                except:
                    room_count = ''
            else:
                room_count = ''
            bathrooms_text = response.xpath('//span[contains(text(), "Bathroom")]/text()').extract_first('').strip() 
            if bathrooms_text:
                try:
                    bathrooms = re.findall(r'\d+', bathrooms_text)[0] 
                except:
                    bathrooms = ''
            else:
                bathrooms = ''
            price = ''.join(response.xpath('//ul[@class="product-coord"]/li[1]//text()').extract()).strip()
            if price:
                if "week" in price.lower():
                    price = price.split("/")[0].replace("£","").replace(",","").strip()
                    price = int(price)*4
                else:
                    price = price.split("/")[0].replace("€","").strip().replace(" ","")
            square_meters = response.xpath('//ul[@class="product-coord"]/li[4]/span/text()').extract_first('').strip()
            
            item_loader = ListingLoader(response=response)
            item_loader.add_value('property_type', property_type)
            item_loader.add_value('external_link', external_link)
            if address:
                item_loader.add_value('address', address)
                item_loader.add_value('title', address)
                if "," in address:
                    city = address.split(",")[-1].strip()
                    if city.replace(" ","").isalpha():
                        item_loader.add_value("city", city)
                    else:
                        city = address.split(",")[-2].strip()
                        if city.replace(" ","").isalpha():
                            item_loader.add_value("city", city)
                    
                    zipcode = address.split(",")[-1].strip()
                    if not zipcode.replace(" ","").isalpha():
                        item_loader.add_value("zipcode", zipcode)
                else:
                    item_loader.add_value("city", address)
 
            item_loader.add_value('description', description)
            item_loader.add_value('rent', price)
            item_loader.add_value("currency", "GBP")
            if square_meters:
                square_meters = square_meters.strip().split(" ")[0]
                item_loader.add_value("square_meters", square_meters)
            images=[x.split("?")[0].split(" ")[0] for x in response.xpath("//ul[@id='product-slider']//li//img/@src").getall()]
            if images:
                # img=[]
                # for i in images:
                #     i=i.split("?")[0]
                #     img.append(i)
                item_loader.add_value('images',images)
            if room_count and room_count >"0": 
                item_loader.add_value('room_count', str(room_count))
            if bathrooms:
                item_loader.add_value('bathroom_count', str(bathrooms))
                
            utilities = response.xpath("//span[contains(.,'Service charge')]/text()").get()
            if utilities:
                utilities = utilities.split(":")[1].split(".")[0].strip()
                item_loader.add_value("utilities", utilities)
            
            deposit = response.xpath("//span[contains(.,'deposit')]/text()").get()
            if deposit:
                deposit = deposit.split(":")[1].split("€")[0].strip() 
                item_loader.add_value("deposit", deposit)
            
            item_loader.add_value('landlord_name', 'Breteuil')
            item_loader.add_value('landlord_email', "location@breteuilimmo.com")
            item_loader.add_value('landlord_phone', "+33 1 42 24 71 71")
            item_loader.add_value("external_source", self.external_source)
            yield item_loader.load_item()