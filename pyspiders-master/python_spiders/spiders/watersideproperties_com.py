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

class MySpider(Spider):
    name = 'watersideproperties_com'    
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.watersideproperties.com/properties-to-rent/?department=Lettings&location=&min-price=&max-price=&type=2&bedrooms=",
                ],
                "property_type" : "apartment"
            },
            {
                "url" : [
                    "https://www.watersideproperties.com/properties-to-rent/?department=Lettings&location=&min-price=&max-price=&type=1&bedrooms=",
                    "https://www.watersideproperties.com/properties-to-rent/?department=Lettings&location=&min-price=&max-price=&type=3&bedrooms="
                ],
                "property_type" : "house"
            },   
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//a[.//p[contains(@class,'card__label') and contains(.,'To Let')]]"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            room_count = item.xpath(".//p[contains(@class,'desc')]/text()").get()
            if room_count:
                room_count = room_count.lower().split('bedroom')[0].strip()
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type'), 'room_count': room_count})
        
        next_page = response.xpath("//a[.='→']/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse, meta={'property_type': response.meta.get('property_type')})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Watersideproperties_PySpider_"+ self.country + "_" + self.locale)

        external_id = response.url.split('/')[-2].split('-')[0].strip()
        if external_id:
            item_loader.add_value("external_id", external_id)
        
        address = response.xpath("//h1/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("title", address.strip())

        description = " ".join(response.xpath("//div[@class='content-area']//text()").getall()).strip()
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))

            # if 'bathroom' in description.lower():
            #     bathroom_count = description.lower().split('bathroom')[0].strip().split(' ')[-1].strip()
            #     if bathroom_count.isnumeric():
            #         item_loader.add_value("bathroom_count", bathroom_count.strip())
            #     else:
            #         item_loader.add_value("bathroom_count", '1')
        
        room_count = response.meta.get('room_count')
        if room_count.isnumeric():
            item_loader.add_value("room_count", room_count)
        
        rent = response.xpath("//div[contains(@class,'sticky-parent')]/p/text()").get()
        if rent:
            term = rent.strip().split(' ')[-1].lower()
            if term == 'pa':
                term = 12
            elif term == 'pcm':
                term = 1
            currency = ''
            if '€' in rent:
                rent = str(int(int(rent.split('€')[-1].strip().split(' ')[0].replace(',', '')) / term))
                currency = 'EUR'
            elif '£' in rent:
                rent = str(int(int(rent.split('£')[-1].strip().split(' ')[0].replace(',', '')) / term))
                currency = 'GBP'
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", currency)
        
        images = [x for x in response.xpath("//div[contains(@class,'images')]//div[contains(@class,'carousel__container')]/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))

        floor_plan_images = [x for x in response.xpath("//div[contains(@class,'floorplan')]/img/@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        latitude = response.xpath("//iframe[@class='google-map']/@src").get()
        if latitude:
            item_loader.add_value("latitude", latitude.split('?q=')[-1].split(',')[0].strip())
        
        longitude = response.xpath("//iframe[@class='google-map']/@src").get()
        if longitude:
            item_loader.add_value("longitude", longitude.split('?q=')[-1].split(',')[-1].split('&')[0].strip())
        
        landlord_name = response.xpath("//div[contains(@class,'image-circular')]//div[contains(@class,'content__top')]/p[contains(@class,'title')]/text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name.strip())
        
        landlord_phone  = response.xpath("//div[contains(@class,'image-circular')]//div[contains(@class,'content__top')]/p[contains(@class,'bottom-4')]/text()").get()
        if landlord_phone :
            item_loader.add_value("landlord_phone", landlord_phone .strip())
        
        item_loader.add_value("landlord_email", "london@watersideproperties.com")

        zipcode_check = response.xpath("//p[contains(@class,'card__text')]/text()").get()
        if zipcode_check:
            agence_zipcode = zipcode_check.split(" ")[-2]
            for i in response.xpath("//h1[contains(@class,'title')]/text()").get().split(","):
                if agence_zipcode in i:
                    item_loader.add_value("zipcode", i)
                    break
        
        if response.xpath("//strong[contains(.,'BALCONY')]").get(): item_loader.add_value("balcony", True)
        if response.xpath("//strong[contains(.,'PARKING')]").get(): item_loader.add_value("parking", True)

        yield item_loader.load_item()
