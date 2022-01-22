# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'evernest_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    start_urls = ["https://www.evernest.co.uk/search-properties-for-rent"] #LEVEL-1

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='summary-thumbnail-outer-container']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
       
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        features = " ".join(response.xpath("//ul[@data-rte-list]/li//text()").getall())
        if get_p_type_string(features):
            item_loader.add_value("property_type", get_p_type_string(features))
        else:
            features = " ".join(response.xpath("//h3[contains(.,'Description')]/..//text()").getall())
            if get_p_type_string(features):
                item_loader.add_value("property_type", get_p_type_string(features))
            else:
                return

        item_loader.add_value("external_source", "Evernestproperties_Co_PySpider_united_kingdom")

        address = response.xpath("//h1/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("zipcode", address.split(',')[-1].strip().split(' ')[-1].strip())
            item_loader.add_value("city", address.split(',')[-1].strip().split(' ')[0].strip())
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        description = " ".join(response.xpath("//div[contains(@class,'map-block')]/following-sibling::div[contains(@class,'html-block')]//p//text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))
        else:
            description = " ".join(response.xpath("//h3[contains(.,'Full Description')]//following-sibling::p//text()").getall()).strip()
            if description:
                item_loader.add_value("description", description.replace('\xa0', ''))

        room_count = response.xpath("//strong[contains(.,'BED')]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split('BED')[0].strip())

        rent = response.xpath("//h3/text()").get()
        if rent:
            rent = rent.split('£')[-1].lower().split('p')[0].strip().replace(',', '').replace('\xa0', '')
            item_loader.add_value("rent", str(int(float(rent))))
            item_loader.add_value("currency", 'GBP')
        
        images = [response.urljoin(x) for x in response.xpath("//div[@class='sqs-gallery']/div/img/@data-src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))

        latitude = response.xpath("//meta[@property='og:latitude']/@content").get()
        if latitude:
            item_loader.add_value("latitude", latitude.strip())

        longitude = response.xpath("//meta[@property='og:longitude']/@content").get()
        if longitude:
            item_loader.add_value("longitude", longitude.strip())
        
        parking = response.xpath("//li[contains(.,'Parking') or contains(.,'parking')]").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//li[contains(.,'Balcony') or contains(.,'balcony')]").get()
        if balcony:
            item_loader.add_value("balcony", True)

        furnished = response.xpath("//a[contains(.,'Part Furnished')]").get()
        furnished2 = response.xpath("//li[contains(.,'Fully Furnished') or contains(.,'Fully furnished')]").get()
        if furnished or furnished2:
            item_loader.add_value("furnished", True)

        item_loader.add_value("landlord_name", "Evernest")
        item_loader.add_value("landlord_phone", "+44 (20) 7587-3017")
        item_loader.add_value("landlord_email", "care@evernest.co.uk")
      
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartement" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "cottage" in p_type_string.lower() or "terrace" in p_type_string.lower()):
        return "house"
    else:
        return None