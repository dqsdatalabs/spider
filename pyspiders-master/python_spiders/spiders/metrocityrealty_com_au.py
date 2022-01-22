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
    name = 'metrocityrealty_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    
    custom_settings = {
        "PROXY_ON" : "True",
        "PASSWORD" : "wmkpu9fkfzyo",
    }
    def start_requests(self):
        start_urls = [
            {   "url": [
                        "https://metrocityrealty.com.au/page/1/?action=epl_search&post_type=rental&property_status=current&property_category=Apartment",
                        "https://metrocityrealty.com.au/page/1/?action=epl_search&post_type=rental&property_status=current&property_category=Unit",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://metrocityrealty.com.au/page/1/?action=epl_search&post_type=rental&property_status=current&property_category=House",
                    "https://metrocityrealty.com.au/page/1/?action=epl_search&post_type=rental&property_status=current&property_category=Townhouse",
                ],
                "property_type": "house"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(item,
                                callback=self.parse,
                                meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        seen = False
        for item in response.xpath("//h3[@class='entry-title']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item,  meta={"property_type":response.meta.get('property_type')})
            seen = True
        
        if page == 1 or seen:
            url = response.url.replace(f"au/page/{page-1}/","au/").replace("au/", f"au/page/{page}/")
            yield Request(url, callback=self.parse, meta={"page": page+1, "property_type":response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Metrocityrealty_Com_PySpider_australia")

        external_id = response.xpath("//link[contains(@rel,'shortlink')]//@href").get()
        if external_id:
            external_id = external_id.split("p=")[1]
            item_loader.add_value("external_id", external_id)
        
        address = response.xpath("//h1[contains(@class,'trail-title')]/text()").get()
        if address:
            item_loader.add_value("title", address)
            item_loader.add_value("address", address)
            city = address.split(",")[-1].split("QLD")[0].strip()
            item_loader.add_value("city", city)
        
        zipcode = " ".join(response.xpath("//div[contains(@class,'title-meta-wrapper')]//span[contains(@class,'entry-title-sub')]//span[contains(@class,'item-state')]//text() | //div[contains(@class,'title-meta-wrapper')]//span[contains(@class,'entry-title-sub')]//span[contains(@class,'item-pcode')]//text()").getall())
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())
        
        rent = response.xpath("//span[@class='page-price']/text()").get()
        if rent:
            price = rent.split("$")[1].strip().split(" ")[0]
            item_loader.add_value("rent", int(price)*4)
        item_loader.add_value("currency", "AUD")
        
        room_count = response.xpath("//span[@title='Bedrooms']//text()").get()
        item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//span[@title='Bathrooms']//text()").get()
        item_loader.add_value("bathroom_count", bathroom_count)
        
        description = " ".join(response.xpath("//div[contains(@class,'tab-content')]//p//text()").getall())
        if description:
            description = re.sub('\s{2,}', ' ', description.strip())
            item_loader.add_value("description", description)
        
        parking = response.xpath("//span[contains(@title,'Parking Spaces')]//text()[.!='0']").get()
        if parking:
            item_loader.add_value("parking", True)
        
        images = [x for x in response.xpath("//div[contains(@class,'gallery-icon')]/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        
        balcony = response.xpath("//li[contains(.,'Balcon')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        dishwasher = response.xpath("//li[contains(.,'Dishwasher')]").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
        
        furnished = response.xpath("//li[contains(.,'Furnished')]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        import dateparser
        available_date = response.xpath("//div[contains(@class,'date-available')]/text()").get()
        if available_date:
            available_date = available_date.split("from")[1].strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        latitude_longitude = response.xpath("//script[contains(.,'lat\"')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('lat":"')[1].split('"')[0]
            longitude = latitude_longitude.split('long":"')[1].split('"')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
                
        item_loader.add_value("landlord_name", "Metrocity Realty")
        item_loader.add_value("landlord_phone", "07 3844 8399")
        item_loader.add_value("landlord_email", "info@metrocityrealty.com.au")
        
        yield item_loader.load_item()