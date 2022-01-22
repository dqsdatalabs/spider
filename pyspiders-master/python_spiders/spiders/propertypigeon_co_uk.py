# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags 
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re

class MySpider(Spider):
    name = 'propertypigeon_co_uk'
    execution_type='testing' 
    country='united_kingdom'
    locale='en'
    external_source="Propertypigeon_Co_PySpider_united_kingdom"

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.propertypigeon.co.uk/flats-for-rent",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.propertypigeon.co.uk/terrace-houses-for-rent",
                    "https://www.propertypigeon.co.uk/cottages-for-rent",
                    "https://www.propertypigeon.co.uk/semi-detached-houses-for-rent",
                    "https://www.propertypigeon.co.uk/detached-houses-for-rent"
                ],
                "property_type": "house"
            }
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//a[@title='More information']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
        
        next_page = response.xpath("//a[.='Next']/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse, meta={"property_type": response.meta.get('property_type')})
 
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        check_removed = response.xpath("//span/text()[contains(.,'Advert Removed')]").get()
        if check_removed:
            return
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        external_id = response.url
        if external_id:
            external_id = external_id.split("-")[-1]
            item_loader.add_value("external_id", external_id)
        if "search" in response.url:
            return
        dontallow=response.xpath("//div[@class='btn-danger badge f-15 fl']/text()").get()
        if dontallow and "let agreed" in dontallow.lower():
            return 


        title = response.xpath("//h1//text()").get()
        if title:
            item_loader.add_value("title", title)

        address = "".join(response.xpath("//div[contains(@class,'address')]//text()").getall())
        if address:
            if "-" in address:
                city = address.split("-")[-1]
                item_loader.add_value("city", city)
            address = re.sub('\s{2,}', ' ', address.strip())
            item_loader.add_value("address", address.strip())
        zipcode="".join(response.xpath("//div[contains(@class,'address')]//text()").getall())
        try:
            zipcode=re.search("[A-Z]+[A-Z0-9].*",zipcode)
            item_loader.add_value("zipcode",zipcode.group().replace("\r",""))
        except AttributeError:
            print("No zipcode!")
             
        rent = response.xpath("//div[@class='priceC']//span//text()").get()
        if rent:
            renti="".join(response.xpath("//div[@class='priceC']//text()").getall())
            if "per week" in renti.lower() or "pppm" in renti.lower():
                rent = rent.strip().replace("£","").replace(",","")
                item_loader.add_value("rent",int(rent)*4) 
            else:
                if rent and "POA" not in rent:
                    rent = rent.strip().replace("£","").replace(",","")
                    if rent.isdigit():
                        item_loader.add_value("rent",int(rent)) 


        item_loader.add_value("currency", "GBP")

        desc = " ".join(response.xpath("//div[@id='descriptionC']//text() | //div[@id='descriptionC']//div/following-sibling::text()").getall())
        if desc:
            desc=desc.replace("\r","").replace("\n","")
            desc = re.sub('\s{2,}', ' ', desc.strip()) 
            item_loader.add_value("description", desc) 

        room_count = response.xpath("//div[contains(@class,'propertyType')]//text()[contains(.,'Bedroom')]").get()
        if room_count:
            room_count = room_count.strip().split(" ")[0]
            item_loader.add_value("room_count", room_count)
        available=response.xpath("//ul[@class='row']//li//text()").getall()
        if available:
            for i in available:  
                if "available" in i.lower():
                    available_date=i.split("Available")[-1]
                    date2 = available_date.strip()
                    date_parsed = dateparser.parse( 
                        date2, date_formats=["%d-%m-%Y"]
                    )
                    if date_parsed:
                        date3 = date_parsed.strftime("%Y-%m-%d")
                        item_loader.add_value("available_date", date3)
                if "bathroom" in i.lower():
                    bathroom=re.findall("\d+",i)
                    item_loader.add_value("bathroom_count",bathroom)
                if "sqft" in i.lower():
                    squ=i.split("sqm")[0].split("/")[-1].strip().split(".")[0]
        
                    item_loader.add_value("square_meters",squ)
                    

        
        images = [x for x in response.xpath("//div[contains(@id,'imgCarousel')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        parking = response.xpath("//div[contains(@class,'description')]//li[contains(.,'parking') or contains(.,'Parking') or contains(.,'PARKING') or contains(.,'garage') or contains(.,'Garage')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        furnished = response.xpath("//div[contains(@class,'description')]//li[contains(.,'Furnished')]//text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
            
        dishwasher = response.xpath("//div[contains(@class,'description')]//li[contains(.,'Dishwasher')]//text()").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
        
        washing_machine = response.xpath("//div[contains(@class,'description')]//li[contains(.,'Washer')]//text()").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)

        latitude_longitude = response.xpath("//script[contains(.,'LatLng(')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('LatLng(')[1].split(',')[0]
            longitude = latitude_longitude.split('LatLng(')[1].split(',')[1].split(')')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        name=response.xpath("//div[@class='marketedByTextC']/div/text()").get()
        if name:
            item_loader.add_value("landlord_name", name)
        phone=response.xpath("//div[@class='marketedByTextC']/a/text()").get()
        if phone:
            item_loader.add_value("landlord_phone",phone.split(":")[-1])

        yield item_loader.load_item()