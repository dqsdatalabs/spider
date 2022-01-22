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
    name = 'townends_co_uk'
    execution_type='testing'
    country='united_kingdom'
    external_source = "Townends_PySpider_united_kingdom_en"
    locale='en'
    
    def start_requests(self):
        start_urls = [
            {"url": "https://www.stirlingackroyd.com/properties/lettings/tag-residential"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,)

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//div/a/div[@class='caption']"):
            
            follow_url = response.urljoin(item.xpath("./parent::a/@href").get())
            prop_type=item.xpath("./p/text()").get()
            if "apartment" in prop_type:
                property_type="apartment"
            elif "house" in prop_type:
                property_type="house"
            else:property_type='pass'
            if property_type != 'pass':
                yield Request(follow_url, callback=self.populate_item,meta={'property_type': property_type})
                seen = True
        
        if page == 2 or seen:
            url = f"https://www.stirlingackroyd.com/properties/lettings/tag-residential/page-{page}"
            yield Request(url, callback=self.parse, meta={"page": page+1, "property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", self.external_source)

        item_loader.add_value("property_type", response.meta.get("property_type"))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-2])

        title = response.xpath("//h1/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        
        room_count=" ".join(response.xpath("//h2[contains(.,'bed')]/text()").getall()).strip()
        room=room_count.split("|")[0]
        if "bed" in room:
            item_loader.add_value("room_count", room.split(" ")[0])
        else:            
            room=" ".join(response.xpath("//div[@class='property--content']/p/text()[contains(.,'studio') or contains(.,'STUDIO')]").getall()).strip()
            if room:
                item_loader.add_value("room_count", "1")
        
        bathroom_count = " ".join(response.xpath("normalize-space(//h2[contains(.,'bath')]/text())").getall()).strip()
        if bathroom_count:
            bathroom = bathroom_count.split("bath")[0].split("|")[-1].strip()
            item_loader.add_value("bathroom_count", bathroom)
        
        
        rent="".join(response.xpath("//h2[contains(.,'pcm')]/text()").getall()).strip()
        if rent:
            price=rent.split("|")[-1].split("pcm")[0].replace(",","")
            item_loader.add_value("rent_string", price)
        
        address = response.xpath("//h1/text()").get()
        if address:
            zipcode = address.split(",")[-1].strip()
            city = address.split(zipcode)[0].strip().strip(",").split(",")[-1].strip()
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
            
        latitude_longitude = response.xpath("//script[contains(.,'LatLng')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('LatLng(')[1].split(",")[0]
            longitude = latitude_longitude.split('LatLng(')[1].split(",")[1].split(")")[0]
            item_loader.add_value("longitude", longitude.strip())
            item_loader.add_value("latitude", latitude.strip())
           
    
        desc="".join(response.xpath("//div[@class='property--content']/p/text()").getall()).strip()
        if "sq foot" in desc:
            square_meters=desc.split("sq")[0].strip().split(" ")[-1]
            sqm = str(int(int(square_meters)* 0.09290304))
            item_loader.add_value("square_meters", sqm)
        if desc:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', desc.strip()))
        
        images=[x for x in response.xpath("//div[@class='rsContent']/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))

        name=response.xpath("//div[@class='branch--info']/p[contains(@class,'title')]//text()").get()
        if name:
            item_loader.add_value("landlord_name", name)

        phone="+".join(response.xpath("//div[@class='branch--info']/p[contains(@class,'phone')]/a/text()").getall()).strip()
        if phone:
            item_loader.add_value("landlord_phone", phone.split("+")[0])
        
        email = response.xpath("//a[contains(@href, 'mailto')]/text()").get()
        if email:
            item_loader.add_value("landlord_email", email)

        parking = "".join(response.xpath("//ul[@class='bullet']/li[contains(.,'Parking') or contains(.,'Garage')]/text()").extract())
        if parking :
                item_loader.add_value("parking", True) 
        
        balcony = "".join(response.xpath("//ul[@class='bullet']/li[contains(.,'balcon')]/text()").extract())
        if balcony :
                item_loader.add_value("balcony", True)         
                   
        yield item_loader.load_item()