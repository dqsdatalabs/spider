# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import math


class MySpider(Spider):
    name = 'home_hunters_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.home-hunters.fr/louer/search/?buy-or-rental=2&type=7&city=&suggest-city=&amount-buy-min=&amount-buy-max=&amount-rental-min=&amount-rental-max=",
                    "https://www.home-hunters.fr/louer/search/?buy-or-rental=2&type=1&city=&suggest-city=&amount-buy-min=&amount-buy-max=&amount-rental-min=&amount-rental-max=",
                ],
                "property_type" : "apartment"
            },
            {
                "url" : ["https://www.home-hunters.fr/louer/search/?buy-or-rental=2&type=2&city=&suggest-city=&amount-buy-min=&amount-buy-max=&amount-rental-min=&amount-rental-max="],
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

        for item in response.xpath("//a[contains(@class,'link-fixed-tablette')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
              
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Home_hunters_PySpider_"+ self.country + "_" + self.locale)

        title = response.xpath("//div[@class='cont']/h1/text()").extract_first()
        if title :            
            item_loader.add_value("title", title)
           
        external_id = response.xpath("//span[@class='infos']/span/text()").extract_first()
        if external_id :
            external_id = external_id.split(":")[1].strip()
            if external_id:
                item_loader.add_value("external_id", external_id)

        city=response.xpath("//span[@class='infos']/h3/text()").extract_first()
        if city:
            item_loader.add_value("city", city)
            item_loader.add_value("address",city)

        price = response.xpath("//span[@class='left cc']/text()").extract_first()
        if price :
            item_loader.add_value("rent_string", price)
        desc = "".join(response.xpath("//div[@class='true text']/h3[contains(.,'Description')]/following-sibling::text()").extract())
        if desc :
            item_loader.add_value("description", desc.strip()) 
            if "terrasse" in desc :
                item_loader.add_value("terrace", True)
            if "parking " in desc :
                item_loader.add_value("parking", True)
        
        contactname=response.xpath("//div[@class='true text']/text()[contains(.,'Contact direct :')]").extract_first()        
        if contactname.split(":")[1].strip():
            item_loader.add_value("landlord_name",contactname.split(":")[1].strip())  
            phone=response.xpath("//div[@class='true text']/text()[contains(.,'Contact direct : ')]//following-sibling::text()[1]").extract_first()        
            mail=response.xpath("//div[@class='true text']/text()[contains(.,'Contact direct : ')]//following-sibling::text()[2]").extract_first()        
            
            item_loader.add_value("landlord_phone",phone.replace("Tel :",""))
            item_loader.add_value("landlord_email", mail.replace("Mail :",""))   

        else:
            contactname=response.xpath("//div[@class='true text']//text()[contains(.,'Contact')]//following-sibling::text()[1]").extract_first()        
            if contactname:
                item_loader.add_value("landlord_name",contactname)
                phone=response.xpath("//div[@class='true text']//text()[contains(.,'Contact')]//following-sibling::text()[2]").extract_first()        
                mail=response.xpath("//div[@class='true text']//text()[contains(.,'Contact')]//following-sibling::text()[3]").extract_first()        
                
                item_loader.add_value("landlord_phone",phone.replace("Tel :",""))
                item_loader.add_value("landlord_email", mail.replace("Mail :",""))
     
        
        square_meters = response.xpath("//div[@class='divtab']/div[contains(.,'Surface Habitable')]/text()").extract_first()
        if square_meters :           
            square_meters = math.ceil(float(square_meters.split(":")[1].split("m")[0].strip()))
            item_loader.add_value("square_meters", str(square_meters))
        
        room_count = response.xpath("//div[@class='divtab']/div[contains(.,'pièce')]/text()").extract_first()
        if room_count :
            item_loader.add_value("room_count", room_count.split(":")[1])
       
        elevator = response.xpath("//div[contains(@class,'divtab')]/div[contains(.,'Ascenseur')]/text()").extract_first()
        if elevator :
            if "non" in  elevator :
                item_loader.add_value("elevator", False)
            else:
                item_loader.add_value("elevator", True)

        balcony = response.xpath("//div[contains(@class,'divtab')]/div[contains(.,'Balcon')]/text()").extract_first()
        if balcony :        
            item_loader.add_value("balcony", True)

        energy_label = response.xpath("//div[contains(@class,'dpe')]//span[contains(@class,'letter')]/text()").extract_first()
        if energy_label :
            item_loader.add_value("energy_label", energy_label)
        
        latlng = response.xpath("//script[@type='text/javascript'][contains(.,'lat')]").extract_first()
        if latlng:
            latitude = latlng.split("lat\":")[1].strip().split(",")[0]
            longitude = latlng.split("lng\":")[1].strip().split(" }")[0]
            if latitude and longitude:
                item_loader.add_value("latitude", latitude)
                item_loader.add_value("longitude", longitude)

        img=response.xpath("//ul[@class='element image']//span/a/@data-image").extract()
        if img :
            images=[]
            for x in img :
                images.append(x)
            if images:
                item_loader.add_value("images",  list(set(images)))

              
        yield item_loader.load_item()

        
       

        
        
          

        

      
     