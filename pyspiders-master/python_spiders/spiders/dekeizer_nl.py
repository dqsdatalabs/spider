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
class MySpider(Spider):
    name = 'dekeizer_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.dekeizer.nl/woningaanbod/huur/type-appartement?orderby=9",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.dekeizer.nl/woningaanbod/huur/type-woonhuis?orderby=9",
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
        for item in response.xpath("//div[contains(@class,'objectcontainer')]"):
            status = item.xpath("./span[contains(@class,'status')]/span/text()").get()
            if status and "verhuurd" in status.lower():
                continue
            follow_url = response.urljoin(item.xpath("./a/@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta['property_type']})
        
        next_page = response.xpath("//a[contains(@class,'next-page')]/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={'property_type': response.meta['property_type']}
            )

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Dekeizer_PySpider_netherlands")
     
        external_id =response.xpath("//table//tr[td[contains(.,'Referentienummer')]]/td[2]//text()").extract_first()
        if external_id:
            item_loader.add_value("external_id",external_id.strip() )       

        title =" ".join(response.xpath("//div/h1[contains(@class,'obj_address')]//text()").extract())
        if title:
            item_loader.add_value("title",title.strip() )       
        address =response.xpath("//div[h1[@class='obj_address title']]/span/text()[normalize-space()]").extract_first()
        if not address:
            address =response.xpath("substring-after(//div/h1[contains(@class,'obj_address')]//text(),':')").extract_first()
        if address:
            item_loader.add_value("address",address.strip())               
            city= address.split(",")[-1]
            zipcode = " ".join(city.strip().split(" ")[0:2])
            city_value = city.replace(zipcode,"").strip()          
            item_loader.add_value("city",city_value.strip())                
            item_loader.add_value("zipcode",zipcode.strip())                
        rent =response.xpath("//table//tr[td[contains(.,'Prijs')]]/td[2]//text()").extract_first()
        if rent:     
           item_loader.add_value("rent_string", rent)   
         
        available_date = response.xpath("//table//tr[td[contains(.,'Aanvaarding')]]/td[2]//text()[not(contains(.,'In overleg'))]").extract_first() 
        if available_date:
            date_parsed = dateparser.parse(available_date.replace("Per maandag","").replace("Per ",""), languages=['nl'])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
      
        floor = response.xpath("//table//tr[td[contains(.,'Aantal bouwlagen')]]/td[2]//text()").extract_first() 
        if floor:   
            item_loader.add_value("floor",floor.strip())      
        room_count = response.xpath("//table//tr[td[contains(.,'Aantal kamers')]]/td[2]//text()").extract_first() 
        if room_count:   
            if "slaapkamer" in room_count:
                room_count = room_count.split("slaapkamer")[0].strip().split(" ")[0]
            item_loader.add_value("room_count",room_count.strip())
      
        square =response.xpath("//table//tr[td[contains(.,'Gebruiksoppervlakte wonen')]]/td[2]//text()").extract_first()
        if square:
            square_meters =  square.split("m")[0].strip()
            item_loader.add_value("square_meters", square_meters) 

        balcony = response.xpath("//table//tr[td[contains(.,'balkon')]]/td[2]//text()").extract_first()       
        if balcony:
            item_loader.add_value("balcony", True) 
        terrace = response.xpath("//table//tr[td[contains(.,'tussenwoning')]]/td[2]//text()").extract_first()       
        if terrace:
            item_loader.add_value("terrace", True) 
        
        utilities =" ".join(response.xpath("//table//tr[td[contains(.,'Servicekosten')]]/td[2]//text()").extract())       
        if utilities:
            item_loader.add_value("utilities",utilities) 
  
        desc = " ".join(response.xpath("//div[@id='object_description_anker']//text()").extract())
        if desc:
            item_loader.add_value("description", desc.replace("Lees meer \r\n ","").replace("\r\n","").strip())
      
        script_map = response.xpath("//script/text()[contains(.,'lat') and contains(.,'lon:')]").get()
        if script_map:
            item_loader.add_value("latitude", script_map.split("lat:")[1].split(",")[0].strip())
            item_loader.add_value("longitude", script_map.split("lon:")[1].split(",")[0].strip())
            
        images = [response.urljoin(x)for x in response.xpath("//div[@id='slider-property']/a//img/@src").extract()]
        if images:
                item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "Dekeizer")
        item_loader.add_value("landlord_phone", "030 275 90 40")
        item_loader.add_value("landlord_email", "utrecht@dekeizer.nl")
        yield item_loader.load_item()