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
    name = 'brinkborgh_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.brinkborgh.nl/aanbod/woningaanbod/beschikbaar/huur/benedenwoning/",
                    "https://www.brinkborgh.nl/aanbod/woningaanbod/beschikbaar/huur/bovenwoning/",
                    "https://www.brinkborgh.nl/aanbod/woningaanbod/beschikbaar/huur/galerijflat/",
                    "https://www.brinkborgh.nl/aanbod/woningaanbod/beschikbaar/huur/tussenverdieping/",
                ],
                "property_type" : "apartment",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='vakfoto']/a[@class='aanbodEntryLink']"):
            status = item.xpath("./div/div/@class").get()
            if status and "verhuurd" in status.lower():
                continue 
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta.get("property_type")})   

        next_page = response.xpath("//span[contains(@class,'next-page')]/a/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type":response.meta.get("property_type")}
            )     
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_id", response.url.split('/huis-')[1].split('-')[0].strip())

        item_loader.add_value("external_source", "Brinkborgh_PySpider_netherlands")

        item_loader.add_xpath("title", "//h1[@class='street-address']//text()")        
         
        address =" ".join(response.xpath("//div[contains(@class,'addressInfo')]//text()[normalize-space()]").extract()) 
        if address:
            item_loader.add_value("address",address.strip() ) 

        item_loader.add_xpath("zipcode", "//div[@class='ogDetails']//span[@class='postal-code']//text()")
        item_loader.add_xpath("city", "//div[@class='ogDetails']//span[@class='locality']//text()")
        item_loader.add_xpath("room_count", "//span[span[contains(.,'slaapkamer')]]/span[2]/text()")
        item_loader.add_xpath("deposit","//span[span[contains(.,'Waarborgsom')]]/span[2]/text()")                
        item_loader.add_xpath("rent_string","//span[span[contains(.,'Huurprijs')]]/span[2]/text()")                
        
        available_date = response.xpath("//span[span[contains(.,'Aanvaarding')]]/span[2]/text()").extract_first() 
        if available_date:
            date_parsed = dateparser.parse(available_date.replace("Per","").strip(), languages=['nl'])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
      
        floor = response.xpath("//span[span[contains(.,'woonlagen')]]/span[2]/text()").extract_first() 
        if floor:   
            item_loader.add_value("floor",floor.replace("woonlaag","").replace("woonlagen","").strip())      
       
        square =response.xpath("//span[span[contains(.,'Woonoppervlakte')]]/span[2]/text()").extract_first()
        if square:
            square_meters =  square.split("m")[0].strip()
            item_loader.add_value("square_meters", square_meters) 

        energy = response.xpath("//span[span[contains(.,'Energieklasse')]]/span[2]/text()").extract_first()
        if energy:
            item_loader.add_value("energy_label", energy.strip())
            
        terrace =response.xpath("//span[span[contains(.,'Dakterras')]]/span[2]/text()").extract_first()    
        if terrace:
            if "ja" in terrace.lower():
                item_loader.add_value("terrace", True)
            else:
                item_loader.add_value("terrace", False)
        balcony =response.xpath("//span[span[contains(.,'Balkon')]]/span[2]/text()").extract_first()    
        if balcony:
            if "ja" in balcony.lower():
                item_loader.add_value("balcony", True)
            else:
                item_loader.add_value("balcony", False)
        parking =response.xpath("//span[span[contains(.,'Garage') or contains(.,'Parkeer')]]/span[2]/text()").extract_first()    
        if parking:
            if "geen" in parking.lower():
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)
        elevator =response.xpath("//span[span[contains(.,'Voorzieningen')]]/span[2]/text()[contains(.,'Lift')]").extract_first()    
        if elevator:
            item_loader.add_value("elevator", True)

        furnished =response.xpath("//span[span[contains(.,'Bijzonderheden')]]/span[2]/text()").extract_first()    
        if furnished:
            if "gestoffeerd" in furnished.lower() or "gemeubeld" in furnished.lower():
                item_loader.add_value("furnished", True)
            
        desc = " ".join(response.xpath("//div[@id='Omschrijving']/h3/following-sibling::text()[normalize-space()]").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
              
        images = [x for x in response.xpath("//div[@class='ogFotos']/div[@class='detailFotos']//a/img/@src").extract()]
        if images:
                item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "BrinkBorgh Makelaardij")
        item_loader.add_value("landlord_phone", "020-6709992")
        item_loader.add_value("landlord_email", "info@brinkborgh.nl")
        yield item_loader.load_item()