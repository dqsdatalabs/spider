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
    name = 'ltcvastgoedadvies_be'
    execution_type='testing'
    country='belgium'
    locale='nl'
    def start_requests(self):
        start_urls = [
            {"url": "https://www.ltcvastgoedadvies.be/te-huur?countries=&transactiontype=Rent&sorts%5B%5D=Flat&NumberOfBedrooms-From=&Price-From=&Price-To=&flowstatus=ForRent%2CToNegotiateForRent%2CForRentReserved%2CToCompleteForRent", "property_type": "apartment"},
	        {"url": "https://www.ltcvastgoedadvies.be/te-huur?countries=&transactiontype=Rent&sorts%5B%5D=Dwelling&NumberOfBedrooms-From=&Price-From=&Price-To=&flowstatus=ForRent%2CToNegotiateForRent%2CForRentReserved%2CToCompleteForRent", "property_type": "house"},  
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                            "base_url":url.get('url')
                        })

    # 1. FOLLOWING
    def parse(self, response):
        property_type = response.meta.get("property_type")
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//div[contains(@class,'pubc')]/a/@href").extract():
            follow_url = response.urljoin(item)
            if "/detail/" in follow_url:
                yield Request(follow_url, callback=self.populate_item, meta={"property_type":property_type})
            seen = True
        
        if page == 2 or seen:
            base_url = response.meta.get("base_url")
            url = base_url.replace(f"huur?countries",f"huur?pageindex={page}&countries")
            yield Request(url, callback=self.parse, meta={"page": page+1, "property_type":property_type, "base_url":base_url})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("title", "//h1/text()")
        item_loader.add_value("external_source", "Ltcvastgoedadvies_PySpider_belgium")
  
        address =" ".join(response.xpath("//tr[td[.='Adres:']]/td[2]//text()").extract())
        if address:
            item_loader.add_value("address",address.strip() ) 
        zipcode =response.xpath("//div[contains(@class,'ligging')]/h2/text()").extract_first()
        if zipcode:   
            item_loader.add_value("zipcode", zipcode.split("- ")[-1].strip().split(" ")[0])  
   
        item_loader.add_xpath("bathroom_count","//tr[td[.='Badkamers:']]/td[2]//text()")        
        item_loader.add_xpath("external_id","//tr[td[.='Referentie:']]/td[2]//text()")                
        item_loader.add_xpath("city","//tr[td[.='Adres:']]/td[2]//text()[last()]")                
        item_loader.add_xpath("floor","//tr[td[.='Op verdieping:']]/td[2]//text()")                
        item_loader.add_xpath("energy_label","substring-before(//tr[td[.='EPC Index:']]/td[2]//text(),',')")                
                            
        rent =response.xpath("//h3/text()[contains(.,'€')]").extract_first()
        if rent:     
           item_loader.add_value("rent_string", rent.replace(" ","").replace("\xa0",""))   

        room_count = response.xpath("//div[span[contains(@class,'icon-bed')]]/text()[normalize-space()]").extract_first() 
        if room_count:   
            item_loader.add_value("room_count",room_count)

        square =response.xpath("//div[span[contains(@class,'icon-surfacearea')]]/text()[normalize-space()]").extract_first()
        if not square:
            square =response.xpath("//tr[td[.='Perceeloppervlakte:']]/td[2]//text()").extract_first()
        if square:
            square_meters =  square.split("m")[0].strip()
            item_loader.add_value("square_meters", square_meters) 

        balcony =response.xpath("//tr[td[.='Balkon:']]/td[2]//text()").extract_first()    
        if balcony:
            if "nee" in balcony.lower():
                item_loader.add_value("balcony", False)
            else:
                item_loader.add_value("balcony", True)
        furnished =response.xpath("//tr[td[.='Gemeubeld:']]/td[2]//text()").extract_first()    
        if furnished:
            if "nee" in furnished.lower():
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)
        elevator =response.xpath("//tr[td[.='Lift:']]/td[2]//text()").extract_first()    
        if elevator:
            if "nee" in elevator.lower():
                item_loader.add_value("elevator", False)
            else:
                item_loader.add_value("elevator", True)
        pets =response.xpath("//tr[td[.='Huisdieren toegelaten:']]/td[2]//text()").extract_first()    
        if pets:
            if "nee" in pets.lower():
                item_loader.add_value("pets_allowed", False)
            else:
                item_loader.add_value("pets_allowed", True)
        terrace =response.xpath("//tr[td[.='Terras:']]/td[2]//text()").extract_first()    
        if terrace:
            if "nee" in terrace.lower():
                item_loader.add_value("terrace", False)
            else:
                item_loader.add_value("terrace", True)
        parking =response.xpath("//div[span[contains(@class,'icon-garage')]]/text()[normalize-space()]").extract_first()    
        if parking:
            if "nee" in parking.lower():
                parking =response.xpath("//tr[td[.='Parking:']]/td[2]//text()").extract_first()    
                if parking:
                    if "nee" in parking.lower():
                        item_loader.add_value("parking", False)                        
                    else:
                        item_loader.add_value("parking", True)
            else:
                item_loader.add_value("parking", True)

        desc = " ".join(response.xpath("//div[@class='col-xs-12']/h2/../text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
              
        images = [x for x in response.xpath("//div[@class='slides']/div[@class='slide']/img/@data-src").extract()]
        if images:
                item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "LTC Vastgoedadvies")
        item_loader.add_value("landlord_phone", "016 82 59 56")
        item_loader.add_value("landlord_email", "info@ltcvastgoedadvies.be")

        script_map = response.xpath("//script/text()[contains(.,'long:')]").extract_first()
        if script_map:
            item_loader.add_value("latitude", script_map.split("lat:")[1].split(",")[0].strip())
            item_loader.add_value("longitude", script_map.split("long:")[1].split(",")[0].strip())
    

        yield item_loader.load_item()