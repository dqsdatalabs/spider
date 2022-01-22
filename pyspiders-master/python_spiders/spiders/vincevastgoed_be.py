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
    name = 'vincevastgoed_be'
    execution_type='testing'
    country='belgium'
    locale='nl'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://vincevastgoed.be/nl/te-huur/?type%5B%5D=1&price-min=&price-max=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://vincevastgoed.be/nl/te-huur/?type%5B%5D=5&price-min=&price-max=",
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
        for item in response.xpath("//div[@class='spotlight__image ']/a"):
            status = item.xpath("./div/text()").get()
            if status and "verhuurd" in status.lower():
                continue
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Vincevastgoed_PySpider_belgium")

        item_loader.add_xpath("title", "//div/h1//text()")        
         
        address = response.xpath("//div[@class='property__header-block__adress__street']//text()").extract_first()
        if address:
            item_loader.add_value("address",address.strip() ) 
            address= address.split(",")[-1].strip()   
            zipcode = address.split(" ")[0]
            city = address.replace(zipcode,"")   
            item_loader.add_value("zipcode",zipcode.strip() )    
            item_loader.add_value("city",city.strip() ) 

        item_loader.add_xpath("external_id", "substring-after(//div[@class='property__header-block__ref']//text(),': ')")
        item_loader.add_xpath("room_count", "//tr[td[contains(.,'slaapkamer')]]/td[2]/text()")
        item_loader.add_xpath("bathroom_count", "//tr[td[contains(.,'badkamer')]]/td[2]/text()")
        rent =response.xpath("//h2[@class='property__sub-title']//text()[contains(.,'€')]").extract_first()
        if rent:     
            item_loader.add_value("rent_string",rent)  
        if not rent:
            item_loader.add_value("currency","EUR")  

        utilities =response.xpath("//tr[td[contains(.,'Gemeenschappelijke kosten')]]/td[2]/text()").extract_first()
        if utilities:     
            utilities = utilities.split("€")[1].strip().replace(".","")
            item_loader.add_value("utilities", int(float(utilities.replace(",","."))))  
        elif not utilities:
            utilities =response.xpath("//div[@class='property__details__block__description']//text()[normalize-space()][contains(.,'Gemeenschappelijke kosten') and contains(.,'€')]").extract_first()
            if utilities:   
                item_loader.add_value("utilities",utilities.strip())  
            
        available_date = response.xpath("//tr[td[contains(.,'Vrij op')]]/td[2]/text()").extract_first() 
        if available_date:
            date_parsed = dateparser.parse(available_date.replace("onmiddellijk","now").strip())
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        square =response.xpath("//tr[td[contains(.,'Woonoppervlakte')]]/td[2]/text()").extract_first()
        if square:
            square_meters =  square.split("m")[0].strip()
            item_loader.add_value("square_meters",int(float(square_meters.replace(",",".")))) 

        energy_label =response.xpath("substring-before(//tr[td[.='EPC waarde']]/td[2]//text(),'k')").extract_first()    
        if energy_label:
            item_loader.add_value("energy_label",energy_label.strip())  
        parking =response.xpath("//tr[td[contains(.,'Garage')]]//text()[normalize-space()]").extract_first()    
        if parking:
            item_loader.add_value("parking", True)
        elif not parking:
            parking =response.xpath("//tr[td[contains(.,'parkeer')]]/td[2]/text()[normalize-space()]").extract_first()    
            if parking:
                item_loader.add_value("parking", True)
        terrace =response.xpath("//tr[td[contains(.,'Terras')]]//text()[normalize-space()]").extract_first()    
        if terrace:
            item_loader.add_value("terrace", True)
        elevator =response.xpath("//tr[td[contains(.,'Lift')]]/td[2]/text()").extract_first()    
        if elevator:
            if "ja" in elevator.lower():
                item_loader.add_value("elevator", True)
            else:
                item_loader.add_value("elevator", False)
        
        desc = " ".join(response.xpath("//div[@class='property__details__block__description']//text()[normalize-space()]").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
              
        images = [response.urljoin(x) for x in response.xpath("//div[@class='prop-pictures']//a/img/@data-src").extract()]
        if images:
                item_loader.add_value("images", images)
        item_loader.add_xpath("latitude", "//div[@id='pand-map']/@data-geolat")
        item_loader.add_xpath("longitude", "//div[@id='pand-map']/@data-geolong")

        item_loader.add_value("landlord_name", "VINCE VASTGOED")
        item_loader.add_value("landlord_phone", "+32 3 313 03 65")
        item_loader.add_value("landlord_email", "info@vincevastgoed.be")
        furnished =response.xpath("//tr[td[contains(.,'Gemeubeld')]]/td[2]/text()").extract_first()    
        if furnished:
            if "ja" in furnished.lower():
                item_loader.add_value("furnished", True)
            else:
                item_loader.add_value("furnished", False)
        floor =response.xpath("//tr[td[.='Verdieping']]/td[2]/text()[normalize-space()]").extract_first()    
        if floor:
            item_loader.add_value("floor", floor.strip())
        yield item_loader.load_item()