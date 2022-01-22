# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request, FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import json2html
from datetime import datetime
import math
class MySpider(Spider):
    name = 'cabinetdevaux_com'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    thousand_separator = ','
    scale_separator = '.'
    
    def start_requests(self):
        start_urls = [
            {"url": "http://www.cabinetdevaux.com/"},
        ]  # LEVEL 1


        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse
                            )

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@class='resultats-annonces']/a[contains(@class,'miniAnnonce')]"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            title = item.xpath(".//div[@class='miniAnnonce-titre']/text()").get()
            if "T1" in title or "T3" in title or "T2" in title or "STUDIO" in title or "APPARTEMENT" in title:
                property_type = "apartment"
                yield Request(follow_url, callback=self.populate_item, meta={'property_type': property_type})
            elif "MAISON" in title or "T4" in title or "T5" in title:
                property_type = "house"
                yield Request(follow_url, callback=self.populate_item, meta={'property_type': property_type})
        

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
    
       
        item_loader.add_value("property_type", response.meta.get("property_type"))
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Cabinetdevaux_PySpider_"+ self.country + "_" + self.locale)
        title = response.xpath("//div[@class='popupAnnonce-description']/h2/text()").extract_first()
        if title:
            item_loader.add_value("title", title)   
            if len(title.split("- "))>2:   
                item_loader.add_value("address",title.split("- ")[2])
            elif len(title.split("-"))>1:
                item_loader.add_value("address",title.split("-")[1])
            elif title.split("- ") is None:
                pass
            else:
                item_loader.add_value("address",title.split(" ")[-1])

        city = ""
        add_city = response.xpath("//div[@class='popupAnnonce-description']/h2/text()").extract_first()
        if add_city:
            if "-" in add_city:
                city = add_city.split("-")[-1]
                
            else:
                city =  add_city.split(" ")[-1]
            item_loader.add_value("city",city.strip())

        rent =response.xpath("//ul[@class='pricelist']/li[contains(.,'Loyer') or contains(.,'Prix')]/strong/text()").get()
        if rent:
            if int(rent.split("€")[0].strip())>100000:
                return
            item_loader.add_value("rent_string", rent)
        item_loader.add_xpath("external_id", "//li[contains(.,'Référence')]/strong/text()")
        

        desc = "".join(response.xpath("//div[@class='popupAnnonce-description']/p/span/following-sibling::text()").extract())
        if desc :
            item_loader.add_value("description", desc.strip()) 
            if "vaisselle" in desc :
                item_loader.add_value("dishwasher", True)
            if "balcon" in desc :
                item_loader.add_value("balcony", True)
            if "terrasse" in desc :
                item_loader.add_value("terrace", True)
        
        square_meters = response.xpath("//li[contains(.,'Surface habitable')]/strong/text()").extract_first()
        if square_meters :         
            square_meters = math.ceil(float(square_meters.split("m")[0].replace(",",".")))
            item_loader.add_value("square_meters", str(square_meters))  

        item_loader.add_xpath("bathroom_count", "//ul[contains(@class,'popupAnnonce-details')]/li[contains(.,'Nb salles')]/strong/text()") 
        item_loader.add_xpath("energy_label", "substring-before(substring-after(substring-after(//div[@class='popupAnnonce-dpe']/img[1]/@src,'dpe-ges/'),'-'),'.')") 
            
    
        utilities = response.xpath("//li[contains(.,'Charges :')]/strong/text()").extract_first()
        if utilities :  
            item_loader.add_value("utilities", utilities.split("€")[0].strip())

        deposit = response.xpath("//ul[@class='pricelist']/li[contains(.,'Dépôt de garantie')]/strong/text()").extract_first()
        if deposit :  
            dp=deposit.split("€")[0].strip().replace(" ","")
            item_loader.add_value("deposit", str(dp))
        
        room_count = response.xpath("//li[contains(.,'Nb chambres')]/strong/text()").extract_first()
        if room_count :
            item_loader.add_value("room_count", room_count)
        elif "STUDIO" in title:
            item_loader.add_value("room_count", "1")

        elevator = response.xpath("//li[contains(.,'Ascenseur')]/strong/text()").extract_first()
        if elevator :
            item_loader.add_value("elevator", True)

        parking = response.xpath("//li[contains(.,'parking')]/strong/text()").extract_first()
        if parking :        
            item_loader.add_value("parking", True)
        
        item_loader.add_value("landlord_name", "Cabinet Devaux")
        item_loader.add_value("landlord_phone", "03 83 35 35 34")
        
        images = [x for x in response.xpath("//div[@class='popupAnnonce-mini']/span/@data-img").extract()]
        if images:
            item_loader.add_value("images", images)

        a_date = response.xpath("//div[@class='popupAnnonce-dispo']/text()[normalize-space()]").extract_first()
        if a_date:
            oldformat = a_date.strip()
            datetimeobject = datetime.strptime(oldformat,'%d/%m/%Y')
            newformat = datetimeobject.strftime('%Y-%m-%d')
            item_loader.add_value("available_date", newformat)


        yield item_loader.load_item()