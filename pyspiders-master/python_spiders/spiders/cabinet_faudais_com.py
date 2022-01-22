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
import re
import dateparser
from datetime import datetime

class MySpider(Spider):
    name = 'cabinet_faudais_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Cabinet_Faudais_PySpider_france'
    start_urls = ["http://www.cabinet-faudais.com/a-louer/"]
    
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//article/div[contains(@class,'card')]"):
            follow_url = response.urljoin(item.xpath(".//a/@href").get())
            prop_type = ""
            property_type = "".join(item.xpath(".//p[@class='card-text']/text()").getall())
            if "Appartement" in property_type:
                prop_type = "apartment"
            elif "Maison" in property_type or "Duplex" in property_type:
                prop_type = "house"
            elif "Studio" in property_type:
                prop_type = "studio"
            seen = True
            if prop_type:
                yield Request(follow_url, callback=self.populate_item, meta={"property_type":prop_type})
             
        if page == 2 or seen:
            follow_url = f"http://www.cabinet-faudais.com/a-louer/{page}"
            yield Request(follow_url, callback=self.parse, meta={"page":page+1})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Cabinet_Faudais_PySpider_france")
        title = response.xpath("//li[@class='breadcrumb-item active']//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        
        item_loader.add_css("title","title")
        
        zipcode = response.xpath("//th[contains(.,'Code')]/following-sibling::th/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode)
        
        address = response.xpath("//th[contains(.,'Ville')]/following-sibling::th/text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address)
        
        square_meters = response.xpath("//th[contains(.,'habitable')]/following-sibling::th/text()").get()
        if square_meters:
            square_meters = square_meters.split("m²")[0].strip().replace(",",".")
            item_loader.add_value("square_meters", math.ceil(int(float(square_meters))))
        
        room_count = response.xpath("//th[contains(.,'chambre')]/following-sibling::th/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            item_loader.add_xpath("room_count", "//th[contains(.,'pièce')]/following-sibling::th/text()")

        bathroom_count = response.xpath("//th[contains(.,'salle')]/following-sibling::th/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        rent = response.xpath("//th[contains(.,'Loyer')]/following-sibling::th/text()").get()
        if rent:
            price =  rent.split("€")[0].strip().replace(",",".").replace(" ","")
            item_loader.add_value("rent", int(float(price)))
        item_loader.add_value("currency", "EUR")
        
        external_id = response.xpath("//span[contains(.,'Ref')]/following-sibling::text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
        
        desc = "".join(response.xpath("//p[@itemprop='description']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc)
            item_loader.add_value("description", desc)
        
        if "A louer un garage" in desc:
            return
        
        available_date = ""
        match = re.search(r'(\d+/\d+/\d+)', desc)
        if match:
            try:
                newformat = dateparser.parse(match.group(1), languages=['en']).strftime("%Y-%m-%d")
                item_loader.add_value("available_date", newformat)
            except: pass
        elif "Disponible" in desc:
            desc2 = desc.replace(" le","")
            try:
                available_date = desc2.replace("\n",".").split("Disponible")[1].split(".")[0].replace(":","").replace("à partir de","").strip()
            except: pass
        elif "Disponible imm\u00e9diatement" in desc:
            item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
        
        if available_date:
            if "immédiatement" in available_date:
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            date_parsed = dateparser.parse(available_date.replace("à partir du",""), date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
            
        images = [ x for x in response.xpath("//ul[contains(@class,'imageGallery')]//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        floor = response.xpath("//th[contains(.,'Etage')]/following-sibling::th/text()").get()
        if floor:
            item_loader.add_value("floor", floor)
        
        utilities = response.xpath("//tr[th[contains(.,'Charge')]]/th[2]/text()").get()
        if utilities:
            uti = utilities.split("€")[0].strip().replace(",",".")
            item_loader.add_value("utilities", int(float(uti)))
        
        deposit = response.xpath("//th[contains(.,'garantie')]/following-sibling::th/text()[not(contains(.,'Non'))]").get()
        if deposit:
            dep = deposit.split("€")[0].strip().replace(",",".")
            item_loader.add_value("deposit",int(float(dep)))
        
        furnished = response.xpath("//th[contains(.,'Meublé')]/following-sibling::th/text()").get()
        if furnished:
            if "non" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "oui" in furnished.lower():
                item_loader.add_value("furnished", True)
        
        elevator = response.xpath("//th[contains(.,'Ascenseur')]/following-sibling::th/text()").get()
        if elevator:
            if "non" in elevator.lower():
                item_loader.add_value("elevator", False)
            elif "oui" in elevator.lower():
                item_loader.add_value("elevator", True)
            
        parking = response.xpath("//th[contains(.,'parking') or contains(.,'garage')]/following-sibling::th/text()[.!='0']").get()
        if parking:
            item_loader.add_value("parking", True)
        
        balcony = response.xpath("//th[contains(.,'Balcon')]/following-sibling::th/text()").get()
        if balcony:
            if "non" in balcony.lower():
                item_loader.add_value("balcony", False)
            elif "oui" in balcony.lower():
                item_loader.add_value("balcony", True)
                
        terrace = response.xpath("//th[contains(.,'Terrasse')]/following-sibling::th/text()").get()
        if terrace:
            if "non" in terrace.lower():
                item_loader.add_value("terrace", False)
            elif "oui" in terrace.lower():
                item_loader.add_value("terrace", True)
        
        latitude_longitude = response.xpath("//script[contains(.,'lat')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split("lat :")[1].split(",")[0].strip()
            longitude = latitude_longitude.split("lng:")[1].split("}")[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        name = response.xpath("//div[@class='media-body']/span[1]/text()").get()
        if name:
            item_loader.add_value("landlord_name", name)
        
        phone = response.xpath("//div[@class='media-body']/span[2]/text()").get()
        if phone:
            item_loader.add_value("landlord_phone", phone)
        
        landlord_email = response.xpath("//div[@class='media-body']/span/a/text()").get()
        if landlord_email:
            item_loader.add_value("landlord_email", landlord_email)
        

        yield item_loader.load_item()